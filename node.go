package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

type phaseViewKey struct {
	Phase MsgType
	View  int
}

type voteSetKey struct {
	Phase    MsgType
	View     int
	NodeHash string
}

type Node struct {
	mu sync.RWMutex

	ID           string
	Network      *Network
	Inbox        chan Message
	ClientInbox  chan Command
	TimeoutInbox chan int
	stopCh       chan struct{}

	StateMachine *StateMachine
	Signer       *Signer
	Verifier     *ThresholdVerifier
	Pacemaker    *Pacemaker

	ViewNumber int
	Phase      MsgType
	NodesCount int
	Quorum     int
	LockedQC   *QuorumCertificate
	PrepareQC  *QuorumCertificate

	Tree             map[string]*TreeNode
	LastExecutedHash string
	ExecutedCommands map[string]bool
	PendingCommands  []Command
	KnownCommands    map[string]bool

	Voted          map[phaseViewKey]string
	Votes          map[voteSetKey]map[string]SignatureShare
	FormedQC       map[voteSetKey]bool
	NewViews       map[int]map[string]*QuorumCertificate
	ProposedInView map[int]bool
	FutureMessages map[int][]Message
}

func NewNode(
	id string,
	net *Network,
	nodesCount int,
	signer *Signer,
	verifier *ThresholdVerifier,
	baseTimeout time.Duration,
) *Node {
	inbox, clientInbox := net.RegisterNode(id)
	f := (nodesCount - 1) / 3
	node := &Node{
		ID:               id,
		Network:          net,
		Inbox:            inbox,
		ClientInbox:      clientInbox,
		TimeoutInbox:     make(chan int, 8),
		stopCh:           make(chan struct{}),
		StateMachine:     NewStateMachine(id),
		Signer:           signer,
		Verifier:         verifier,
		ViewNumber:       1,
		Phase:            NewView,
		NodesCount:       nodesCount,
		Quorum:           nodesCount - f,
		Tree:             make(map[string]*TreeNode),
		LastExecutedHash: GenesisHash,
		ExecutedCommands: make(map[string]bool),
		KnownCommands:    make(map[string]bool),
		Voted:            make(map[phaseViewKey]string),
		Votes:            make(map[voteSetKey]map[string]SignatureShare),
		FormedQC:         make(map[voteSetKey]bool),
		NewViews:         make(map[int]map[string]*QuorumCertificate),
		ProposedInView:   make(map[int]bool),
		FutureMessages:   make(map[int][]Message),
	}
	node.Pacemaker = NewPacemaker(node, baseTimeout)
	return node
}

func hashNode(node *TreeNode) string {
	if node == nil {
		return ""
	}
	payload, err := json.Marshal(struct {
		ParentHash string  `json:"parent_hash"`
		Command    Command `json:"command"`
	}{
		ParentHash: node.ParentHash,
		Command:    node.Cmd,
	})
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func (n *Node) Start() {
	go n.Run()
	n.Pacemaker.Start()

	n.mu.Lock()
	n.sendNewViewLocked(0, 1)
	n.Pacemaker.Arm(1, true)
	n.mu.Unlock()
}

func (n *Node) Stop() {
	n.Pacemaker.Stop()
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
}

func (n *Node) Run() {
	for {
		select {
		case msg := <-n.Inbox:
			n.handleMessage(msg)
		case cmd := <-n.ClientInbox:
			n.handleClientCommand(cmd)
		case viewNumber := <-n.TimeoutInbox:
			n.handleTimeout(viewNumber)
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) NotifyTimeout(viewNumber int) {
	select {
	case n.TimeoutInbox <- viewNumber:
	default:
	}
}

func (n *Node) handleClientCommand(cmd Command) {
	if cmd.ID == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.KnownCommands[cmd.ID] {
		return
	}
	n.KnownCommands[cmd.ID] = true
	n.PendingCommands = append(n.PendingCommands, cmd)
	n.tryProposeLocked()
}

func (n *Node) handleTimeout(viewNumber int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if viewNumber != n.ViewNumber {
		return
	}
	AddLog("[%s] nextView interrupt in view %d.\n", n.ID, viewNumber)
	n.enterViewLocked(viewNumber+1, false)
}

func (n *Node) handleMessage(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.Verifier.IsMember(msg.SenderID) {
		return
	}

	if msg.Type == NewView {
		n.processNewViewLocked(msg)
		return
	}

	if msg.ViewNumber > n.ViewNumber {
		n.FutureMessages[msg.ViewNumber] = append(n.FutureMessages[msg.ViewNumber], cloneMessage(msg))
		return
	}
	if msg.ViewNumber < n.ViewNumber {
		return
	}

	if msg.IsVote {
		n.processLeaderVoteLocked(msg)
		return
	}
	n.processReplicaMessageLocked(msg)
}

func (n *Node) processNewViewLocked(msg Message) {
	targetView := msg.ViewNumber + 1
	if targetView < 1 || n.Network.LeaderForView(targetView) != n.ID {
		return
	}
	if msg.IsVote || msg.Node != nil || msg.PartialSig != nil {
		return
	}
	if msg.Justify != nil {
		if msg.Justify.Type != Prepare || msg.Justify.ViewNumber > msg.ViewNumber || !n.Verifier.VerifyQC(msg.Justify) {
			return
		}
	}

	if n.NewViews[targetView] == nil {
		n.NewViews[targetView] = make(map[string]*QuorumCertificate)
	}
	if _, duplicate := n.NewViews[targetView][msg.SenderID]; duplicate {
		return
	}
	n.NewViews[targetView][msg.SenderID] = cloneQC(msg.Justify)

	if targetView == n.ViewNumber {
		n.tryProposeLocked()
	}
}

func (n *Node) tryProposeLocked() {
	viewNumber := n.ViewNumber
	if n.Network.LeaderForView(viewNumber) != n.ID || n.ProposedInView[viewNumber] {
		return
	}
	newViews := n.NewViews[viewNumber]
	if len(newViews) < n.Quorum {
		return
	}

	cmd, exists := n.nextPendingCommandLocked()
	if !exists {
		return
	}

	highQC := highestQC(newViews)
	parentHash := GenesisHash
	if highQC != nil {
		if highQC.Type != Prepare || !n.Verifier.VerifyQC(highQC) {
			return
		}
		parentHash = highQC.NodeHash
		if !n.ensureNodeLocked(parentHash) {
			return
		}
	}

	proposal := &TreeNode{ParentHash: parentHash, Cmd: cmd, ProposedView: viewNumber}
	proposal.Hash = hashNode(proposal)
	n.Tree[proposal.Hash] = cloneTreeNode(proposal)
	n.Network.StoreNode(proposal)
	n.ProposedInView[viewNumber] = true
	n.Phase = Prepare

	AddLog("[%s] Leader of view %d proposes command %s, parent=%s.\n", n.ID, viewNumber, cmd.ID, shortHash(parentHash))
	n.Network.Broadcast(n.ID, Message{
		Type:       Prepare,
		ViewNumber: viewNumber,
		Node:       proposal,
		Justify:    highQC,
	})
}

func highestQC(newViews map[string]*QuorumCertificate) *QuorumCertificate {
	var candidates []*QuorumCertificate
	for _, qc := range newViews {
		if qc != nil {
			candidates = append(candidates, qc)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ViewNumber == candidates[j].ViewNumber {
			return candidates[i].NodeHash < candidates[j].NodeHash
		}
		return candidates[i].ViewNumber > candidates[j].ViewNumber
	})
	return cloneQC(candidates[0])
}

func (n *Node) processReplicaMessageLocked(msg Message) {
	if msg.SenderID != n.Network.LeaderForView(msg.ViewNumber) {
		return
	}

	switch msg.Type {
	case Prepare:
		n.processPrepareLocked(msg)
	case PreCommit:
		n.processPreCommitLocked(msg)
	case Commit:
		n.processCommitLocked(msg)
	case Decide:
		n.processDecideLocked(msg)
	}
}

func (n *Node) processPrepareLocked(msg Message) {
	if msg.Node == nil || msg.PartialSig != nil || msg.IsVote {
		return
	}
	if !n.acceptNodeLocked(msg.Node) {
		return
	}

	expectedParent := GenesisHash
	if msg.Justify != nil {
		if msg.Justify.Type != Prepare || msg.Justify.ViewNumber >= msg.ViewNumber || !n.Verifier.VerifyQC(msg.Justify) {
			return
		}
		expectedParent = msg.Justify.NodeHash
		if !n.ensureNodeLocked(expectedParent) {
			return
		}
	}
	if msg.Node.ParentHash != expectedParent {
		return
	}
	if !n.safeNodeLocked(msg.Node, msg.Justify) {
		return
	}

	n.Phase = Prepare
	n.sendVoteLocked(Prepare, msg.ViewNumber, msg.Node, msg.SenderID)
}

func (n *Node) processPreCommitLocked(msg Message) {
	if msg.Node != nil || !n.validPhaseQCLocked(msg.Justify, Prepare, msg.ViewNumber) {
		return
	}
	if !n.ensureNodeLocked(msg.Justify.NodeHash) {
		return
	}
	if n.PrepareQC == nil || msg.Justify.ViewNumber > n.PrepareQC.ViewNumber {
		n.PrepareQC = cloneQC(msg.Justify)
	}
	n.Phase = PreCommit
	n.sendVoteForHashLocked(PreCommit, msg.ViewNumber, msg.Justify.NodeHash, msg.SenderID)
}

func (n *Node) processCommitLocked(msg Message) {
	if msg.Node != nil || !n.validPhaseQCLocked(msg.Justify, PreCommit, msg.ViewNumber) {
		return
	}
	if !n.ensureNodeLocked(msg.Justify.NodeHash) {
		return
	}
	n.LockedQC = cloneQC(msg.Justify)
	n.Phase = Commit
	n.sendVoteForHashLocked(Commit, msg.ViewNumber, msg.Justify.NodeHash, msg.SenderID)
}

func (n *Node) processDecideLocked(msg Message) {
	if msg.Node != nil || !n.validPhaseQCLocked(msg.Justify, Commit, msg.ViewNumber) {
		return
	}
	if !n.ensureNodeLocked(msg.Justify.NodeHash) {
		return
	}
	n.Phase = Decide
	if !n.executeThroughLocked(msg.Justify.NodeHash) {
		return
	}

	AddLog("[%s] DECIDE command branch through %s in view %d.\n", n.ID, shortHash(msg.Justify.NodeHash), msg.ViewNumber)
	n.enterViewLocked(msg.ViewNumber+1, true)
}

func (n *Node) validPhaseQCLocked(qc *QuorumCertificate, expectedType MsgType, expectedView int) bool {
	return qc != nil &&
		qc.Type == expectedType &&
		qc.ViewNumber == expectedView &&
		n.Verifier.VerifyQC(qc)
}

func (n *Node) processLeaderVoteLocked(msg Message) {
	if n.Network.LeaderForView(msg.ViewNumber) != n.ID || msg.ViewNumber != n.ViewNumber {
		return
	}
	if msg.Type != Prepare && msg.Type != PreCommit && msg.Type != Commit {
		return
	}
	if msg.Node == nil || msg.Justify != nil || msg.PartialSig == nil {
		return
	}
	if msg.PartialSig.SignerID != msg.SenderID || !n.acceptNodeLocked(msg.Node) {
		return
	}
	if !n.Verifier.VerifyShare(*msg.PartialSig, msg.Type, msg.ViewNumber, msg.Node.Hash) {
		return
	}

	key := voteSetKey{Phase: msg.Type, View: msg.ViewNumber, NodeHash: msg.Node.Hash}
	if n.FormedQC[key] {
		return
	}
	if n.Votes[key] == nil {
		n.Votes[key] = make(map[string]SignatureShare)
	}
	if _, duplicate := n.Votes[key][msg.SenderID]; duplicate {
		return
	}
	n.Votes[key][msg.SenderID] = cloneSignatureShare(*msg.PartialSig)
	if len(n.Votes[key]) < n.Quorum {
		return
	}

	combinedSignature, err := n.Verifier.Combine(msg.Type, msg.ViewNumber, msg.Node.Hash, n.Votes[key])
	if err != nil {
		AddLog("[%s] Failed to combine votes: %v\n", n.ID, err)
		return
	}
	qc := &QuorumCertificate{
		Type:               msg.Type,
		ViewNumber:         msg.ViewNumber,
		NodeHash:           msg.Node.Hash,
		ThresholdSignature: combinedSignature,
	}
	n.FormedQC[key] = true

	switch msg.Type {
	case Prepare:
		n.Phase = PreCommit
		AddLog("[%s] Formed prepareQC for %s in view %d.\n", n.ID, shortHash(msg.Node.Hash), msg.ViewNumber)
		n.Network.Broadcast(n.ID, Message{Type: PreCommit, ViewNumber: msg.ViewNumber, Justify: qc})
	case PreCommit:
		n.Phase = Commit
		AddLog("[%s] Formed precommitQC for %s in view %d.\n", n.ID, shortHash(msg.Node.Hash), msg.ViewNumber)
		n.Network.Broadcast(n.ID, Message{Type: Commit, ViewNumber: msg.ViewNumber, Justify: qc})
	case Commit:
		n.Phase = Decide
		AddLog("[%s] Formed commitQC for %s in view %d.\n", n.ID, shortHash(msg.Node.Hash), msg.ViewNumber)
		n.Network.Broadcast(n.ID, Message{Type: Decide, ViewNumber: msg.ViewNumber, Justify: qc})
	}
}

func (n *Node) sendVoteLocked(phase MsgType, viewNumber int, node *TreeNode, leaderID string) {
	if node == nil {
		return
	}
	n.sendVoteForHashLocked(phase, viewNumber, node.Hash, leaderID)
}

func (n *Node) sendVoteForHashLocked(phase MsgType, viewNumber int, nodeHash, leaderID string) {
	key := phaseViewKey{Phase: phase, View: viewNumber}
	if priorNodeHash, voted := n.Voted[key]; voted {
		if priorNodeHash != nodeHash {
			AddLog("[%s] Rejects conflicting %s vote in view %d.\n", n.ID, phase, viewNumber)
		}
		return
	}
	if !n.ensureNodeLocked(nodeHash) {
		return
	}

	n.Voted[key] = nodeHash
	share := n.Signer.Sign(phase, viewNumber, nodeHash)
	n.Network.Send(n.ID, leaderID, Message{
		Type:       phase,
		ViewNumber: viewNumber,
		Node:       n.Tree[nodeHash],
		PartialSig: &share,
		IsVote:     true,
	})
}

func (n *Node) safeNodeLocked(node *TreeNode, qc *QuorumCertificate) bool {
	if n.LockedQC == nil {
		return true
	}
	safetyRule := n.extendsFromLocked(node.Hash, n.LockedQC.NodeHash)
	livenessRule := qc != nil && qc.ViewNumber > n.LockedQC.ViewNumber
	return safetyRule || livenessRule
}

func (n *Node) acceptNodeLocked(node *TreeNode) bool {
	if node == nil || node.ParentHash == "" || node.Hash == "" || node.Cmd.ID == "" {
		return false
	}
	if hashNode(node) != node.Hash {
		return false
	}
	if node.ParentHash != GenesisHash && !n.ensureNodeLocked(node.ParentHash) {
		return false
	}
	n.Tree[node.Hash] = cloneTreeNode(node)
	n.Network.StoreNode(node)
	return true
}

func (n *Node) ensureNodeLocked(nodeHash string) bool {
	if nodeHash == GenesisHash {
		return true
	}
	if _, exists := n.Tree[nodeHash]; exists {
		return true
	}

	visited := make(map[string]bool)
	currentHash := nodeHash
	var fetched []*TreeNode
	for currentHash != GenesisHash {
		if visited[currentHash] {
			return false
		}
		visited[currentHash] = true
		if _, exists := n.Tree[currentHash]; exists {
			break
		}
		node, exists := n.Network.FetchNode(currentHash)
		if !exists || hashNode(node) != node.Hash {
			return false
		}
		fetched = append(fetched, node)
		currentHash = node.ParentHash
	}
	for i := len(fetched) - 1; i >= 0; i-- {
		n.Tree[fetched[i].Hash] = cloneTreeNode(fetched[i])
	}
	return true
}

func (n *Node) extendsFromLocked(descendantHash, ancestorHash string) bool {
	if ancestorHash == GenesisHash {
		return true
	}
	if !n.ensureNodeLocked(descendantHash) || !n.ensureNodeLocked(ancestorHash) {
		return false
	}

	currentHash := descendantHash
	visited := make(map[string]bool)
	for currentHash != GenesisHash {
		if currentHash == ancestorHash {
			return true
		}
		if visited[currentHash] {
			return false
		}
		visited[currentHash] = true
		node, exists := n.Tree[currentHash]
		if !exists {
			return false
		}
		currentHash = node.ParentHash
	}
	return false
}

func (n *Node) executeThroughLocked(committedHash string) bool {
	if committedHash == n.LastExecutedHash {
		return true
	}
	if !n.ensureNodeLocked(committedHash) {
		return false
	}

	var reversePath []*TreeNode
	currentHash := committedHash
	visited := make(map[string]bool)
	for currentHash != n.LastExecutedHash {
		if currentHash == GenesisHash || visited[currentHash] {
			return false
		}
		visited[currentHash] = true
		node, exists := n.Tree[currentHash]
		if !exists {
			return false
		}
		reversePath = append(reversePath, node)
		currentHash = node.ParentHash
	}

	for i := len(reversePath) - 1; i >= 0; i-- {
		node := reversePath[i]
		if !n.ExecutedCommands[node.Cmd.ID] {
			n.StateMachine.Execute(node.Cmd)
			n.ExecutedCommands[node.Cmd.ID] = true
		}
		n.LastExecutedHash = node.Hash
	}
	return true
}

func (n *Node) enterViewLocked(newView int, resetBackoff bool) {
	if newView <= n.ViewNumber {
		return
	}
	oldView := n.ViewNumber
	n.ViewNumber = newView
	n.Phase = NewView
	n.sendNewViewLocked(oldView, newView)
	n.Pacemaker.Arm(newView, resetBackoff)

	buffered := n.FutureMessages[newView]
	delete(n.FutureMessages, newView)
	for _, msg := range buffered {
		if msg.IsVote {
			n.processLeaderVoteLocked(msg)
		} else {
			n.processReplicaMessageLocked(msg)
		}
	}
	n.tryProposeLocked()
}

func (n *Node) sendNewViewLocked(previousView, targetView int) {
	leaderID := n.Network.LeaderForView(targetView)
	if leaderID == "" {
		return
	}
	n.Network.Send(n.ID, leaderID, Message{
		Type:       NewView,
		ViewNumber: previousView,
		Justify:    n.PrepareQC,
	})
}

func (n *Node) nextPendingCommandLocked() (Command, bool) {
	for _, cmd := range n.PendingCommands {
		if !n.ExecutedCommands[cmd.ID] {
			return cmd, true
		}
	}
	return Command{}, false
}

func (n *Node) HasExecuted(commandID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ExecutedCommands[commandID]
}

func (n *Node) CurrentView() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ViewNumber
}

func (n *Node) VotedNode(phase MsgType, viewNumber int) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Voted[phaseViewKey{Phase: phase, View: viewNumber}]
}

func (n *Node) StateSnapshot() StateSnapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.StateMachine.Snapshot()
}

func (n *Node) VisualSnapshot() VisualNode {
	n.mu.RLock()
	defer n.mu.RUnlock()

	state := n.StateMachine.Snapshot()
	votesByPhase := make(map[MsgType]int)
	for key := range n.Voted {
		votesByPhase[key.Phase]++
	}

	return VisualNode{
		ID:               n.ID,
		ViewNumber:       n.ViewNumber,
		Phase:            n.Phase,
		PrepareQC:        visualQCRef(n.PrepareQC),
		LockedQC:         visualQCRef(n.LockedQC),
		LastExecutedHash: n.LastExecutedHash,
		PendingCommands:  len(n.PendingCommands),
		ExecutedCommands: len(n.ExecutedCommands),
		Balances:         state.Balances,
		Blocked:          state.Blocked,
		ApprovedLoans:    state.ApprovedLoans,
		VotesByPhase:     votesByPhase,
	}
}

func visualQCRef(qc *QuorumCertificate) *VisualQCRef {
	if qc == nil {
		return nil
	}
	return &VisualQCRef{Type: qc.Type, ViewNumber: qc.ViewNumber, NodeHash: qc.NodeHash}
}

func shortHash(hash string) string {
	if hash == GenesisHash {
		return GenesisHash
	}
	if len(hash) <= 10 {
		return hash
	}
	return fmt.Sprintf("%s…", hash[:10])
}
