package hotstuff

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type EquivocationRule struct {
	View             int
	Primary          Command
	Conflict         Command
	PrimaryTargets   []string
	ConflictTargets  []string
	StopAfterPrepare bool
	ConflictFirst    bool
}

type FaultConfig struct {
	Byzantine          bool
	SilentViews        map[int]bool
	EquivocationByView map[int]EquivocationRule
	ForgedQCViews      map[int]bool
	SpoofVoteAs        string
}

type Replica struct {
	ID         string
	Config     Config
	Inbox      <-chan Message
	Transport  Transport
	Commands   CommandSource
	SM         StateMachine
	Timeout    time.Duration
	MaxTimeout time.Duration
	Crypto     *ReplicaCrypto
	Logger     Logger
	Faults     FaultConfig

	mu             sync.Mutex
	CurrentView    int
	currentTimeout time.Duration
	LockedQC       *QC
	PrepareQC      *QC
	Tree           *Tree

	newViews    map[int]map[string]Message
	votes       map[string]map[string]Vote
	voted       map[string]string
	proposed    map[int]bool
	broadcasted map[string]bool

	executedIDs map[string]bool
	Ledger      []string
	Decided     []string
	stopped     bool
}

func NewReplica(id string, cfg Config, inbox <-chan Message, transport Transport, commands CommandSource, sm StateMachine, timeout time.Duration, logger Logger, crypto *ReplicaCrypto) *Replica {
	if logger == nil {
		logger = NopLogger{}
	}
	if timeout <= 0 {
		timeout = 150 * time.Millisecond
	}
	maxTimeout := timeout * 16
	if maxTimeout < timeout {
		maxTimeout = timeout
	}
	return &Replica{
		ID:             id,
		Config:         cfg,
		Inbox:          inbox,
		Transport:      transport,
		Commands:       commands,
		SM:             sm,
		Timeout:        timeout,
		MaxTimeout:     maxTimeout,
		currentTimeout: timeout,
		Crypto:         crypto,
		Logger:         logger,
		CurrentView:    1,
		LockedQC:       GenesisQC(),
		PrepareQC:      GenesisQC(),
		Tree:           NewTree(),
		newViews:       make(map[int]map[string]Message),
		votes:          make(map[string]map[string]Vote),
		voted:          make(map[string]string),
		proposed:       make(map[int]bool),
		broadcasted:    make(map[string]bool),
		executedIDs:    make(map[string]bool),
	}
}

func (r *Replica) Start(ctx context.Context) {
	r.startView(1, "initial")
	for {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.stopped = true
			r.mu.Unlock()
			return
		case msg := <-r.Inbox:
			r.handle(msg)
		}
	}
}

func (r *Replica) startView(view int, reason string) {
	r.mu.Lock()
	if view < r.CurrentView {
		r.mu.Unlock()
		return
	}
	r.CurrentView = view
	leader := r.Config.LeaderForView(view)
	timeout := r.currentTimeout
	qc := r.PrepareQC.Clone()
	if qc == nil {
		qc = GenesisQC()
	}
	branch := r.Tree.BranchTo(qc.NodeID)
	r.logLocked("[%s] enter view=%d leader=%s reason=%s timeout=%s", r.ID, view, leader, reason, timeout)
	r.mu.Unlock()

	r.Transport.Send(Message{
		Type:    MessageNewView,
		From:    r.ID,
		To:      leader,
		View:    view,
		Justify: qc,
		Branch:  branch,
	})
	go r.timeoutView(view, timeout)
}

func (r *Replica) timeoutView(view int, duration time.Duration) {
	if duration <= 0 {
		return
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	<-timer.C

	r.mu.Lock()
	if r.stopped || r.CurrentView != view {
		r.mu.Unlock()
		return
	}
	previous := r.currentTimeout
	r.currentTimeout = doubledTimeout(r.currentTimeout, r.MaxTimeout)
	r.Logger.Logf("[%s] TIMEOUT view=%d -> NEW_VIEW; timeout backoff %s -> %s", r.ID, view, previous, r.currentTimeout)
	r.CurrentView++
	next := r.CurrentView
	r.mu.Unlock()
	r.startView(next, "timeout")
}

func doubledTimeout(current, maximum time.Duration) time.Duration {
	if current <= 0 {
		return current
	}
	if maximum <= 0 {
		maximum = current
	}
	if current >= maximum/2 {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func (r *Replica) handle(msg Message) {
	if !r.Config.ContainsReplica(msg.From) {
		r.Logger.Logf("[%s] rejects %s from non-member %s", r.ID, msg.Type, msg.From)
		return
	}
	switch msg.Type {
	case MessageNewView:
		r.handleNewView(msg)
	case MessagePrepare:
		r.handlePrepare(msg)
	case MessagePreCommit:
		r.handlePreCommit(msg)
	case MessageCommit:
		r.handleCommit(msg)
	case MessageDecide:
		r.handleDecide(msg)
	case MessageVote:
		r.handleVote(msg)
	}
}

func (r *Replica) handleNewView(msg Message) {
	r.mu.Lock()
	view := msg.View
	isLeader := r.ID == r.Config.LeaderForView(view)
	if !isLeader || r.Faults.SilentViews[view] {
		if isLeader && r.Faults.SilentViews[view] {
			r.Logger.Logf("[view=%d leader=%s] SILENT leader ignores NEW_VIEW from %s", view, r.ID, msg.From)
		}
		r.mu.Unlock()
		return
	}
	if view != r.CurrentView {
		r.mu.Unlock()
		return
	}
	if !r.validNewViewQCLocked(msg.Justify, view) {
		r.Logger.Logf("[view=%d leader=%s] rejects NEW_VIEW from %s with invalid/high-view QC", view, r.ID, msg.From)
		r.mu.Unlock()
		return
	}
	if err := r.Tree.ImportBranch(msg.Branch); err != nil {
		r.Logger.Logf("[view=%d leader=%s] rejects NEW_VIEW branch from %s: %v", view, r.ID, msg.From, err)
		r.mu.Unlock()
		return
	}
	if msg.Justify.NodeID != GenesisID && r.Tree.Get(msg.Justify.NodeID) == nil {
		r.Logger.Logf("[view=%d leader=%s] rejects NEW_VIEW from %s: missing highQC node %s", view, r.ID, msg.From, msg.Justify.NodeID)
		r.mu.Unlock()
		return
	}
	if r.newViews[view] == nil {
		r.newViews[view] = make(map[string]Message)
	}
	r.newViews[view][msg.From] = msg
	count := len(r.newViews[view])
	r.Logger.Logf("[view=%d leader=%s] NEW_VIEW from %s (%d/%d)", view, r.ID, msg.From, count, r.Config.Quorum())
	if count < r.Config.Quorum() || r.proposed[view] {
		r.mu.Unlock()
		return
	}
	r.proposed[view] = true
	messages := make([]Message, 0, len(r.newViews[view]))
	for _, message := range r.newViews[view] {
		messages = append(messages, message)
	}
	r.mu.Unlock()

	if r.Faults.ForgedQCViews[view] {
		r.forgeQC(view)
		return
	}
	if rule, ok := r.Faults.EquivocationByView[view]; ok {
		r.equivocate(view, rule)
		return
	}
	r.propose(view, messages)
}

func (r *Replica) validNewViewQCLocked(qc *QC, targetView int) bool {
	if qc == nil || !qc.Valid(r.Crypto, r.Config.Quorum()) {
		return false
	}
	if qc.Genesis {
		return true
	}
	return qc.Phase == PhasePrepare && qc.View < targetView
}

func (r *Replica) propose(view int, newViewMessages []Message) {
	highQC := GenesisQC()
	for _, msg := range newViewMessages {
		if msg.Justify != nil && msg.Justify.Valid(r.Crypto, r.Config.Quorum()) &&
			(msg.Justify.Genesis || msg.Justify.Phase == PhasePrepare) && msg.Justify.View > highQC.View {
			highQC = msg.Justify.Clone()
		}
	}

	r.mu.Lock()
	parent := r.Tree.Get(highQC.NodeID)
	if parent == nil {
		r.Logger.Logf("[view=%d leader=%s] cannot propose: highQC node %s is unavailable; waiting for a later view", view, r.ID, highQC.NodeID)
		r.mu.Unlock()
		return
	}
	parent = parent.Clone()
	parentBranch := r.Tree.BranchTo(parent.ID)
	r.mu.Unlock()

	cmd, ok := r.Commands.Next(view, r.ID)
	if !ok {
		r.Logger.Logf("[view=%d leader=%s] no pending banking command; leader stays idle", view, r.ID)
		return
	}
	node := NewNode(parent, cmd, r.ID, view)
	r.mu.Lock()
	if _, err := r.Tree.AddValidated(node); err != nil {
		r.Logger.Logf("[view=%d leader=%s] cannot add local proposal: %v", view, r.ID, err)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.Logger.Logf("[view=%d leader=%s] PREPARE proposal %s: %s extends %s", view, r.ID, node.ID, cmd.String(), highQC.NodeID)
	r.Transport.Broadcast(Message{
		Type:    MessagePrepare,
		From:    r.ID,
		To:      Broadcast,
		View:    view,
		Node:    node,
		Justify: highQC,
		Branch:  parentBranch,
	})
}

func (r *Replica) equivocate(view int, rule EquivocationRule) {
	r.mu.Lock()
	parent := r.Tree.Genesis().Clone()
	primary := NewNode(parent, rule.Primary, r.ID, view)
	conflict := NewNode(parent, rule.Conflict, r.ID, view)
	_, _ = r.Tree.AddValidated(primary)
	_, _ = r.Tree.AddValidated(conflict)
	branch := r.Tree.BranchTo(parent.ID)
	r.mu.Unlock()

	r.Logger.Logf("[view=%d leader=%s] BYZANTINE EQUIVOCATION primary=%s %s conflict=%s %s", view, r.ID, primary.ID, primary.Command.String(), conflict.ID, conflict.Command.String())
	sendSet := func(targets []string, node *Node, label string) {
		for _, to := range targets {
			r.Logger.Logf("[view=%d leader=%s] sends %s proposal %s to %s", view, r.ID, label, node.ID, to)
			r.Transport.Send(Message{Type: MessagePrepare, From: r.ID, To: to, View: view, Node: node, Justify: GenesisQC(), Branch: branch, Note: label})
		}
	}
	if rule.ConflictFirst {
		sendSet(rule.ConflictTargets, conflict, "conflict")
		sendSet(rule.PrimaryTargets, primary, "primary")
	} else {
		sendSet(rule.PrimaryTargets, primary, "primary")
		sendSet(rule.ConflictTargets, conflict, "conflict")
	}
	if rule.StopAfterPrepare {
		r.Logger.Logf("[view=%d leader=%s] Byzantine leader stops after equivocation; correct replicas must timeout", view, r.ID)
	}
}

func (r *Replica) forgeQC(view int) {
	forged := &QC{
		Phase:              PhasePrepare,
		View:               view,
		NodeID:             GenesisID,
		AggregateSignature: "forged-without-quorum",
	}
	r.Logger.Logf("[view=%d leader=%s] BYZANTINE FORGERY broadcasts PRECOMMIT with an invalid compact QC", view, r.ID)
	r.Transport.Broadcast(Message{Type: MessagePreCommit, From: r.ID, To: Broadcast, View: view, Justify: forged})
}

func (r *Replica) handlePrepare(msg Message) {
	r.mu.Lock()
	if msg.View != r.CurrentView {
		r.mu.Unlock()
		return
	}
	leader := r.Config.LeaderForView(msg.View)
	if msg.From != leader {
		r.Logger.Logf("[%s] rejects PREPARE from non-leader %s in view=%d", r.ID, msg.From, msg.View)
		r.mu.Unlock()
		return
	}
	if msg.Node == nil || !r.validPrepareJustificationLocked(msg.Justify, msg.View) {
		r.Logger.Logf("[%s] rejects PREPARE with invalid node/QC", r.ID)
		r.mu.Unlock()
		return
	}
	if err := r.Tree.ImportBranch(msg.Branch); err != nil {
		r.Logger.Logf("[%s] rejects PREPARE branch: %v", r.ID, err)
		r.mu.Unlock()
		return
	}
	if r.Tree.Get(msg.Justify.NodeID) == nil {
		r.Logger.Logf("[%s] rejects PREPARE: missing justify node %s", r.ID, msg.Justify.NodeID)
		r.mu.Unlock()
		return
	}
	candidate, err := r.Tree.PrepareNode(msg.Node)
	if err != nil || candidate.View != msg.View || candidate.Proposer != leader {
		r.Logger.Logf("[%s] rejects PREPARE with malformed proposal: %v", r.ID, err)
		r.mu.Unlock()
		return
	}
	parentOK := candidate.Extends(msg.Justify.NodeID)
	safe := SafeNode(candidate, msg.Justify, r.LockedQC)
	canVote := r.canVoteLocked(PhasePrepare, msg.View, candidate.ID)
	if !parentOK || !safe || !canVote {
		reasons := []string{}
		if !parentOK {
			reasons = append(reasons, "node does not extend justify.node")
		}
		if !safe {
			reasons = append(reasons, "safeNode=false")
		}
		if !canVote {
			reasons = append(reasons, "already voted conflicting PREPARE")
		}
		r.Logger.Logf("[%s] safeNode rejected %s in view=%d reason=%s", r.ID, candidate.ID, msg.View, strings.Join(reasons, ","))
		r.mu.Unlock()
		return
	}
	if _, err := r.Tree.AddValidated(candidate); err != nil {
		r.Logger.Logf("[%s] rejects PREPARE insertion: %v", r.ID, err)
		r.mu.Unlock()
		return
	}
	r.recordVoteLocked(PhasePrepare, msg.View, candidate.ID)
	r.Logger.Logf("[%s] safeNode accepted %s (%s)", r.ID, candidate.ID, candidate.Command.String())
	r.mu.Unlock()
	r.sendVote(leader, PhasePrepare, msg.View, candidate.ID)
}

func (r *Replica) validPrepareJustificationLocked(qc *QC, proposalView int) bool {
	if qc == nil || !qc.Valid(r.Crypto, r.Config.Quorum()) {
		return false
	}
	if qc.Genesis {
		return true
	}
	return qc.Phase == PhasePrepare && qc.View < proposalView
}

func (r *Replica) handlePreCommit(msg Message) {
	r.mu.Lock()
	if msg.View != r.CurrentView || msg.From != r.Config.LeaderForView(msg.View) || !r.validPhaseQCLocked(msg.Justify, PhasePrepare, msg.View) {
		r.Logger.Logf("[%s] rejects PRECOMMIT with invalid or stale prepareQC", r.ID)
		r.mu.Unlock()
		return
	}
	if err := r.Tree.ImportBranch(msg.Branch); err != nil || r.Tree.Get(msg.Justify.NodeID) == nil {
		r.Logger.Logf("[%s] rejects PRECOMMIT with unavailable branch", r.ID)
		r.mu.Unlock()
		return
	}
	if !r.canVoteLocked(PhasePreCommit, msg.View, msg.Justify.NodeID) {
		r.Logger.Logf("[%s] rejects PRECOMMIT vote: already voted conflicting node", r.ID)
		r.mu.Unlock()
		return
	}
	r.PrepareQC = msg.Justify.Clone()
	r.recordVoteLocked(PhasePreCommit, msg.View, msg.Justify.NodeID)
	r.Logger.Logf("[%s] stores prepareQC = %s", r.ID, msg.Justify.Short())
	r.mu.Unlock()
	r.sendVote(msg.From, PhasePreCommit, msg.View, msg.Justify.NodeID)
}

func (r *Replica) handleCommit(msg Message) {
	r.mu.Lock()
	if msg.View != r.CurrentView || msg.From != r.Config.LeaderForView(msg.View) || !r.validPhaseQCLocked(msg.Justify, PhasePreCommit, msg.View) {
		r.Logger.Logf("[%s] rejects COMMIT with invalid or stale precommitQC", r.ID)
		r.mu.Unlock()
		return
	}
	if err := r.Tree.ImportBranch(msg.Branch); err != nil || r.Tree.Get(msg.Justify.NodeID) == nil {
		r.Logger.Logf("[%s] rejects COMMIT with unavailable branch", r.ID)
		r.mu.Unlock()
		return
	}
	if !r.canVoteLocked(PhaseCommit, msg.View, msg.Justify.NodeID) {
		r.Logger.Logf("[%s] rejects COMMIT vote: already voted conflicting node", r.ID)
		r.mu.Unlock()
		return
	}
	r.LockedQC = msg.Justify.Clone()
	r.recordVoteLocked(PhaseCommit, msg.View, msg.Justify.NodeID)
	r.Logger.Logf("[%s] lockedQC = %s", r.ID, msg.Justify.Short())
	r.mu.Unlock()
	r.sendVote(msg.From, PhaseCommit, msg.View, msg.Justify.NodeID)
}

func (r *Replica) handleDecide(msg Message) {
	r.mu.Lock()
	if msg.View != r.CurrentView || msg.From != r.Config.LeaderForView(msg.View) || !r.validPhaseQCLocked(msg.Justify, PhaseCommit, msg.View) {
		r.Logger.Logf("[%s] rejects DECIDE with invalid or stale commitQC", r.ID)
		r.mu.Unlock()
		return
	}
	if err := r.Tree.ImportBranch(msg.Branch); err != nil || r.Tree.Get(msg.Justify.NodeID) == nil {
		r.Logger.Logf("[%s] rejects DECIDE with unavailable branch", r.ID)
		r.mu.Unlock()
		return
	}
	nodeID := msg.Justify.NodeID
	r.Logger.Logf("[%s] DECIDE %s", r.ID, nodeID)
	executed := r.executeThroughLocked(nodeID)
	for _, line := range executed {
		r.Logger.Logf("[%s] EXECUTE %s", r.ID, line)
	}
	if msg.View == r.CurrentView {
		r.currentTimeout = r.Timeout
		r.CurrentView++
		next := r.CurrentView
		r.mu.Unlock()
		r.startView(next, "decide")
		return
	}
	r.mu.Unlock()
}

func (r *Replica) validPhaseQCLocked(qc *QC, phase Phase, view int) bool {
	return qc != nil && !qc.Genesis && qc.Phase == phase && qc.View == view && qc.Valid(r.Crypto, r.Config.Quorum())
}

func (r *Replica) handleVote(msg Message) {
	if msg.Vote == nil {
		return
	}
	r.mu.Lock()
	view := msg.Vote.View
	phase := msg.Vote.Phase
	nodeID := msg.Vote.NodeID
	if msg.From != msg.Vote.VoterID || msg.View != view || !r.Config.ContainsReplica(msg.Vote.VoterID) || !r.Crypto.VerifyVote(*msg.Vote) {
		r.Logger.Logf("[leader=%s] rejects unauthenticated/spoofed vote from=%s claimed=%s", r.ID, msg.From, msg.Vote.VoterID)
		r.mu.Unlock()
		return
	}
	if r.ID != r.Config.LeaderForView(view) || view != r.CurrentView || r.Tree.Get(nodeID) == nil {
		r.mu.Unlock()
		return
	}
	if _, ok := r.Faults.EquivocationByView[view]; ok {
		r.recordVoteFromPeerLocked(*msg.Vote)
		counts := r.voteCountsLocked(phase, view)
		r.Logger.Logf("[view=%d leader=%s] equivocation vote counts phase=%s %v; no conflicting node has quorum=%d", view, r.ID, phase, counts, r.Config.Quorum())
		r.mu.Unlock()
		return
	}
	r.recordVoteFromPeerLocked(*msg.Vote)
	count := len(r.votes[voteBucket(phase, view, nodeID)])
	r.Logger.Logf("[leader=%s] vote %s for node=%s from=%s (%d/%d)", r.ID, phase, nodeID, msg.From, count, r.Config.Quorum())
	if count < r.Config.Quorum() {
		r.mu.Unlock()
		return
	}
	broadcastID := broadcastKey(phase, view, nodeID)
	if r.broadcasted[broadcastID] {
		r.mu.Unlock()
		return
	}
	r.broadcasted[broadcastID] = true
	votes := make([]Vote, 0, len(r.votes[voteBucket(phase, view, nodeID)]))
	for _, vote := range r.votes[voteBucket(phase, view, nodeID)] {
		votes = append(votes, vote)
	}
	qc, err := NewQC(phase, view, nodeID, votes, r.Config.Quorum(), r.Crypto)
	if err != nil {
		r.Logger.Logf("[leader=%s] failed to form QC: %v", r.ID, err)
		r.mu.Unlock()
		return
	}
	branch := r.Tree.BranchTo(nodeID)
	r.Logger.Logf("[leader=%s] formed %s", r.ID, qc.Short())
	r.mu.Unlock()

	switch phase {
	case PhasePrepare:
		r.Transport.Broadcast(Message{Type: MessagePreCommit, From: r.ID, To: Broadcast, View: view, Justify: qc, Branch: branch})
	case PhasePreCommit:
		r.Transport.Broadcast(Message{Type: MessageCommit, From: r.ID, To: Broadcast, View: view, Justify: qc, Branch: branch})
	case PhaseCommit:
		r.Transport.Broadcast(Message{Type: MessageDecide, From: r.ID, To: Broadcast, View: view, Justify: qc, Branch: branch})
	}
}

func (r *Replica) sendVote(to string, phase Phase, view int, nodeID string) {
	partial, err := r.Crypto.SignVote(phase, view, nodeID)
	if err != nil {
		r.Logger.Logf("[%s] cannot sign vote: %v", r.ID, err)
		return
	}
	voterID := r.ID
	if r.Faults.Byzantine && r.Faults.SpoofVoteAs != "" {
		voterID = r.Faults.SpoofVoteAs
	}
	vote := Vote{VoterID: voterID, Phase: phase, View: view, NodeID: nodeID, PartialSignature: partial}
	r.Transport.Send(Message{Type: MessageVote, From: r.ID, To: to, View: view, Vote: &vote})
}

func (r *Replica) canVoteLocked(phase Phase, view int, nodeID string) bool {
	key := fmt.Sprintf("%s:%d", phase, view)
	previous, ok := r.voted[key]
	return !ok || previous == nodeID || r.Faults.Byzantine
}

func (r *Replica) recordVoteLocked(phase Phase, view int, nodeID string) {
	key := fmt.Sprintf("%s:%d", phase, view)
	if _, ok := r.voted[key]; !ok || r.Faults.Byzantine {
		r.voted[key] = nodeID
	}
}

func (r *Replica) recordVoteFromPeerLocked(vote Vote) {
	key := voteBucket(vote.Phase, vote.View, vote.NodeID)
	if r.votes[key] == nil {
		r.votes[key] = make(map[string]Vote)
	}
	r.votes[key][vote.VoterID] = vote
}

func (r *Replica) voteCountsLocked(phase Phase, view int) map[string]int {
	out := make(map[string]int)
	prefix := fmt.Sprintf("%s:%d:", phase, view)
	for key, votes := range r.votes {
		if strings.HasPrefix(key, prefix) {
			out[strings.TrimPrefix(key, prefix)] = len(votes)
		}
	}
	return out
}

func voteBucket(phase Phase, view int, nodeID string) string {
	return fmt.Sprintf("%s:%d:%s", phase, view, nodeID)
}

func broadcastKey(phase Phase, view int, nodeID string) string {
	return fmt.Sprintf("broadcast:%s", voteBucket(phase, view, nodeID))
}

func (r *Replica) executeThroughLocked(nodeID string) []string {
	branch := r.Tree.BranchTo(nodeID)
	executed := []string{}
	for _, node := range branch {
		if node.ID == GenesisID || r.executedIDs[node.ID] {
			continue
		}
		r.executedIDs[node.ID] = true
		r.Decided = append(r.Decided, node.ID)
		if node.Command == nil {
			continue
		}
		result := r.SM.Apply(node.Command)
		line := fmt.Sprintf("%s -> %s", node.Command.String(), result)
		r.Ledger = append(r.Ledger, line)
		executed = append(executed, line)
	}
	return executed
}

func (r *Replica) highestPrepareQC() *QC {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.PrepareQC == nil {
		return GenesisQC()
	}
	return r.PrepareQC.Clone()
}

func (r *Replica) logLocked(format string, args ...any) { r.Logger.Logf(format, args...) }

func (r *Replica) Snapshot() (view int, ledger []string, state string, lockedQC *QC) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.CurrentView, append([]string{}, r.Ledger...), r.SM.Snapshot(), r.LockedQC.Clone()
}

func (r *Replica) CurrentTimeoutValue() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTimeout
}

func (r *Replica) ExecutedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.Ledger)
}

func (r *Replica) VoteCounts(phase Phase, view int) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.voteCountsLocked(phase, view)
}

func SameLedger(replicas []*Replica, ids []string) bool {
	var expected []string
	for _, replica := range replicas {
		if len(ids) > 0 && !contains(ids, replica.ID) {
			continue
		}
		_, ledger, _, _ := replica.Snapshot()
		if expected == nil {
			expected = ledger
			continue
		}
		if strings.Join(expected, "\n") != strings.Join(ledger, "\n") {
			return false
		}
	}
	return true
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func SortedCorrectIDs(all []string, faulty map[string]bool) []string {
	ids := []string{}
	for _, id := range all {
		if !faulty[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
