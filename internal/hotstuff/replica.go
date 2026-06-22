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
}

type Replica struct {
	ID        string
	Config    Config
	Inbox     <-chan Message
	Transport Transport
	Commands  CommandSource
	SM        StateMachine
	Timeout   time.Duration
	Logger    Logger
	Faults    FaultConfig

	mu          sync.Mutex
	CurrentView int
	LockedQC    *QC
	PrepareQC   *QC
	Tree        *Tree

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

func NewReplica(id string, cfg Config, inbox <-chan Message, transport Transport, commands CommandSource, sm StateMachine, timeout time.Duration, logger Logger) *Replica {
	if logger == nil {
		logger = NopLogger{}
	}
	return &Replica{
		ID:          id,
		Config:      cfg,
		Inbox:       inbox,
		Transport:   transport,
		Commands:    commands,
		SM:          sm,
		Timeout:     timeout,
		Logger:      logger,
		CurrentView: 1,
		LockedQC:    GenesisQC(),
		PrepareQC:   GenesisQC(),
		Tree:        NewTree(),
		newViews:    make(map[int]map[string]Message),
		votes:       make(map[string]map[string]Vote),
		voted:       make(map[string]string),
		proposed:    make(map[int]bool),
		broadcasted: make(map[string]bool),
		executedIDs: make(map[string]bool),
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
	r.logLocked("[%s] enter view=%d leader=%s reason=%s", r.ID, view, leader, reason)
	r.mu.Unlock()

	r.Transport.Send(Message{Type: MessageNewView, From: r.ID, To: leader, View: view, Justify: r.highestPrepareQC()})
	go r.timeoutView(view)
}

func (r *Replica) timeoutView(view int) {
	if r.Timeout <= 0 {
		return
	}
	t := time.NewTimer(r.Timeout)
	defer t.Stop()
	<-t.C
	r.mu.Lock()
	if r.stopped || r.CurrentView != view {
		r.mu.Unlock()
		return
	}
	r.Logger.Logf("[%s] TIMEOUT view=%d -> NEW_VIEW", r.ID, view)
	r.CurrentView++
	next := r.CurrentView
	r.mu.Unlock()
	r.startView(next, "timeout")
}

func (r *Replica) handle(msg Message) {
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
	for _, m := range r.newViews[view] {
		messages = append(messages, m)
	}
	r.mu.Unlock()

	if rule, ok := r.Faults.EquivocationByView[view]; ok {
		r.equivocate(view, rule)
		return
	}
	r.propose(view, messages)
}

func (r *Replica) propose(view int, newViewMessages []Message) {
	highQC := GenesisQC()
	for _, m := range newViewMessages {
		if m.Justify != nil && m.Justify.Valid() && m.Justify.View > highQC.View {
			highQC = m.Justify.Clone()
		}
	}
	cmd, ok := r.Commands.Next(view, r.ID)
	if !ok {
		r.Logger.Logf("[view=%d leader=%s] no pending banking command; leader stays idle", view, r.ID)
		return
	}
	r.mu.Lock()
	parent := r.Tree.Get(highQC.NodeID)
	if parent == nil {
		parent = r.Tree.Genesis()
	}
	node := NewNode(parent, cmd, r.ID, view)
	r.Tree.Add(node)
	r.mu.Unlock()
	r.Logger.Logf("[view=%d leader=%s] PREPARE proposal %s: %s extends %s", view, r.ID, node.ID, cmd.String(), highQC.NodeID)
	r.Transport.Broadcast(Message{Type: MessagePrepare, From: r.ID, To: Broadcast, View: view, Node: node, Justify: highQC})
}

func (r *Replica) equivocate(view int, rule EquivocationRule) {
	r.mu.Lock()
	parent := r.Tree.Genesis()
	primary := NewNode(parent, rule.Primary, r.ID, view)
	conflict := NewNode(parent, rule.Conflict, r.ID, view)
	r.Tree.Add(primary)
	r.Tree.Add(conflict)
	r.mu.Unlock()
	r.Logger.Logf("[view=%d leader=%s] BYZANTINE EQUIVOCATION primary=%s %s conflict=%s %s", view, r.ID, primary.ID, primary.Command.String(), conflict.ID, conflict.Command.String())
	sendSet := func(targets []string, node *Node, label string) {
		for _, to := range targets {
			r.Logger.Logf("[view=%d leader=%s] sends %s proposal %s to %s", view, r.ID, label, node.ID, to)
			r.Transport.Send(Message{Type: MessagePrepare, From: r.ID, To: to, View: view, Node: node, Justify: GenesisQC(), Note: label})
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
	if msg.Node == nil || msg.Justify == nil || !msg.Justify.Valid() {
		r.Logger.Logf("[%s] rejects PREPARE with invalid node/QC", r.ID)
		r.mu.Unlock()
		return
	}
	r.Tree.Add(msg.Node)
	parentOK := msg.Node.Extends(msg.Justify.NodeID)
	safe := SafeNode(msg.Node, msg.Justify, r.LockedQC)
	canVote := r.canVoteLocked(PhasePrepare, msg.View, msg.Node.ID)
	if !parentOK || !safe || !canVote {
		reason := []string{}
		if !parentOK {
			reason = append(reason, "node does not extend justify.node")
		}
		if !safe {
			reason = append(reason, "safeNode=false")
		}
		if !canVote {
			reason = append(reason, "already voted conflicting PREPARE")
		}
		r.Logger.Logf("[%s] safeNode rejected %s in view=%d reason=%s", r.ID, msg.Node.ID, msg.View, strings.Join(reason, ","))
		r.mu.Unlock()
		return
	}
	r.recordVoteLocked(PhasePrepare, msg.View, msg.Node.ID)
	r.Logger.Logf("[%s] safeNode accepted %s (%s)", r.ID, msg.Node.ID, msg.Node.Command.String())
	r.mu.Unlock()
	r.sendVote(leader, PhasePrepare, msg.View, msg.Node.ID)
}

func (r *Replica) handlePreCommit(msg Message) {
	r.mu.Lock()
	if msg.View != r.CurrentView || msg.From != r.Config.LeaderForView(msg.View) || msg.Justify == nil || !msg.Justify.Valid() || msg.Justify.Phase != PhasePrepare {
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
	if msg.View != r.CurrentView || msg.From != r.Config.LeaderForView(msg.View) || msg.Justify == nil || !msg.Justify.Valid() || msg.Justify.Phase != PhasePreCommit {
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
	if msg.View != r.CurrentView || msg.From != r.Config.LeaderForView(msg.View) || msg.Justify == nil || !msg.Justify.Valid() || msg.Justify.Phase != PhaseCommit {
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
		r.CurrentView++
		next := r.CurrentView
		r.mu.Unlock()
		r.startView(next, "decide")
		return
	}
	r.mu.Unlock()
}

func (r *Replica) handleVote(msg Message) {
	if msg.Vote == nil {
		return
	}
	r.mu.Lock()
	view := msg.Vote.View
	phase := msg.Vote.Phase
	nodeID := msg.Vote.NodeID
	if r.ID != r.Config.LeaderForView(view) || view != r.CurrentView {
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
	bkey := broadcastKey(phase, view, nodeID)
	if r.broadcasted[bkey] {
		r.mu.Unlock()
		return
	}
	r.broadcasted[bkey] = true
	votes := make([]Vote, 0, len(r.votes[voteBucket(phase, view, nodeID)]))
	for _, vote := range r.votes[voteBucket(phase, view, nodeID)] {
		votes = append(votes, vote)
	}
	qc, err := NewQC(phase, view, nodeID, votes, r.Config.Quorum())
	if err != nil {
		r.Logger.Logf("[leader=%s] failed to form QC: %v", r.ID, err)
		r.mu.Unlock()
		return
	}
	r.Logger.Logf("[leader=%s] formed %s", r.ID, qc.Short())
	r.mu.Unlock()

	switch phase {
	case PhasePrepare:
		r.Transport.Broadcast(Message{Type: MessagePreCommit, From: r.ID, To: Broadcast, View: view, Justify: qc})
	case PhasePreCommit:
		r.Transport.Broadcast(Message{Type: MessageCommit, From: r.ID, To: Broadcast, View: view, Justify: qc})
	case PhaseCommit:
		r.Transport.Broadcast(Message{Type: MessageDecide, From: r.ID, To: Broadcast, View: view, Justify: qc})
	}
}

func (r *Replica) sendVote(to string, phase Phase, view int, nodeID string) {
	vote := Vote{VoterID: r.ID, Phase: phase, View: view, NodeID: nodeID}
	r.Transport.Send(Message{Type: MessageVote, From: r.ID, To: to, View: view, Vote: &vote})
}

func (r *Replica) canVoteLocked(phase Phase, view int, nodeID string) bool {
	key := fmt.Sprintf("%s:%d", phase, view)
	prev, ok := r.voted[key]
	return !ok || prev == nodeID || r.Faults.Byzantine
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
	for _, r := range replicas {
		if len(ids) > 0 && !contains(ids, r.ID) {
			continue
		}
		_, ledger, _, _ := r.Snapshot()
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
	for _, v := range values {
		if v == target {
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
