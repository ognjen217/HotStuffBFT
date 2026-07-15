package main

import (
	"sort"
	"sync"
	"time"
)

const maxVisualEvents = 600

type ProtocolEvent struct {
	Sequence      uint64  `json:"sequence"`
	Timestamp     string  `json:"timestamp"`
	Kind          string  `json:"kind"`
	From          string  `json:"from"`
	To            string  `json:"to"`
	Type          MsgType `json:"type,omitempty"`
	ViewNumber    int     `json:"view_number,omitempty"`
	NodeHash      string  `json:"node_hash,omitempty"`
	JustifyType   MsgType `json:"justify_type,omitempty"`
	JustifyView   int     `json:"justify_view,omitempty"`
	IsVote        bool    `json:"is_vote,omitempty"`
	CommandID     string  `json:"command_id,omitempty"`
	CommandType   string  `json:"command_type,omitempty"`
	DeliveryState string  `json:"delivery_state"`
	Detail        string  `json:"detail,omitempty"`
}

type VisualQCRef struct {
	Type       MsgType `json:"type"`
	ViewNumber int     `json:"view_number"`
	NodeHash   string  `json:"node_hash"`
}

type VisualNode struct {
	ID               string          `json:"id"`
	ViewNumber       int             `json:"view_number"`
	Phase            MsgType         `json:"phase"`
	Role             string          `json:"role"`
	Crashed          bool            `json:"crashed"`
	PrepareQC        *VisualQCRef    `json:"prepare_qc,omitempty"`
	LockedQC         *VisualQCRef    `json:"locked_qc,omitempty"`
	LastExecutedHash string          `json:"last_executed_hash"`
	PendingCommands  int             `json:"pending_commands"`
	ExecutedCommands int             `json:"executed_commands"`
	Balances         map[string]int  `json:"balances"`
	Blocked          map[string]bool `json:"blocked"`
	ApprovedLoans    map[string]int  `json:"approved_loans"`
	VotesByPhase     map[MsgType]int `json:"votes_by_phase"`
}

type VisualQC struct {
	Type       MsgType `json:"type"`
	ViewNumber int     `json:"view_number"`
	NodeHash   string  `json:"node_hash"`
}

type VisualBlock struct {
	Hash         string     `json:"hash"`
	ParentHash   string     `json:"parent_hash"`
	ProposedView int        `json:"proposed_view"`
	Command      Command    `json:"command"`
	Status       string     `json:"status"`
	QCs          []VisualQC `json:"qcs"`
	PreparedBy   []string   `json:"prepared_by"`
	LockedBy     []string   `json:"locked_by"`
	CommittedBy  []string   `json:"committed_by"`
}

type VisualState struct {
	Scenario    string          `json:"scenario"`
	Running     bool            `json:"running"`
	StartedAt   string          `json:"started_at,omitempty"`
	FinishedAt  string          `json:"finished_at,omitempty"`
	GeneratedAt string          `json:"generated_at"`
	CurrentView int             `json:"current_view"`
	Leader      string          `json:"leader"`
	Phase       string          `json:"phase"`
	Quorum      int             `json:"quorum"`
	NodeCount   int             `json:"node_count"`
	Nodes       []VisualNode    `json:"nodes"`
	Blocks      []VisualBlock   `json:"blocks"`
	Events      []ProtocolEvent `json:"events"`
}

type visualRegistry struct {
	mu         sync.RWMutex
	cluster    *Cluster
	scenario   string
	running    bool
	startedAt  time.Time
	finishedAt time.Time
}

var activeVisualSimulation visualRegistry

func BeginVisualSimulation(cluster *Cluster, scenario string) {
	activeVisualSimulation.mu.Lock()
	activeVisualSimulation.cluster = cluster
	activeVisualSimulation.scenario = scenario
	activeVisualSimulation.running = true
	activeVisualSimulation.startedAt = time.Now()
	activeVisualSimulation.finishedAt = time.Time{}
	activeVisualSimulation.mu.Unlock()
}

func EndVisualSimulation(cluster *Cluster) {
	activeVisualSimulation.mu.Lock()
	if activeVisualSimulation.cluster == cluster {
		activeVisualSimulation.running = false
		activeVisualSimulation.finishedAt = time.Now()
	}
	activeVisualSimulation.mu.Unlock()
}

func CurrentVisualState() VisualState {
	activeVisualSimulation.mu.RLock()
	cluster := activeVisualSimulation.cluster
	scenario := activeVisualSimulation.scenario
	running := activeVisualSimulation.running
	startedAt := activeVisualSimulation.startedAt
	finishedAt := activeVisualSimulation.finishedAt
	activeVisualSimulation.mu.RUnlock()

	if cluster == nil {
		return VisualState{
			GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Phase:       "IDLE",
			Nodes:       []VisualNode{},
			Blocks:      []VisualBlock{},
			Events:      []ProtocolEvent{},
		}
	}
	return BuildVisualState(cluster, scenario, running, startedAt, finishedAt)
}

func BuildVisualState(
	cluster *Cluster,
	scenario string,
	running bool,
	startedAt time.Time,
	finishedAt time.Time,
) VisualState {
	state := VisualState{
		Scenario:    scenario,
		Running:     running,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		NodeCount:   len(cluster.Nodes),
	}
	if !startedAt.IsZero() {
		state.StartedAt = startedAt.UTC().Format(time.RFC3339Nano)
	}
	if !finishedAt.IsZero() {
		state.FinishedAt = finishedAt.UTC().Format(time.RFC3339Nano)
	}

	crashed := cluster.Network.SnapshotCrashed()
	for _, node := range cluster.Nodes {
		snapshot := node.VisualSnapshot()
		snapshot.Crashed = crashed[snapshot.ID]
		state.Nodes = append(state.Nodes, snapshot)
		if !snapshot.Crashed && snapshot.ViewNumber > state.CurrentView {
			state.CurrentView = snapshot.ViewNumber
		}
		if snapshot.Crashed && state.CurrentView == 0 && snapshot.ViewNumber > state.CurrentView {
			state.CurrentView = snapshot.ViewNumber
		}
		if state.Quorum == 0 {
			state.Quorum = node.Quorum
		}
	}
	if state.CurrentView == 0 && len(state.Nodes) > 0 {
		state.CurrentView = state.Nodes[0].ViewNumber
	}
	state.Leader = cluster.Network.LeaderForView(state.CurrentView)
	for i := range state.Nodes {
		if state.Nodes[i].ID == state.Leader && state.Nodes[i].ViewNumber == state.CurrentView {
			state.Nodes[i].Role = "leader"
			if state.Nodes[i].Crashed {
				state.Phase = "LEADER CRASHED / VIEW CHANGE"
			} else {
				state.Phase = string(state.Nodes[i].Phase)
			}
		} else {
			state.Nodes[i].Role = "replica"
		}
	}
	if state.Phase == "" {
		state.Phase = inferPhaseFromEvents(cluster.Network.SnapshotEvents(80), state.CurrentView)
	}

	state.Events = cluster.Network.SnapshotEvents(100)
	state.Blocks = buildVisualBlocks(cluster, state.Nodes)
	return state
}

func inferPhaseFromEvents(events []ProtocolEvent, currentView int) string {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Kind != "protocol" || event.ViewNumber != currentView {
			continue
		}
		if event.Type == NewView {
			return string(NewView)
		}
		return string(event.Type)
	}
	return string(NewView)
}

func buildVisualBlocks(cluster *Cluster, nodes []VisualNode) []VisualBlock {
	stored := cluster.Network.SnapshotNodeStore()
	blocks := make(map[string]*VisualBlock, len(stored))
	for hash, node := range stored {
		blocks[hash] = &VisualBlock{
			Hash:         hash,
			ParentHash:   node.ParentHash,
			ProposedView: node.ProposedView,
			Command:      node.Cmd,
			Status:       "proposed",
			QCs:          []VisualQC{},
			PreparedBy:   []string{},
			LockedBy:     []string{},
			CommittedBy:  []string{},
		}
	}

	for _, qc := range cluster.Verifier.SnapshotCertificates() {
		block := blocks[qc.NodeHash]
		if block == nil {
			continue
		}
		block.QCs = append(block.QCs, VisualQC{Type: qc.Type, ViewNumber: qc.ViewNumber, NodeHash: qc.NodeHash})
		setBlockStatus(block, statusForQC(qc.Type))
	}

	for _, node := range nodes {
		if node.PrepareQC != nil {
			if block := blocks[node.PrepareQC.NodeHash]; block != nil {
				block.PreparedBy = appendUnique(block.PreparedBy, node.ID)
				setBlockStatus(block, "prepare-qc")
			}
		}
		if node.LockedQC != nil {
			if block := blocks[node.LockedQC.NodeHash]; block != nil {
				block.LockedBy = appendUnique(block.LockedBy, node.ID)
				setBlockStatus(block, "precommit-qc")
			}
		}

		currentHash := node.LastExecutedHash
		visited := make(map[string]bool)
		for currentHash != "" && currentHash != GenesisHash && !visited[currentHash] {
			visited[currentHash] = true
			block := blocks[currentHash]
			if block == nil {
				break
			}
			block.CommittedBy = appendUnique(block.CommittedBy, node.ID)
			setBlockStatus(block, "committed")
			currentHash = block.ParentHash
		}
	}

	result := make([]VisualBlock, 0, len(blocks))
	for _, block := range blocks {
		sort.Strings(block.PreparedBy)
		sort.Strings(block.LockedBy)
		sort.Strings(block.CommittedBy)
		sort.Slice(block.QCs, func(i, j int) bool {
			if block.QCs[i].ViewNumber == block.QCs[j].ViewNumber {
				return block.QCs[i].Type < block.QCs[j].Type
			}
			return block.QCs[i].ViewNumber < block.QCs[j].ViewNumber
		})
		result = append(result, *block)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ProposedView == result[j].ProposedView {
			if result[i].Command.ID == result[j].Command.ID {
				return result[i].Hash < result[j].Hash
			}
			return result[i].Command.ID < result[j].Command.ID
		}
		return result[i].ProposedView < result[j].ProposedView
	})
	return result
}

func statusForQC(messageType MsgType) string {
	switch messageType {
	case Prepare:
		return "prepare-qc"
	case PreCommit:
		return "precommit-qc"
	case Commit:
		return "commit-qc"
	default:
		return "proposed"
	}
}

func setBlockStatus(block *VisualBlock, status string) {
	rank := map[string]int{
		"proposed":     0,
		"prepare-qc":   1,
		"precommit-qc": 2,
		"commit-qc":    3,
		"committed":    4,
	}
	if rank[status] > rank[block.Status] {
		block.Status = status
	}
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
