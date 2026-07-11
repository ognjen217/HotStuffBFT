package hotstuff

import "testing"

type testCommand string

func (c testCommand) ID() string     { return string(c) }
func (c testCommand) String() string { return string(c) }

func TestSafeNodeAcceptsExtensionOfLockedNode(t *testing.T) {
	genesis := NewGenesisNode()
	lockedNode := NewNode(genesis, testCommand("a"), "B1", 1)
	child := NewNode(lockedNode, testCommand("b"), "B2", 2)
	lockedQC := &QC{Phase: PhasePreCommit, View: 1, NodeID: lockedNode.ID}
	justify := &QC{Phase: PhasePrepare, View: 1, NodeID: lockedNode.ID}
	if !SafeNode(child, justify, lockedQC) {
		t.Fatal("expected extension of locked node to be safe")
	}
}

func TestSafeNodeRejectsConflictWithStaleQC(t *testing.T) {
	genesis := NewGenesisNode()
	lockedNode := NewNode(genesis, testCommand("a"), "B1", 1)
	conflict := NewNode(genesis, testCommand("x"), "B2", 2)
	lockedQC := &QC{Phase: PhasePreCommit, View: 3, NodeID: lockedNode.ID}
	justify := &QC{Phase: PhasePrepare, View: 2, NodeID: GenesisID}
	if SafeNode(conflict, justify, lockedQC) {
		t.Fatal("expected conflicting node with stale QC to be rejected")
	}
}

func TestSafeNodeAcceptsConflictWithHigherQC(t *testing.T) {
	genesis := NewGenesisNode()
	lockedNode := NewNode(genesis, testCommand("a"), "B1", 1)
	conflict := NewNode(genesis, testCommand("x"), "B2", 4)
	lockedQC := &QC{Phase: PhasePreCommit, View: 3, NodeID: lockedNode.ID}
	justify := &QC{Phase: PhasePrepare, View: 4, NodeID: GenesisID}
	if !SafeNode(conflict, justify, lockedQC) {
		t.Fatal("expected conflicting node with higher QC to be accepted")
	}
}
