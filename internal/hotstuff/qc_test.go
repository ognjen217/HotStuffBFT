package hotstuff

import "testing"

func TestQuorumSize(t *testing.T) {
	if got := QuorumSize(4, 1); got != 3 {
		t.Fatalf("quorum got %d want 3", got)
	}
	if got := RequiredN(2); got != 7 {
		t.Fatalf("required n got %d want 7", got)
	}
}

func TestQCValidity(t *testing.T) {
	votes := []Vote{
		{VoterID: "B1", Phase: PhasePrepare, View: 2, NodeID: "n1"},
		{VoterID: "B2", Phase: PhasePrepare, View: 2, NodeID: "n1"},
		{VoterID: "B3", Phase: PhasePrepare, View: 2, NodeID: "n1"},
	}
	qc, err := NewQC(PhasePrepare, 2, "n1", votes, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !qc.Valid() {
		t.Fatal("expected QC to be valid")
	}
}

func TestQCRejectsDuplicateOrMismatchedVotes(t *testing.T) {
	_, err := NewQC(PhasePrepare, 2, "n1", []Vote{
		{VoterID: "B1", Phase: PhasePrepare, View: 2, NodeID: "n1"},
		{VoterID: "B1", Phase: PhasePrepare, View: 2, NodeID: "n1"},
		{VoterID: "B2", Phase: PhasePrepare, View: 2, NodeID: "n1"},
	}, 3)
	if err == nil {
		t.Fatal("expected duplicate voters to fail quorum")
	}
	_, err = NewQC(PhasePrepare, 2, "n1", []Vote{
		{VoterID: "B1", Phase: PhasePrepare, View: 2, NodeID: "n1"},
		{VoterID: "B2", Phase: PhaseCommit, View: 2, NodeID: "n1"},
		{VoterID: "B3", Phase: PhasePrepare, View: 2, NodeID: "n1"},
	}, 3)
	if err == nil {
		t.Fatal("expected mismatched vote phase to fail")
	}
}
