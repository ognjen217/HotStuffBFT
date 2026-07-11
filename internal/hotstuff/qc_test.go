package hotstuff

import "testing"

func testCrypto(t *testing.T, ids []string) (*SimulatedThresholdOracle, map[string]*ReplicaCrypto) {
	t.Helper()
	oracle := NewSimulatedThresholdOracle(ids, "test-seed")
	out := make(map[string]*ReplicaCrypto, len(ids))
	for _, id := range ids {
		crypto, err := oracle.ForReplica(id)
		if err != nil {
			t.Fatal(err)
		}
		out[id] = crypto
	}
	return oracle, out
}

func signedVotes(t *testing.T, crypto map[string]*ReplicaCrypto, ids []string, phase Phase, view int, nodeID string) []Vote {
	t.Helper()
	votes := make([]Vote, 0, len(ids))
	for _, id := range ids {
		partial, err := crypto[id].SignVote(phase, view, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		votes = append(votes, Vote{VoterID: id, Phase: phase, View: view, NodeID: nodeID, PartialSignature: partial})
	}
	return votes
}

func TestQuorumSize(t *testing.T) {
	if got := QuorumSize(4, 1); got != 3 {
		t.Fatalf("quorum got %d want 3", got)
	}
	if got := RequiredN(2); got != 7 {
		t.Fatalf("required n got %d want 7", got)
	}
}

func TestCompactQCValidity(t *testing.T) {
	ids := []string{"B1", "B2", "B3", "B4"}
	_, crypto := testCrypto(t, ids)
	votes := signedVotes(t, crypto, ids[:3], PhasePrepare, 2, "n1")
	qc, err := NewQC(PhasePrepare, 2, "n1", votes, 3, crypto["B1"])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !qc.Valid(crypto["B4"], 3) {
		t.Fatal("expected compact aggregate QC to be valid")
	}
	if len(qc.AggregateSignature) != 64 {
		t.Fatalf("expected one fixed-size SHA-256 aggregate, got %d hex chars", len(qc.AggregateSignature))
	}
}

func TestQCRejectsDuplicateMismatchedOrForgedVotes(t *testing.T) {
	ids := []string{"B1", "B2", "B3", "B4"}
	_, crypto := testCrypto(t, ids)
	valid := signedVotes(t, crypto, ids[:2], PhasePrepare, 2, "n1")
	duplicate := append(valid, valid[0])
	if _, err := NewQC(PhasePrepare, 2, "n1", duplicate, 3, crypto["B1"]); err == nil {
		t.Fatal("expected duplicate voters to fail quorum")
	}

	mismatch := signedVotes(t, crypto, ids[:3], PhasePrepare, 2, "n1")
	mismatch[1].Phase = PhaseCommit
	if _, err := NewQC(PhasePrepare, 2, "n1", mismatch, 3, crypto["B1"]); err == nil {
		t.Fatal("expected mismatched vote phase to fail")
	}

	forged := &QC{Phase: PhasePrepare, View: 2, NodeID: "n1", AggregateSignature: "forged"}
	if forged.Valid(crypto["B1"], 3) {
		t.Fatal("forged aggregate must not validate")
	}
}

func TestVoteSignatureBindsVoterAndTuple(t *testing.T) {
	ids := []string{"B1", "B2", "B3", "B4"}
	_, crypto := testCrypto(t, ids)
	partial, err := crypto["B1"].SignVote(PhasePrepare, 1, "n1")
	if err != nil {
		t.Fatal(err)
	}
	vote := Vote{VoterID: "B1", Phase: PhasePrepare, View: 1, NodeID: "n1", PartialSignature: partial}
	if !crypto["B2"].VerifyVote(vote) {
		t.Fatal("valid partial signature rejected")
	}
	vote.VoterID = "B2"
	if crypto["B2"].VerifyVote(vote) {
		t.Fatal("same signature must not authenticate a different voter")
	}
}
