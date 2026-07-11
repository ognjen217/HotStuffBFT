package hotstuff

import (
	"testing"
	"time"
)

type captureTransport struct {
	sent       []Message
	broadcasts []Message
}

func (c *captureTransport) Send(msg Message)      { c.sent = append(c.sent, msg) }
func (c *captureTransport) Broadcast(msg Message) { c.broadcasts = append(c.broadcasts, msg) }

func TestPacemakerTimeoutUsesExponentialBackoffAndCap(t *testing.T) {
	base := 100 * time.Millisecond
	max := 800 * time.Millisecond
	if got := doubledTimeout(base, max); got != 200*time.Millisecond {
		t.Fatalf("first backoff got %s", got)
	}
	if got := doubledTimeout(400*time.Millisecond, max); got != max {
		t.Fatalf("cap got %s want %s", got, max)
	}
	if got := doubledTimeout(max, max); got != max {
		t.Fatalf("timeout must stay capped, got %s", got)
	}
}

func TestStalePhaseQCIsRejected(t *testing.T) {
	cfg, err := NewConfig(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	oracle, crypto := testCrypto(t, cfg.ReplicaIDs)
	_ = oracle
	votes := signedVotes(t, crypto, cfg.ReplicaIDs[:3], PhasePrepare, 1, "n1")
	qc, err := NewQC(PhasePrepare, 1, "n1", votes, cfg.Quorum(), crypto["B1"])
	if err != nil {
		t.Fatal(err)
	}
	replica := NewReplica("B1", cfg, nil, &captureTransport{}, NewSliceCommandSource(nil), nil, time.Second, NopLogger{}, crypto["B1"])
	if replica.validPhaseQCLocked(qc, PhasePrepare, 2) {
		t.Fatal("prepareQC from view 1 must not satisfy PRECOMMIT in view 2")
	}
}

func TestLeaderDoesNotFallBackToGenesisWhenHighQCNodeIsMissing(t *testing.T) {
	cfg, err := NewConfig(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, crypto := testCrypto(t, cfg.ReplicaIDs)
	votes := signedVotes(t, crypto, cfg.ReplicaIDs[:3], PhasePrepare, 1, "missing-node")
	qc, err := NewQC(PhasePrepare, 1, "missing-node", votes, cfg.Quorum(), crypto["B1"])
	if err != nil {
		t.Fatal(err)
	}
	transport := &captureTransport{}
	commands := NewSliceCommandSource([]Command{testCommand("must-not-be-consumed")})
	replica := NewReplica("B2", cfg, nil, transport, commands, nil, time.Second, NopLogger{}, crypto["B2"])
	replica.propose(2, []Message{{Type: MessageNewView, From: "B1", To: "B2", View: 2, Justify: qc}})
	if len(transport.broadcasts) != 0 {
		t.Fatal("leader proposed from genesis even though the highest QC node was missing")
	}
	if _, ok := commands.Next(2, "B2"); !ok {
		t.Fatal("command was consumed even though no safe proposal could be created")
	}
}

func TestSpoofedVoteIsRejectedBeforeCounting(t *testing.T) {
	cfg, _ := NewConfig(4, 1)
	_, crypto := testCrypto(t, cfg.ReplicaIDs)
	transport := &captureTransport{}
	replica := NewReplica("B1", cfg, nil, transport, NewSliceCommandSource(nil), nil, time.Second, NopLogger{}, crypto["B1"])
	node := NewNode(replica.Tree.Genesis(), testCommand("x"), "B1", 1)
	replica.Tree.Add(node)
	partial, err := crypto["B2"].SignVote(PhasePrepare, 1, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	spoofed := Vote{VoterID: "B3", Phase: PhasePrepare, View: 1, NodeID: node.ID, PartialSignature: partial}
	replica.handleVote(Message{Type: MessageVote, From: "B2", To: "B1", View: 1, Vote: &spoofed})
	if got := len(replica.votes[voteBucket(PhasePrepare, 1, node.ID)]); got != 0 {
		t.Fatalf("spoofed vote was counted: %d", got)
	}
}
