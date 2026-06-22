package scenario

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

func TestHappyPathFinalLedgerEquality(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	res, err := Run(ctx, Config{Name: "happy", Timeout: 80 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if !hotstuff.SameLedger(res.Replicas, res.CorrectIDs) {
		t.Fatal("correct replicas did not end with equal ledgers")
	}
	if !strings.Contains(res.Summary(), "b129") || !strings.Contains(res.Summary(), "INVALID rejected: source account Marko is blocked") {
		t.Fatal("expected block-before-transfer invalid execution in summary")
	}
}

func TestByzantineEquivocationSafety(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	res, err := Run(ctx, Config{Name: "byzantine-equivocation", Timeout: 80 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if !hotstuff.SameLedger(res.Replicas, res.CorrectIDs) {
		t.Fatal("correct replicas diverged after Byzantine equivocation")
	}
	if !strings.Contains(res.Trace, "safeNode rejected") {
		t.Fatal("expected trace to include rejection of conflicting proposal")
	}
}

func TestSilentLeaderViewChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	res, err := Run(ctx, Config{Name: "silent-leader", Timeout: 70 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if !hotstuff.SameLedger(res.Replicas, res.CorrectIDs) {
		t.Fatal("correct replicas diverged after silent leader")
	}
	if !strings.Contains(res.Trace, "SILENT leader") || !strings.Contains(res.Trace, "TIMEOUT") {
		t.Fatal("expected trace to show silent leader and timeout")
	}
}
