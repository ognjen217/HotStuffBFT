package scenario

import (
	"context"
	"fmt"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

func runByzantineForgedQC(ctx context.Context, cfg Config) (*Result, error) {
	faults := map[string]hotstuff.FaultConfig{
		"B1": {
			Byzantine:     true,
			ForgedQCViews: map[int]bool{1: true},
		},
	}
	faulty := map[string]bool{"B1": true}
	result, cancel, err := buildCluster(ctx, cfg, clusterOptions{faults: faults, faultyIDs: faulty, commands: defaultCommands()})
	if err != nil {
		return nil, err
	}
	if err := waitForCorrectDecisions(ctx, result.Replicas, result.CorrectIDs, 1, 6*time.Second); err != nil {
		cancel()
		return nil, fmt.Errorf("Byzantine forged-QC scenario failed: %w", err)
	}
	trace := result.Replicas[0].Logger.(*Trace)
	trace.Logf("[scenario] Correct replicas rejected the forged compact QC and later decided under a correct leader.")
	return finish(result, cancel, trace, 1), nil
}
