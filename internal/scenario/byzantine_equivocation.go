package scenario

import (
	"context"
	"fmt"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/banking"
	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

func runByzantineEquivocation(ctx context.Context, cfg Config) (*Result, error) {
	primary := banking.BlockAccount("b128", "Marko", "fraud-check")
	conflict := banking.Transfer("b128-prime", "Marko", "Luka", 500)
	faults := map[string]hotstuff.FaultConfig{
		"B1": {
			Byzantine: true,
			EquivocationByView: map[int]hotstuff.EquivocationRule{
				1: {
					View:             1,
					Primary:          primary,
					Conflict:         conflict,
					PrimaryTargets:   []string{"B2", "B3"},
					ConflictTargets:  []string{"B3", "B4"},
					StopAfterPrepare: true,
				},
			},
		},
	}
	faulty := map[string]bool{"B1": true}
	result, cancel, err := buildCluster(ctx, cfg, clusterOptions{faults: faults, faultyIDs: faulty, commands: defaultCommands()})
	if err != nil {
		return nil, err
	}
	if err := waitForCorrectDecisions(ctx, result.Replicas, result.CorrectIDs, 1, 5*time.Second); err != nil {
		cancel()
		return nil, fmt.Errorf("byzantine equivocation scenario failed: %w", err)
	}
	trace := result.Replicas[0].Logger.(*Trace)
	trace.Logf("[scenario] Conflicting b128-prime did not obtain a valid QC; correct replicas later decide via view-change.")
	return finish(result, cancel, trace, 1), nil
}
