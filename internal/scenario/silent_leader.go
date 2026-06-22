package scenario

import (
	"context"
	"fmt"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

func runSilentLeader(ctx context.Context, cfg Config) (*Result, error) {
	faults := map[string]hotstuff.FaultConfig{
		"B1": {SilentViews: map[int]bool{1: true}},
	}
	result, cancel, err := buildCluster(ctx, cfg, clusterOptions{faults: faults, commands: defaultCommands()})
	if err != nil {
		return nil, err
	}
	if err := waitForCorrectDecisions(ctx, result.Replicas, result.CorrectIDs, 3, 5*time.Second); err != nil {
		cancel()
		return nil, fmt.Errorf("silent leader scenario failed: %w", err)
	}
	trace := result.Replicas[0].Logger.(*Trace)
	return finish(result, cancel, trace, 3), nil
}
