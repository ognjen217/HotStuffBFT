package scenario

import (
	"context"
	"fmt"
	"time"
)

func runDelayedNetwork(ctx context.Context, cfg Config) (*Result, error) {
	opts := clusterOptions{
		commands:   defaultCommands(),
		delay:      delayedBeforeGST(cfg.Timeout, cfg.Seed),
		verboseNet: cfg.Verbose,
	}
	result, cancel, err := buildCluster(ctx, cfg, opts)
	if err != nil {
		return nil, err
	}
	if err := waitForCorrectDecisions(ctx, result.Replicas, result.CorrectIDs, 1, 6*time.Second); err != nil {
		cancel()
		return nil, fmt.Errorf("delayed network scenario failed: %w", err)
	}
	trace := result.Replicas[0].Logger.(*Trace)
	trace.Logf("[scenario] Early messages were delayed beyond timeout; after GST-like stabilization, a correct leader decides.")
	return finish(result, cancel, trace, 1), nil
}
