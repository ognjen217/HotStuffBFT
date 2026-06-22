package scenario

import (
	"context"
	"fmt"
	"time"
)

func runBankingBlockTransfer(ctx context.Context, cfg Config) (*Result, error) {
	result, cancel, err := buildCluster(ctx, cfg, clusterOptions{commands: defaultCommands()})
	if err != nil {
		return nil, err
	}
	if err := waitForCorrectDecisions(ctx, result.Replicas, result.CorrectIDs, 3, 4*time.Second); err != nil {
		cancel()
		return nil, fmt.Errorf("banking block-transfer scenario failed: %w", err)
	}
	trace := result.Replicas[0].Logger.(*Trace)
	trace.Logf("[scenario] b128 blocks Marko before b129; deterministic execution rejects b129 on every correct replica.")
	return finish(result, cancel, trace, 3), nil
}
