package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/scenario"
)

func main() {
	name := flag.String("scenario", "happy", "scenario to run: happy, silent-leader, byzantine-equivocation, banking-block-transfer, delayed-network")
	n := flag.Int("n", 4, "number of replicas")
	f := flag.Int("f", 1, "maximum Byzantine replicas")
	timeoutMS := flag.Int("timeout-ms", 150, "view timeout in milliseconds")
	seed := flag.Int64("seed", 1, "deterministic random seed")
	verbose := flag.Bool("verbose", false, "print network-level trace entries")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := scenario.Run(ctx, scenario.Config{
		Name:    *name,
		N:       *n,
		F:       *f,
		Timeout: time.Duration(*timeoutMS) * time.Millisecond,
		Seed:    *seed,
		Verbose: *verbose,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(result.Summary())
}
