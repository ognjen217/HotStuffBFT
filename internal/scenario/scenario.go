package scenario

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/banking"
	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
	"github.com/ognjen217/HotStuffBFT/internal/network"
)

type Trace struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	Verbose bool
}

func (t *Trace) Logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf.WriteString(line)
	t.buf.WriteByte('\n')
}

func (t *Trace) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.String()
}

type Config struct {
	Name    string
	N       int
	F       int
	Timeout time.Duration
	Seed    int64
	Verbose bool
}

type Result struct {
	Name       string
	Trace      string
	Replicas   []*hotstuff.Replica
	CorrectIDs []string
	FaultyIDs  map[string]bool
	Decisions  int
}

func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.N == 0 {
		cfg.N = 4
	}
	if cfg.F == 0 {
		cfg.F = 1
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 150 * time.Millisecond
	}
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}

	switch cfg.Name {
	case "happy":
		return runHappy(ctx, cfg)
	case "silent-leader":
		return runSilentLeader(ctx, cfg)
	case "byzantine-equivocation":
		return runByzantineEquivocation(ctx, cfg)
	case "byzantine-forged-qc":
		return runByzantineForgedQC(ctx, cfg)
	case "banking-block-transfer":
		return runBankingBlockTransfer(ctx, cfg)
	case "delayed-network":
		return runDelayedNetwork(ctx, cfg)
	default:
		return nil, fmt.Errorf("unknown scenario %q", cfg.Name)
	}
}

type clusterOptions struct {
	faults     map[string]hotstuff.FaultConfig
	faultyIDs  map[string]bool
	delay      network.DelayFunc
	drop       network.DropFunc
	commands   []hotstuff.Command
	verboseNet bool
}

func buildCluster(parent context.Context, cfg Config, opts clusterOptions) (*Result, context.CancelFunc, error) {
	hsCfg, err := hotstuff.NewConfig(cfg.N, cfg.F)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	trace := &Trace{Verbose: cfg.Verbose}
	oracle := hotstuff.NewSimulatedThresholdOracle(hsCfg.ReplicaIDs, fmt.Sprintf("seed-%d", cfg.Seed))
	verifier, err := oracle.ForReplica(hsCfg.ReplicaIDs[0])
	if err != nil {
		cancel()
		return nil, nil, err
	}
	net := network.New(ctx, trace, verifier)
	net.Verbose = cfg.Verbose || opts.verboseNet
	net.Delay = opts.delay
	net.Drop = opts.drop
	net.Rand = rand.New(rand.NewSource(cfg.Seed))
	commandSource := hotstuff.NewSliceCommandSource(opts.commands)

	replicas := make([]*hotstuff.Replica, 0, cfg.N)
	for _, id := range hsCfg.ReplicaIDs {
		crypto, cryptoErr := oracle.ForReplica(id)
		if cryptoErr != nil {
			cancel()
			return nil, nil, cryptoErr
		}
		inbox := net.Register(id, 256)
		transport := net.Endpoint(id, crypto)
		replica := hotstuff.NewReplica(id, hsCfg, inbox, transport, commandSource, banking.DefaultState(), cfg.Timeout, trace, crypto)
		if fault, ok := opts.faults[id]; ok {
			replica.Faults = fault
		}
		replicas = append(replicas, replica)
	}
	for _, replica := range replicas {
		go replica.Start(ctx)
	}
	faulty := opts.faultyIDs
	if faulty == nil {
		faulty = map[string]bool{}
	}
	correct := hotstuff.SortedCorrectIDs(hsCfg.ReplicaIDs, faulty)
	return &Result{Name: cfg.Name, Replicas: replicas, CorrectIDs: correct, FaultyIDs: faulty}, cancel, nil
}

func waitForCorrectDecisions(ctx context.Context, replicas []*hotstuff.Replica, correctIDs []string, decisions int, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for %d decisions", decisions)
		case <-tick.C:
			all := true
			for _, replica := range replicas {
				if !contains(correctIDs, replica.ID) {
					continue
				}
				if replica.ExecutedCount() < decisions {
					all = false
					break
				}
			}
			if all {
				return nil
			}
		}
	}
}

func finish(result *Result, cancel context.CancelFunc, trace *Trace, decisions int) *Result {
	cancel()
	time.Sleep(20 * time.Millisecond)
	result.Trace = trace.String()
	result.Decisions = decisions
	return result
}

func (r *Result) Summary() string {
	var b strings.Builder
	b.WriteString(r.Trace)
	b.WriteString("\nFinal correct replica states:\n")
	ids := make([]string, 0, len(r.CorrectIDs))
	ids = append(ids, r.CorrectIDs...)
	sort.Strings(ids)
	for _, id := range ids {
		for _, replica := range r.Replicas {
			if replica.ID != id {
				continue
			}
			view, ledger, state, locked := replica.Snapshot()
			b.WriteString(fmt.Sprintf("[%s] view=%d locked=%s\n", id, view, locked.Short()))
			b.WriteString(fmt.Sprintf("  ledger=%v\n", ledger))
			b.WriteString(fmt.Sprintf("  state=%s\n", state))
		}
	}
	b.WriteString(fmt.Sprintf("Ledger equality among correct replicas: %v\n", hotstuff.SameLedger(r.Replicas, r.CorrectIDs)))
	return b.String()
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func defaultCommands() []hotstuff.Command {
	commands := banking.DefaultCommands()
	out := make([]hotstuff.Command, len(commands))
	for i := range commands {
		out[i] = commands[i]
	}
	return out
}

func delayedBeforeGST(timeout time.Duration, seed int64) network.DelayFunc {
	var count atomic.Int64
	rng := rand.New(rand.NewSource(seed))
	var mu sync.Mutex
	return func(msg hotstuff.Message) time.Duration {
		current := count.Add(1)
		if current <= 18 {
			return timeout + 40*time.Millisecond
		}
		mu.Lock()
		defer mu.Unlock()
		return time.Duration(rng.Intn(12)) * time.Millisecond
	}
}
