package hotstuff

import (
	"fmt"
	"sync"
)

type Transport interface {
	Send(Message)
	Broadcast(Message)
}

type CommandSource interface {
	Next(view int, leader string) (Command, bool)
}

type SliceCommandSource struct {
	mu       sync.Mutex
	commands []Command
	idx      int
}

func NewSliceCommandSource(commands []Command) *SliceCommandSource {
	return &SliceCommandSource{commands: append([]Command{}, commands...)}
}

func (s *SliceCommandSource) Next(int, string) (Command, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.commands) {
		return nil, false
	}
	cmd := s.commands[s.idx]
	s.idx++
	return cmd, true
}

type NoopCommand struct{}

func (NoopCommand) ID() string     { return "noop" }
func (NoopCommand) String() string { return "NOOP" }

type Config struct {
	N          int
	F          int
	ReplicaIDs []string
}

func NewConfig(n, f int) (Config, error) {
	if n < RequiredN(f) {
		return Config{}, fmt.Errorf("invalid BFT configuration: n=%d, f=%d, need n >= 3f+1 = %d", n, f, RequiredN(f))
	}
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("B%d", i+1)
	}
	return Config{N: n, F: f, ReplicaIDs: ids}, nil
}

func (c Config) Quorum() int { return QuorumSize(c.N, c.F) }

func (c Config) LeaderForView(view int) string {
	if len(c.ReplicaIDs) == 0 {
		return ""
	}
	idx := (view - 1) % len(c.ReplicaIDs)
	if idx < 0 {
		idx = 0
	}
	return c.ReplicaIDs[idx]
}

func (c Config) ContainsReplica(id string) bool {
	for _, candidate := range c.ReplicaIDs {
		if candidate == id {
			return true
		}
	}
	return false
}
