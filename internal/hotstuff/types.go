package hotstuff

import "fmt"

type Phase string

const (
	PhaseGenesis   Phase = "GENESIS"
	PhasePrepare   Phase = "PREPARE"
	PhasePreCommit Phase = "PRECOMMIT"
	PhaseCommit    Phase = "COMMIT"
)

type MessageType string

const (
	MessageNewView   MessageType = "NEW_VIEW"
	MessagePrepare   MessageType = "PREPARE"
	MessagePreCommit MessageType = "PRECOMMIT"
	MessageCommit    MessageType = "COMMIT"
	MessageDecide    MessageType = "DECIDE"
	MessageVote      MessageType = "VOTE"
)

const Broadcast = "*"

// Command is the application payload wrapped inside a HotStuff node.
// The banking package implements this interface with concrete transfer,
// account-blocking and loan-approval commands.
type Command interface {
	ID() string
	String() string
}

// StateMachine is executed only after a node is decided.
type StateMachine interface {
	Apply(Command) string
	Snapshot() string
}

type Logger interface {
	Logf(format string, args ...any)
}

type StdoutLogger struct{}

func (StdoutLogger) Logf(format string, args ...any) { fmt.Printf(format+"\n", args...) }

type NopLogger struct{}

func (NopLogger) Logf(string, ...any) {}

func QuorumSize(n, f int) int { return n - f }

func RequiredN(f int) int { return 3*f + 1 }
