package banking

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

type Execution struct {
	CommandID string
	Valid     bool
	Result    string
}

type State struct {
	Balances map[string]int
	Blocked  map[string]string
	Loans    map[string]int
	Ledger   []Execution
}

func NewState(initial map[string]int) *State {
	balances := make(map[string]int, len(initial))
	for k, v := range initial {
		balances[k] = v
	}
	return &State{Balances: balances, Blocked: make(map[string]string), Loans: make(map[string]int)}
}

func DefaultState() *State {
	return NewState(map[string]int{"Marko": 1000, "Ana": 200, "Luka": 100})
}

func (s *State) Apply(cmd hotstuff.Command) string {
	bankCmd, ok := cmd.(Command)
	if !ok {
		res := Execution{CommandID: cmd.ID(), Valid: true, Result: "NOOP/unknown command ignored"}
		s.Ledger = append(s.Ledger, res)
		return formatExecution(res)
	}
	var exec Execution
	switch bankCmd.Kind {
	case BlockAccountKind:
		exec = s.applyBlock(bankCmd)
	case TransferKind:
		exec = s.applyTransfer(bankCmd)
	case ApproveLoanKind:
		exec = s.applyLoan(bankCmd)
	default:
		exec = Execution{CommandID: bankCmd.ID(), Valid: false, Result: "unsupported banking command"}
	}
	s.Ledger = append(s.Ledger, exec)
	return formatExecution(exec)
}

func (s *State) applyBlock(cmd Command) Execution {
	if cmd.AccountID == "" {
		return Execution{CommandID: cmd.ID(), Valid: false, Result: "missing account id"}
	}
	s.Blocked[cmd.AccountID] = cmd.Reason
	return Execution{CommandID: cmd.ID(), Valid: true, Result: fmt.Sprintf("blocked %s (%s)", cmd.AccountID, cmd.Reason)}
}

func (s *State) applyTransfer(cmd Command) Execution {
	if _, ok := s.Blocked[cmd.From]; ok {
		return Execution{CommandID: cmd.ID(), Valid: false, Result: fmt.Sprintf("rejected: source account %s is blocked", cmd.From)}
	}
	if _, ok := s.Blocked[cmd.To]; ok {
		return Execution{CommandID: cmd.ID(), Valid: false, Result: fmt.Sprintf("rejected: destination account %s is blocked", cmd.To)}
	}
	if cmd.Amount <= 0 {
		return Execution{CommandID: cmd.ID(), Valid: false, Result: "rejected: amount must be positive"}
	}
	if s.Balances[cmd.From] < cmd.Amount {
		return Execution{CommandID: cmd.ID(), Valid: false, Result: fmt.Sprintf("rejected: insufficient funds in %s", cmd.From)}
	}
	s.Balances[cmd.From] -= cmd.Amount
	s.Balances[cmd.To] += cmd.Amount
	return Execution{CommandID: cmd.ID(), Valid: true, Result: fmt.Sprintf("transferred %d from %s to %s", cmd.Amount, cmd.From, cmd.To)}
}

func (s *State) applyLoan(cmd Command) Execution {
	if cmd.AccountID == "" || cmd.LoanID == "" || cmd.Amount <= 0 {
		return Execution{CommandID: cmd.ID(), Valid: false, Result: "rejected: invalid loan fields"}
	}
	s.Loans[cmd.LoanID] = cmd.Amount
	return Execution{CommandID: cmd.ID(), Valid: true, Result: fmt.Sprintf("approved loan %s for %s amount=%d", cmd.LoanID, cmd.AccountID, cmd.Amount)}
}

func formatExecution(exec Execution) string {
	status := "VALID"
	if !exec.Valid {
		status = "INVALID"
	}
	return fmt.Sprintf("%s %s", status, exec.Result)
}

func (s *State) Snapshot() string {
	parts := []string{fmt.Sprintf("balances=%s", sortedIntMap(s.Balances)), fmt.Sprintf("blocked=%s", sortedStringMap(s.Blocked)), fmt.Sprintf("loans=%s", sortedIntMap(s.Loans))}
	ledger := make([]string, 0, len(s.Ledger))
	for _, entry := range s.Ledger {
		ledger = append(ledger, fmt.Sprintf("%s:%t:%s", entry.CommandID, entry.Valid, entry.Result))
	}
	parts = append(parts, "ledger=["+strings.Join(ledger, "; ")+"]")
	return strings.Join(parts, " ")
}

func sortedIntMap(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func sortedStringMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%s", k, m[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
