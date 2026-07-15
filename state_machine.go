package main

import "fmt"

type StateMachine struct {
	NodeID        string
	Balances      map[string]int
	Blocked       map[string]bool
	ApprovedLoans map[string]int
}

type StateSnapshot struct {
	Balances      map[string]int
	Blocked       map[string]bool
	ApprovedLoans map[string]int
}

func NewStateMachine(nodeID string) *StateMachine {
	return &StateMachine{
		NodeID: nodeID,
		Balances: map[string]int{
			"Marko": 1000,
			"Ana":   200,
			"Luka":  100,
		},
		Blocked:       make(map[string]bool),
		ApprovedLoans: make(map[string]int),
	}
}

func (sm *StateMachine) Execute(cmd Command) bool {
	if cmd.ID == "" {
		AddLog("[Bank %s] Rejected command with empty ID.\n", sm.NodeID)
		return false
	}

	switch cmd.Type {
	case Transfer:
		if sm.Blocked[cmd.From] {
			AddLog("[Bank %s] Rejected: account '%s' is blocked.\n", sm.NodeID, cmd.From)
			return false
		}
		if cmd.Amount <= 0 || cmd.From == "" || cmd.To == "" {
			AddLog("[Bank %s] Rejected malformed transfer %s.\n", sm.NodeID, cmd.ID)
			return false
		}
		if sm.Balances[cmd.From] >= cmd.Amount {
			sm.Balances[cmd.From] -= cmd.Amount
			sm.Balances[cmd.To] += cmd.Amount
			AddLog("[Bank %s] Successful transfer: %d from %s to %s.\n", sm.NodeID, cmd.Amount, cmd.From, cmd.To)
			return true
		}
		AddLog("[Bank %s] Rejected transfer: account '%s' has insufficient funds.\n", sm.NodeID, cmd.From)
		return false

	case BlockAccount:
		if cmd.From == "" {
			AddLog("[Bank %s] Rejected malformed block command %s.\n", sm.NodeID, cmd.ID)
			return false
		}
		sm.Blocked[cmd.From] = true
		AddLog("[Bank %s] Account '%s' is BLOCKED. Reason: %s\n", sm.NodeID, cmd.From, cmd.Metadata)
		return true

	case ApproveLoan:
		if sm.Blocked[cmd.From] {
			AddLog("[Bank %s] Rejected: account '%s' is blocked.\n", sm.NodeID, cmd.From)
			return false
		}
		if cmd.From == "" || cmd.Metadata == "" || cmd.Amount <= 0 {
			AddLog("[Bank %s] Rejected malformed loan command %s.\n", sm.NodeID, cmd.ID)
			return false
		}
		if _, exists := sm.ApprovedLoans[cmd.Metadata]; exists {
			AddLog("[Bank %s] Loan '%s' was already processed.\n", sm.NodeID, cmd.Metadata)
			return false
		}
		sm.ApprovedLoans[cmd.Metadata] = cmd.Amount
		sm.Balances[cmd.From] += cmd.Amount
		AddLog("[Bank %s] Loan '%s' approved for %d to account '%s'.\n", sm.NodeID, cmd.Metadata, cmd.Amount, cmd.From)
		return true
	}

	AddLog("[Bank %s] Rejected unknown command type %q.\n", sm.NodeID, fmt.Sprint(cmd.Type))
	return false
}

func (sm *StateMachine) Snapshot() StateSnapshot {
	balances := make(map[string]int, len(sm.Balances))
	for account, value := range sm.Balances {
		balances[account] = value
	}
	blocked := make(map[string]bool, len(sm.Blocked))
	for account, value := range sm.Blocked {
		blocked[account] = value
	}
	loans := make(map[string]int, len(sm.ApprovedLoans))
	for id, value := range sm.ApprovedLoans {
		loans[id] = value
	}
	return StateSnapshot{Balances: balances, Blocked: blocked, ApprovedLoans: loans}
}
