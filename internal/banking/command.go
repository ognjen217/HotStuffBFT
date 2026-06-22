package banking

import "fmt"

type CommandKind string

const (
	TransferKind     CommandKind = "TRANSFER"
	BlockAccountKind CommandKind = "BLOCK_ACCOUNT"
	ApproveLoanKind  CommandKind = "APPROVE_LOAN"
)

type Command struct {
	IDStr     string
	Kind      CommandKind
	From      string
	To        string
	AccountID string
	LoanID    string
	Amount    int
	Reason    string
}

func Transfer(id, from, to string, amount int) Command {
	return Command{IDStr: id, Kind: TransferKind, From: from, To: to, Amount: amount}
}

func BlockAccount(id, accountID, reason string) Command {
	return Command{IDStr: id, Kind: BlockAccountKind, AccountID: accountID, Reason: reason}
}

func ApproveLoan(id, accountID, loanID string, amount int) Command {
	return Command{IDStr: id, Kind: ApproveLoanKind, AccountID: accountID, LoanID: loanID, Amount: amount}
}

func (c Command) ID() string { return c.IDStr }

func (c Command) String() string {
	switch c.Kind {
	case TransferKind:
		return fmt.Sprintf("%s: TRANSFER(%s -> %s, %d)", c.IDStr, c.From, c.To, c.Amount)
	case BlockAccountKind:
		return fmt.Sprintf("%s: BLOCK_ACCOUNT(%s, reason=%q)", c.IDStr, c.AccountID, c.Reason)
	case ApproveLoanKind:
		return fmt.Sprintf("%s: APPROVE_LOAN(%s, loanId=%q, amount=%d)", c.IDStr, c.AccountID, c.LoanID, c.Amount)
	default:
		return fmt.Sprintf("%s: UNKNOWN", c.IDStr)
	}
}

func DefaultCommands() []Command {
	return []Command{
		BlockAccount("b128", "Marko", "fraud-check"),
		Transfer("b129", "Marko", "Ana", 500),
		ApproveLoan("b130", "Ana", "L-42", 2000),
	}
}
