package banking

import (
	"strings"
	"testing"
)

func TestBankingCommandExecutionBlockBeforeTransfer(t *testing.T) {
	state := DefaultState()
	res1 := state.Apply(BlockAccount("b128", "Marko", "fraud-check"))
	res2 := state.Apply(Transfer("b129", "Marko", "Ana", 500))
	res3 := state.Apply(ApproveLoan("b130", "Ana", "L-42", 2000))
	if !strings.Contains(res1, "VALID") {
		t.Fatalf("block should be valid: %s", res1)
	}
	if !strings.Contains(res2, "INVALID rejected: source account Marko is blocked") {
		t.Fatalf("transfer should be invalid after block: %s", res2)
	}
	if !strings.Contains(res3, "VALID") {
		t.Fatalf("loan should be valid: %s", res3)
	}
	if state.Balances["Marko"] != 1000 || state.Balances["Ana"] != 200 {
		t.Fatalf("balances should not change after rejected transfer: %v", state.Balances)
	}
	if state.Loans["L-42"] != 2000 {
		t.Fatal("expected approved loan in state")
	}
}
