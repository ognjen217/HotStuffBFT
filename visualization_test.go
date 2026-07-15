package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestVisualStateReflectsProtocolExecution(t *testing.T) {
	cluster, err := NewCluster(0, 2, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Stop()
	time.Sleep(30 * time.Millisecond)

	cmd := Command{ID: "visual-1", Type: Transfer, From: "Marko", To: "Ana", Amount: 25}
	cluster.Network.BroadcastClientCommand(cmd)
	if !cluster.WaitForExecution(cmd.ID, 3*time.Second) {
		t.Fatal("command did not commit")
	}

	state := BuildVisualState(cluster, "test", true, time.Now(), time.Time{})
	if len(state.Nodes) != 4 {
		t.Fatalf("got %d visual nodes, want 4", len(state.Nodes))
	}
	if len(state.Events) == 0 {
		t.Fatal("visual state contains no protocol events")
	}
	if len(state.Blocks) == 0 {
		t.Fatal("visual state contains no proposal blocks")
	}

	committed := false
	for _, block := range state.Blocks {
		if block.Command.ID == cmd.ID && block.Status == "committed" {
			committed = true
			break
		}
	}
	if !committed {
		t.Fatal("committed command is not marked as committed in the visual tree")
	}
	if _, err := json.Marshal(state); err != nil {
		t.Fatalf("visual state is not JSON serializable: %v", err)
	}
}
