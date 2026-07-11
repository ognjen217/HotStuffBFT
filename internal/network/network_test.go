package network

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

func testNetwork(t *testing.T) (context.CancelFunc, *Network, hotstuff.Transport, hotstuff.Transport, <-chan hotstuff.Message, <-chan hotstuff.Message) {
	t.Helper()
	ids := []string{"B1", "B2", "B3", "B4"}
	oracle := hotstuff.NewSimulatedThresholdOracle(ids, "network-test")
	c1, _ := oracle.ForReplica("B1")
	c2, _ := oracle.ForReplica("B2")
	ctx, cancel := context.WithCancel(context.Background())
	net := New(ctx, nil, c1)
	in1 := net.Register("B1", 8)
	in2 := net.Register("B2", 8)
	return cancel, net, net.Endpoint("B1", c1), net.Endpoint("B2", c2), in1, in2
}

func TestEndpointRejectsSenderSpoofing(t *testing.T) {
	cancel, _, endpoint1, _, _, inbox2 := testNetwork(t)
	defer cancel()
	endpoint1.Send(hotstuff.Message{Type: hotstuff.MessageNewView, From: "B2", To: "B2", View: 1, Justify: hotstuff.GenesisQC()})
	select {
	case msg := <-inbox2:
		t.Fatalf("spoofed message was delivered: %v", msg)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestAuthenticatedMessageIsDelivered(t *testing.T) {
	cancel, _, endpoint1, _, _, inbox2 := testNetwork(t)
	defer cancel()
	endpoint1.Send(hotstuff.Message{Type: hotstuff.MessageNewView, From: "B1", To: "B2", View: 1, Justify: hotstuff.GenesisQC()})
	select {
	case msg := <-inbox2:
		if msg.From != "B1" || msg.AuthTag == "" {
			t.Fatalf("unexpected delivered message: %+v", msg)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("authenticated message was not delivered")
	}
}

func TestTransientDropsAreRetriedUntilDelivery(t *testing.T) {
	cancel, net, endpoint1, _, _, inbox2 := testNetwork(t)
	defer cancel()
	var attempts atomic.Int32
	net.RetryInterval = time.Millisecond
	net.Drop = func(hotstuff.Message) bool {
		return attempts.Add(1) <= 2
	}
	endpoint1.Send(hotstuff.Message{Type: hotstuff.MessageNewView, From: "B1", To: "B2", View: 1, Justify: hotstuff.GenesisQC()})
	select {
	case <-inbox2:
		if attempts.Load() < 3 {
			t.Fatalf("expected retries, got %d attempts", attempts.Load())
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("message was not eventually delivered after transient drops")
	}
}
