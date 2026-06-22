package network

import (
	"math/rand"
	"sync"
	"time"

	"github.com/ognjen217/HotStuffBFT/internal/hotstuff"
)

type Logger interface {
	Logf(format string, args ...any)
}

type DelayFunc func(hotstuff.Message) time.Duration
type DropFunc func(hotstuff.Message) bool

type Network struct {
	mu      sync.RWMutex
	inboxes map[string]chan hotstuff.Message
	Delay   DelayFunc
	Drop    DropFunc
	Logger  Logger
	Verbose bool
	Rand    *rand.Rand
}

func New(logger Logger) *Network {
	return &Network{inboxes: make(map[string]chan hotstuff.Message), Logger: logger, Rand: rand.New(rand.NewSource(1))}
}

func (n *Network) Register(id string, buffer int) <-chan hotstuff.Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan hotstuff.Message, buffer)
	n.inboxes[id] = ch
	return ch
}

func (n *Network) Send(msg hotstuff.Message) {
	if msg.To == hotstuff.Broadcast {
		n.Broadcast(msg)
		return
	}
	n.deliver(msg)
}

func (n *Network) Broadcast(msg hotstuff.Message) {
	n.mu.RLock()
	ids := make([]string, 0, len(n.inboxes))
	for id := range n.inboxes {
		ids = append(ids, id)
	}
	n.mu.RUnlock()
	for _, id := range ids {
		copyMsg := msg
		copyMsg.To = id
		n.deliver(copyMsg)
	}
}

func (n *Network) deliver(msg hotstuff.Message) {
	if n.Drop != nil && n.Drop(msg) {
		if n.Verbose && n.Logger != nil {
			n.Logger.Logf("[network] DROP %s", msg.String())
		}
		return
	}
	delay := time.Duration(0)
	if n.Delay != nil {
		delay = n.Delay(msg)
	}
	if n.Verbose && n.Logger != nil {
		n.Logger.Logf("[network] deliver in %s: %s", delay, msg.String())
	}
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		n.mu.RLock()
		ch := n.inboxes[msg.To]
		n.mu.RUnlock()
		if ch == nil {
			return
		}
		ch <- msg
	}()
}
