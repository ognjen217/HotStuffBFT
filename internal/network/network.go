package network

import (
	"context"
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
	ctx           context.Context
	mu            sync.RWMutex
	inboxes       map[string]chan hotstuff.Message
	verifier      *hotstuff.ReplicaCrypto
	RetryInterval time.Duration
	Delay         DelayFunc
	Drop          DropFunc
	Logger        Logger
	Verbose       bool
	Rand          *rand.Rand
}

type Endpoint struct {
	id      string
	network *Network
	crypto  *hotstuff.ReplicaCrypto
}

func New(ctx context.Context, logger Logger, verifier *hotstuff.ReplicaCrypto) *Network {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Network{
		ctx:           ctx,
		inboxes:       make(map[string]chan hotstuff.Message),
		verifier:      verifier,
		RetryInterval: 5 * time.Millisecond,
		Logger:        logger,
		Rand:          rand.New(rand.NewSource(1)),
	}
}

func (n *Network) Register(id string, buffer int) <-chan hotstuff.Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan hotstuff.Message, buffer)
	n.inboxes[id] = ch
	return ch
}

func (n *Network) Endpoint(id string, crypto *hotstuff.ReplicaCrypto) hotstuff.Transport {
	return &Endpoint{id: id, network: n, crypto: crypto}
}

func (e *Endpoint) Send(msg hotstuff.Message) {
	if msg.To == hotstuff.Broadcast {
		e.Broadcast(msg)
		return
	}
	e.sendOne(msg)
}

// Broadcast is implemented as authenticated point-to-point sends, including a
// send to the broadcaster itself, matching the network model in the paper.
func (e *Endpoint) Broadcast(msg hotstuff.Message) {
	if e == nil || e.network == nil {
		return
	}
	e.network.mu.RLock()
	ids := make([]string, 0, len(e.network.inboxes))
	for id := range e.network.inboxes {
		ids = append(ids, id)
	}
	e.network.mu.RUnlock()
	for _, id := range ids {
		copyMsg := msg
		copyMsg.To = id
		e.sendOne(copyMsg)
	}
}

func (e *Endpoint) sendOne(msg hotstuff.Message) {
	if e == nil || e.network == nil || e.crypto == nil {
		return
	}
	if msg.From == "" {
		msg.From = e.id
	}
	if msg.From != e.id {
		e.network.logf("[network] AUTH REJECT endpoint=%s claimed-sender=%s type=%s", e.id, msg.From, msg.Type)
		return
	}
	if msg.To == "" || msg.To == hotstuff.Broadcast {
		e.network.logf("[network] AUTH REJECT invalid point-to-point destination %q", msg.To)
		return
	}
	tag, err := e.crypto.SignMessage(msg)
	if err != nil {
		e.network.logf("[network] AUTH REJECT sign failed: %v", err)
		return
	}
	msg.AuthTag = tag
	e.network.deliver(msg)
}

func (n *Network) deliver(msg hotstuff.Message) {
	if n == nil || n.verifier == nil || !n.verifier.VerifyMessage(msg) {
		n.logf("[network] AUTH REJECT invalid message authentication: %s", msg.String())
		return
	}
	n.mu.RLock()
	_, knownDestination := n.inboxes[msg.To]
	n.mu.RUnlock()
	if !knownDestination {
		n.logf("[network] DROP unknown destination %s", msg.To)
		return
	}

	go func() {
		attempt := 0
		for {
			select {
			case <-n.ctx.Done():
				return
			default:
			}

			attempt++
			if n.Drop != nil && n.Drop(msg) {
				if n.Verbose {
					n.logf("[network] TRANSIENT DROP attempt=%d; retrying: %s", attempt, msg.String())
				}
				if !wait(n.ctx, n.RetryInterval) {
					return
				}
				continue
			}

			delay := time.Duration(0)
			if n.Delay != nil {
				delay = n.Delay(msg)
			}
			if n.Verbose {
				n.logf("[network] deliver in %s attempt=%d: %s", delay, attempt, msg.String())
			}
			if !wait(n.ctx, delay) {
				return
			}

			n.mu.RLock()
			ch := n.inboxes[msg.To]
			n.mu.RUnlock()
			if ch == nil {
				return
			}
			select {
			case <-n.ctx.Done():
				return
			case ch <- msg:
				return
			}
		}
	}()
}

func wait(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (n *Network) logf(format string, args ...any) {
	if n != nil && n.Logger != nil {
		n.Logger.Logf(format, args...)
	}
}
