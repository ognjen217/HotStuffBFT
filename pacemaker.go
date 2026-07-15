package main

import (
	"sync"
	"time"
)

type pacemakerArm struct {
	ViewNumber   int
	ResetBackoff bool
}

type Pacemaker struct {
	Node        *Node
	BaseTimeout time.Duration
	MaxTimeout  time.Duration

	armCh  chan pacemakerArm
	stopCh chan struct{}
	once   sync.Once
}

func NewPacemaker(node *Node, baseTimeout time.Duration) *Pacemaker {
	return &Pacemaker{
		Node:        node,
		BaseTimeout: baseTimeout,
		MaxTimeout:  32 * baseTimeout,
		armCh:       make(chan pacemakerArm, 16),
		stopCh:      make(chan struct{}),
	}
}

func (p *Pacemaker) Start() {
	go p.run()
}

func (p *Pacemaker) run() {
	currentTimeout := p.BaseTimeout
	activeView := 0
	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		select {
		case request := <-p.armCh:
			stopTimer()
			if request.ResetBackoff {
				currentTimeout = p.BaseTimeout
			}
			activeView = request.ViewNumber
			timer = time.NewTimer(currentTimeout)
			timerC = timer.C

		case <-timerC:
			expiredView := activeView
			timerC = nil
			currentTimeout *= 2
			if currentTimeout > p.MaxTimeout {
				currentTimeout = p.MaxTimeout
			}
			p.Node.NotifyTimeout(expiredView)

		case <-p.stopCh:
			stopTimer()
			return
		}
	}
}

func (p *Pacemaker) Arm(viewNumber int, resetBackoff bool) {
	select {
	case p.armCh <- pacemakerArm{ViewNumber: viewNumber, ResetBackoff: resetBackoff}:
	default:
		// Keep only the newest arm request if a burst occurs.
		select {
		case <-p.armCh:
		default:
		}
		p.armCh <- pacemakerArm{ViewNumber: viewNumber, ResetBackoff: resetBackoff}
	}
}

func (p *Pacemaker) Stop() {
	p.once.Do(func() { close(p.stopCh) })
}
