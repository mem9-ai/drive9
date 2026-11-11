package fuse

import (
	"context"
	"errors"
	"sync"
)

var (
	errViewAdmissionPaused = errors.New("mount view admission paused")
	errViewPauseLost       = errors.New("mount view pause no longer owned")
	errViewCallbacksActive = errors.New("mount callbacks still active")
)

// viewAdmission counts complete foreground operations, including their local
// cache/handle updates. It does not replace draining background upload queues
// or protecting old handles. Its zero value admits requests.
type viewAdmission struct {
	mu     sync.Mutex
	active int
	paused *viewPause
}

// enter returns a paused error rather than waiting for resumption: a caller
// must not occupy a FUSE worker waiting for kernel notifications to complete.
// The adapter must handle cleanup callbacks separately rather than discard a
// release/forget notification when ordinary admission is closed.
func (g *viewAdmission) enter() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused != nil {
		return nil, errViewAdmissionPaused
	}
	return g.trackLocked(), nil
}

// enterCleanup admits mandatory release/forget notifications even while ordinary
// I/O is paused, but counts their entire side effects before a binding decision.
func (g *viewAdmission) enterCleanup() func() {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.trackLocked()
}

func (g *viewAdmission) trackLocked() func() {
	if g.active == 0 && g.paused != nil {
		// Cleanup may arrive after a previous idle observation. Give subsequent
		// waiters a fresh notification; idle is a condition, not a permanent latch.
		g.paused.idle = make(chan struct{})
	}
	g.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.active--
			if g.active == 0 && g.paused != nil {
				close(g.paused.idle)
			}
		})
	}
}

// pause closes admission before observing outstanding operations. The returned
// ownership token is required to reopen it. A timeout does not reopen it: the
// switch owner must choose between pre-decision abort and post-decision recovery.
func (g *viewAdmission) pause() (*viewPause, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused != nil {
		return nil, errViewAdmissionPaused
	}
	p := &viewPause{gate: g, idle: make(chan struct{}), released: make(chan struct{})}
	g.paused = p
	if g.active == 0 {
		close(p.idle)
	}
	return p, nil
}

type viewPause struct {
	gate     *viewAdmission
	idle     chan struct{}
	released chan struct{}
}

func (p *viewPause) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.gate.mu.Lock()
		if p.gate.paused != p {
			p.gate.mu.Unlock()
			return errViewPauseLost
		}
		if p.gate.active == 0 {
			p.gate.mu.Unlock()
			return nil
		}
		idle := p.idle
		p.gate.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.released:
			return errViewPauseLost
		case <-idle:
		}
	}
}

// atIdle rechecks quiescence and prevents cleanup admission throughout a short
// binding-decision section. Waiting for idle alone is insufficient: a Release
// may start immediately afterwards and publish a final write. fn must not wait
// for callbacks, issue kernel notifications, or reenter the admission gate.
// Cache invalidation and other work which needs cleanup progress happen outside
// this section, with ordinary I/O still paused.
func (p *viewPause) atIdle(ctx context.Context, fn func() error) error {
	p.gate.mu.Lock()
	defer p.gate.mu.Unlock()
	if p.gate.paused != p {
		return errViewPauseLost
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.gate.active != 0 {
		return errViewCallbacksActive
	}
	return fn()
}

// resume cannot reopen a successor's pause. The caller may use it only before
// a binding decision or after the decided target is fully installed.
func (p *viewPause) resume() error {
	p.gate.mu.Lock()
	defer p.gate.mu.Unlock()
	if p.gate.paused != p {
		return errViewPauseLost
	}
	p.gate.paused = nil
	close(p.released)
	return nil
}

// resumeAtIdle completes a local durable decision and reopens admission in one
// idle section. complete may sync the local journal, but must not notify the
// kernel, wait for callbacks or reenter the gate. A failure keeps admission closed.
// The context check in atIdle is the cancellation cutoff: after complete starts,
// its durable success wins over cancellation and admission is reopened.
func (p *viewPause) resumeAtIdle(ctx context.Context, complete func() error) error {
	return p.atIdle(ctx, func() error {
		if err := complete(); err != nil {
			return err
		}
		p.gate.paused = nil
		close(p.released)
		return nil
	})
}
