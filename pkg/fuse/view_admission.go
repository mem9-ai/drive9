package fuse

import (
	"context"
	"errors"
	"sync"
)

var (
	errViewAdmissionPaused = errors.New("mount view admission paused")
	errViewPauseLost       = errors.New("mount view pause no longer owned")
)

// viewAdmission counts complete foreground operations, including their local
// cache/handle updates. It does not replace draining background upload queues
// or protecting old handles. Its zero value admits requests.
type viewAdmission struct {
	mu     sync.Mutex
	active int
	paused *viewPause
}

// enter is deliberately nonblocking: a caller cannot occupy a FUSE request
// worker while waiting for a switch which itself needs kernel notifications.
// The adapter must handle cleanup callbacks separately rather than discard a
// release/forget notification when ordinary admission is closed.
func (g *viewAdmission) enter() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused != nil {
		return nil, errViewAdmissionPaused
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
	}, nil
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
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.released:
		return errViewPauseLost
	case <-p.idle:
	}
	p.gate.mu.Lock()
	defer p.gate.mu.Unlock()
	if p.gate.paused != p {
		return errViewPauseLost
	}
	return ctx.Err()
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
