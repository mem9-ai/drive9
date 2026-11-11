package fuse

import (
	"context"
	"sync"
)

// kernelNotifications tracks async notifications without a goroutine per wait.
// Producers may enqueue while an ordinary drain waits. A view switch must stop
// those producers before using an idle result as a permanent boundary.
type kernelNotifications struct {
	mu     sync.Mutex
	active int
	idle   chan struct{}
}

func (n *kernelNotifications) run(fn func()) {
	n.mu.Lock()
	if n.active == 0 {
		n.idle = make(chan struct{})
	}
	n.active++
	n.mu.Unlock()
	go func() {
		defer func() {
			n.mu.Lock()
			n.active--
			if n.active == 0 {
				close(n.idle)
			}
			n.mu.Unlock()
		}()
		fn()
	}()
}

func (n *kernelNotifications) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n.mu.Lock()
		if n.active == 0 {
			n.mu.Unlock()
			return nil
		}
		idle := n.idle
		n.mu.Unlock()
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
