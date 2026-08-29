package migration

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrMemoryBudgetExceeded = errors.New("migration memory budget exceeded")

type memoryBudget struct {
	mu     sync.Mutex
	limit  int64
	used   int64
	peak   int64
	notify chan struct{}
}

func newMemoryBudget(limit int64) (*memoryBudget, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("memory budget must be positive")
	}
	return &memoryBudget{limit: limit, notify: make(chan struct{})}, nil
}

func (b *memoryBudget) Acquire(ctx context.Context, bytes int64) (func(), error) {
	if bytes < 0 || bytes > b.limit {
		return nil, fmt.Errorf("%w: request=%d limit=%d", ErrMemoryBudgetExceeded, bytes, b.limit)
	}
	if bytes == 0 {
		return func() {}, nil
	}
	for {
		b.mu.Lock()
		if b.used+bytes <= b.limit {
			b.used += bytes
			b.peak = max(b.peak, b.used)
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { b.release(bytes) })
			}, nil
		}
		notify := b.notify
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (b *memoryBudget) release(bytes int64) {
	b.mu.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
}

func (b *memoryBudget) Snapshot() (used, peak, limit int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.peak, b.limit
}
