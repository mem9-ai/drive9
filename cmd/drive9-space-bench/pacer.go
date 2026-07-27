package main

import (
	"context"
	"sync"
	"time"
)

type pacedLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newPacedLimiter(rps float64) *pacedLimiter {
	if rps == 0 {
		return &pacedLimiter{}
	}
	return &pacedLimiter{interval: time.Duration(float64(time.Second) / rps)}
}

func (l *pacedLimiter) Wait(ctx context.Context) error {
	if l.interval == 0 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.IsZero() || l.next.Before(now) {
		l.next = now
	}
	slot := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()

	delay := time.Until(slot)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
