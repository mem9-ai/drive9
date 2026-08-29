package migration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const adaptiveHealthyWindow = 30 * time.Second

type adaptiveLimit struct {
	mu           sync.Mutex
	current      int
	cap          int
	healthySince time.Time
}

func newAdaptiveLimit(capacity int, now time.Time) (*adaptiveLimit, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("adaptive limit capacity must be positive")
	}
	return &adaptiveLimit{current: min(4, capacity), cap: capacity, healthySince: now}, nil
}

func (l *adaptiveLimit) Current() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

func (l *adaptiveLimit) OnFailure(err error, now time.Time) bool {
	if !isBackpressureError(err) {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.current = max(1, l.current/2)
	l.healthySince = now
	return true
}

func (l *adaptiveLimit) OnSuccess(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.current >= l.cap || now.Sub(l.healthySince) < adaptiveHealthyWindow {
		return
	}
	l.current++
	l.healthySince = now
}

func isBackpressureError(err error) bool {
	if err == nil {
		return false
	}
	var status *client.StatusError
	if errors.As(err, &status) {
		return status.StatusCode == http.StatusTooManyRequests || status.StatusCode >= http.StatusInternalServerError
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) && network.Timeout()
}
