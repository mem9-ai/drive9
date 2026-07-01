package server

import (
	"context"
	"testing"
)

func TestSemanticOutboxPollerShardFilter(t *testing.T) {
	// Verify the poller struct can be constructed with a shard function.
	ownsAll := func(string) bool { return true }
	ownsNone := func(string) bool { return false }

	worker := &semanticWorkerManager{
		kickPending: make(map[string]struct{}),
		kicks:       make(chan string, 100),
	}

	p1 := newSemanticOutboxPoller(nil, worker, ownsAll, "pod-1", 0, 0)
	if p1 == nil {
		t.Fatal("expected non-nil poller")
	}
	p2 := newSemanticOutboxPoller(nil, worker, ownsNone, "pod-2", 0, 0)
	if p2 == nil {
		t.Fatal("expected non-nil poller")
	}
}

func TestSemanticOutboxPollerNilSafe(t *testing.T) {
	// A nil poller should be safe to call initCursor and flushCursor on.
	var p *semanticOutboxPoller
	p.initCursor(context.Background()) // should not panic
	p.flushCursor(context.Background()) // should not panic
	p.maybeFlushCursor(context.Background()) // should not panic
}