package migration

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const eventRequestTimeout = 500 * time.Millisecond

type ReporterSnapshot struct {
	QueueDepth int   `json:"queue_depth"`
	Sent       int64 `json:"sent"`
	Failed     int64 `json:"failed"`
	Dropped    int64 `json:"dropped"`
}

type eventReporter struct {
	api     atomic.Pointer[client.Client]
	queue   chan client.MigrationEvent
	once    sync.Once
	waiters sync.WaitGroup
	sent    atomic.Int64
	failed  atomic.Int64
	dropped atomic.Int64
}

func newEventReporter(api *client.Client, capacity int) *eventReporter {
	r := &eventReporter{queue: make(chan client.MigrationEvent, capacity)}
	r.api.Store(api)
	return r
}

func (r *eventReporter) start(ctx context.Context) {
	r.once.Do(func() {
		r.waiters.Add(1)
		go r.run(ctx)
	})
}

func (r *eventReporter) run(ctx context.Context) {
	defer r.waiters.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-r.queue:
			sent := false
			for attempt := 0; attempt < 2; attempt++ {
				requestCtx, cancel := context.WithTimeout(ctx, eventRequestTimeout)
				err := r.api.Load().PostMigrationEvent(requestCtx, event)
				cancel()
				if err == nil {
					r.sent.Add(1)
					sent = true
					break
				}
				if attempt == 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(50 * time.Millisecond):
					}
				}
			}
			if !sent {
				r.failed.Add(1)
			}
		}
	}
}

func (r *eventReporter) enqueue(event client.MigrationEvent) bool {
	select {
	case r.queue <- event:
		return true
	default:
		r.dropped.Add(1)
		return false
	}
}

func (r *eventReporter) snapshot() ReporterSnapshot {
	return ReporterSnapshot{QueueDepth: len(r.queue), Sent: r.sent.Load(), Failed: r.failed.Load(), Dropped: r.dropped.Load()}
}

func (r *eventReporter) wait() { r.waiters.Wait() }
