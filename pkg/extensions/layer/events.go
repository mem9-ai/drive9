// Lifecycle and the layer-event watcher: live layer mounts poll
// fs_layer_events so other clients' writes — and rollbacks — propagate.
package layer

import (
	"context"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// EmitCheckoutForTest emits a coarse checkout event (test hook for the
// view-fence row).
func (l *Layer) EmitCheckoutForTest() {
	l.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
}

// Attach wires the facade-owned kernel notifier (mount calls it after the
// facade exists).
func (l *Layer) Attach(notify fs.Notifier) { l.notify = notify }

func (l *Layer) Start(ctx context.Context) error {
	if err := l.Restore(ctx); err != nil {
		return err
	}
	// Checkpoint mounts are static: no watcher.
	if l.checkpoint == "" {
		l.startEventWatcher()
	}
	return nil
}

func (l *Layer) Drain(ctx context.Context) error { return nil }

func (l *Layer) Stop(ctx context.Context) error {
	if l.watcherCancel != nil {
		l.watcherCancel()
		<-l.watcherDone
	}
	return nil
}

// startEventWatcher polls fs_layer_events at 1s.
func (l *Layer) startEventWatcher() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l.watcherCancel = cancel
	l.watcherDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = l.refreshEvents(ctx)
			}
		}
	}()
}

// refreshEvents consumes new layer events: a rollback clears the overlay
// and abandons the write path (never re-replaying the abandoned layer);
// anything else replays the log and resets the mount view so existing
// handles and kernel pages stop serving the old payload.
func (l *Layer) refreshEvents(ctx context.Context) error {
	l.mu.RLock()
	since := l.lastSeq
	l.mu.RUnlock()
	events, err := l.cl.ListFSLayerEvents(ctx, l.layerRef, since)
	if err != nil || len(events) == 0 {
		return err
	}
	max := since
	rolledBack := false
	for i := range events {
		if events[i].Seq > max {
			max = events[i].Seq
		}
		if events[i].Op == client.FSLayerEventOpRollback {
			rolledBack = true
		}
	}
	if rolledBack {
		l.Rollback()
		l.mu.Lock()
		l.lastSeq = max
		l.mu.Unlock()
		return nil
	}
	if err := l.Restore(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	l.lastSeq = max
	l.mu.Unlock()
	l.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if l.notify != nil {
		l.notify.InvalidateAll()
	}
	return nil
}
