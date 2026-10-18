// Lifecycle and the layer-event watcher: live layer mounts poll
// fs_layer_events so other clients' writes — and rollbacks — propagate.
package layer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// EmitCheckoutForTest emits a coarse checkout event (test hook for the
// view-fence row).
// refreshEventsForTest drives one event refresh synchronously (tests
// need the checkout sweep without waiting for the poller).
func (l *Layer) refreshEventsForTest(ctx context.Context) {
	_ = l.refreshEvents(ctx)
}

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

// startEventWatcher polls fs_layer_events at 1s. Refresh failures surface
// on stderr (v1 layer_events.go): a silently-dead watcher looks exactly
// like a layer that never changes.
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
				if err := l.refreshEvents(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: layer event refresh failed for %s: %v\n", l.layerRef, err)
				}
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
	// Open handles follow external layer changes (v1 open-handle refresh):
	// a path whose overlay entry changed re-resolves from the new overlay
	// (drop the stale buffer when it holds no uncommitted writes); a path
	// now whiteouted serves ENOENT even on an open fd.
	changed := make(map[string]bool)
	for i := range events {
		if events[i].Path != "" {
			// Event paths are remote (root-prefixed); the live handle
			// index and the overlay are keyed by the mount-local path.
			changed[l.localPathOf(events[i].Path)] = true
		}
	}
	// EVERY descriptor of a changed path follows the checkout, not just
	// the per-path newest slot (l.live keeps one handle per path; the
	// handles set has them all — an older sibling fd kept serving its
	// stale buffer through a refresh that updated only the newest).
	l.handles.Range(func(key, _ any) bool {
		h, ok := key.(*layerHandle)
		if !ok {
			return true
		}
		// Snapshot and apply under ONE lock hold: a gap between them let
		// a rename retarget the fd (or a Release retire it) while the
		// sweep still held the old path, writing the old name's
		// whiteout/content into the moved fd (review P1).
		h.mu.Lock()
		path := h.path
		if h.released || !changed[path] {
			h.mu.Unlock()
			return true
		}
		if h.dirty {
			// v1 preserveLayerReplayConflict: an external change under
			// uncommitted writes fences the handle — publishing its
			// buffer onto the new tip would splice stale bytes. The
			// buffer is kept; writes and commits answer EAGAIN.
			h.conflicted = true
		}
		if _, _, _, _, ok := l.lookup(path); ok {
			kind, _, _, content, _ := l.lookup(path)
			if kind == "file" && !h.dirty {
				h.buf = nil
				h.origSize = int64(len(content))
				h.baseRev = 0
			}
		} else if !h.dirty {
			h.buf = nil
			h.origSize = 0
		}
		kind, _, _, _, look := l.lookup(path)
		if look && kind == "whiteout" {
			h.unlinked = true
		}
		h.mu.Unlock()
		return true
	})
	l.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if l.notify != nil {
		l.notify.InvalidateAll()
	}
	return nil
}
