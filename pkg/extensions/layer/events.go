// Event plumbing: the rollback poll loop (metadata API) and the bus
// subscription translating remote marker-tree changes into user-view
// invalidations.
package layer

import (
	"context"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// rollbackEventOp mirrors the registry's rollback event marker (the
// client's FSLayerEventOpRollback value).
const rollbackEventOp = "rollback"

// pollInterval mirrors v1's layer-event poll cadence.
const pollInterval = time.Second

// Attach wires the facade-owned kernel notifier and inode table (mount
// calls it after assembly); remote marker-tree changes need both to
// invalidate the user-visible paths the kernel actually holds.
func (l *Layer) Attach(notify fs.Notifier, inodes fs.InodeTable) {
	l.notify = notify
	l.inodes = inodes
}

// startWatcher runs the rollback poll loop and the bus subscription until
// Stop. Safe to call once per mount.
func (l *Layer) startWatcher() {
	if l.events != nil {
		l.events.On(fs.EventInvalidated, l.onRemoteChange)
		l.events.On(fs.EventDelete, l.onRemoteChange)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.watcherCancel = cancel
	go func() {
		defer close(l.watcherDone)
		l.subscribeInitialCursor(ctx)
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l.pollRollback(ctx)
			}
		}
	}()
}

// subscribeInitialCursor seeds the event-log cursor so the first poll does
// not replay history as a fresh rollback.
func (l *Layer) subscribeInitialCursor(ctx context.Context) {
	l.mu.RLock()
	v := l.cur
	l.mu.RUnlock()
	if v.mode == viewLive || v.layerID == "" {
		return
	}
	events, err := l.reg.ListEvents(ctx, v.layerID, 0)
	if err != nil {
		return
	}
	for _, e := range events {
		if e.Seq > v.lastSeq {
			v.lastSeq = e.Seq
		}
	}
}

// pollRollback watches the layer event log for a rollback of the current
// layer: the overlay clears, writes fail ESTALE, and the classic view
// reappears without a remount.
func (l *Layer) pollRollback(ctx context.Context) {
	l.mu.RLock()
	v := l.cur
	l.mu.RUnlock()
	if v.mode == viewLive || v.layerID == "" {
		return
	}
	events, err := l.reg.ListEvents(ctx, v.layerID, v.lastSeq)
	if err != nil || len(events) == 0 {
		return
	}
	rolled := false
	for _, e := range events {
		if e.Seq > v.lastSeq {
			v.lastSeq = e.Seq
		}
		if e.Op == rollbackEventOp {
			rolled = true
		}
	}
	if !rolled {
		return
	}
	l.mu.Lock()
	if l.cur == v && !v.abandoned {
		v.abandoned = true
		hooks := append([]func(){}, l.onRollback...)
		// The overlay clears (v1): reads fall through to the classic view
		// — blackouts stop hiding — while the abandoned latch fails every
		// later mutation with ESTALE until remount or checkout.
		cleared := &view{
			mode:      viewLayer,
			layerID:   v.layerID,
			abandoned: true,
			lastSeq:   v.lastSeq,
			idx:       newViewIndex(),
		}
		l.cur = cleared
		l.mu.Unlock()
		for _, hook := range hooks {
			hook()
		}
		if l.notify != nil {
			l.notify.InvalidateAll()
		}
		l.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
		return
	}
	l.mu.Unlock()
}

// onRemoteChange translates a remote change under the active marker roots
// into the user view: the index entry refreshes and the kernel entry and
// cached listings for the user-visible path drop. Events for everything
// else (including our own mount's echoes below the roots) pass through.
func (l *Layer) onRemoteChange(e fs.Event) {
	if e.Path == "" {
		return
	}
	l.mu.RLock()
	v := l.cur
	l.mu.RUnlock()
	if v.mode == viewLive {
		return
	}
	var rel string
	switch {
	case v.treeRoot != "" && strings.HasPrefix(e.Path, v.treeRoot+"/"):
		rel = e.Path[len(v.treeRoot):]
	case v.blackRoot != "" && strings.HasPrefix(e.Path, v.blackRoot+"/"):
		rel = e.Path[len(v.blackRoot):]
	case v.blackDirRoot != "" && strings.HasPrefix(e.Path, v.blackDirRoot+"/"):
		rel = e.Path[len(v.blackDirRoot):]
	case v.casRoot != "" && strings.HasPrefix(e.Path, v.casRoot+"/"):
		rel = e.Path[len(v.casRoot):]
	case v.attrsRoot != "" && strings.HasPrefix(e.Path, v.attrsRoot+"/"):
		rel = e.Path[len(v.attrsRoot):]
	default:
		return
	}
	vp, ok := l.viewPathOf(rel)
	if !ok {
		return
	}
	l.refreshIndexEntry(context.Background(), v, vp)
	l.invalidateUserPath(vp)
}

// refreshIndexEntry re-reads the marker state of one view path from the
// server (targeted; no full re-walk).
func (l *Layer) refreshIndexEntry(ctx context.Context, v *view, vp string) {
	serverP := l.serverAbs(vp)
	if st, err := l.io.Stat(ctx, v.treeRoot+serverP); err == nil && st != nil {
		v.idx.put(vp, &indexEntry{kind: ekTree, isDir: st.IsDir, isSymlink: st.IsSymlink, mode: st.Mode & 0o777})
	} else {
		v.idx.remove(vp)
	}
	if content, err := l.io.Read(ctx, v.blackRoot+serverP); err == nil {
		v.idx.put(vp, &indexEntry{kind: ekBlackout, blackout: decodeBlackout(content)})
	}
	if content, err := l.io.Read(ctx, v.blackDirRoot+serverP); err == nil {
		bl := decodeBlackout(content)
		bl.isDir = true
		v.idx.put(vp, &indexEntry{kind: ekBlackout, blackout: bl})
	}
	if content, err := l.io.Read(ctx, v.attrsRoot+serverP); err == nil {
		var patch attrsPatch
		if err := decodeAttrs(content, &patch); err == nil {
			v.idx.put(vp, &indexEntry{kind: ekAttrs, patch: patch})
		}
	}
}

// invalidateUserPath drops the kernel entry and the cached listings of the
// user-visible path the marker tree shadows.
func (l *Layer) invalidateUserPath(vp string) {
	l.emit(fs.Event{Kind: fs.EventInvalidated, Path: vp})
	if l.notify == nil || l.inodes == nil {
		return
	}
	if ino, ok := l.inodes.Ino(vp); ok {
		l.notify.InvalidateContent(ino, 0)
	}
	if parentIno, ok := l.inodes.Ino(pathutil.ParentPath(vp)); ok {
		l.notify.InvalidateEntry(parentIno, pathutil.BaseName(vp))
	}
}
