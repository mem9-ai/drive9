// Package sse is the fusev2 remote-coherence extension, ported from
// pkg/fuse/sse.go: it subscribes to the server's /v1/events stream,
// filters out this mount's own actor, and turns remote changes into chain
// events (EventInvalidated / EventDelete) plus kernel cache invalidations.
// Resets sweep everything (EventCheckout + InvalidateAll).
package sse

import (
	"context"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// Options configures the watcher.
type Options struct {
	Client     *client.Client
	Actor      string // this mount's actor ID for self-filtering
	RemoteRoot string
	Events     *fs.EventBus
	Inodes     fs.InodeTable // optional: resolves paths to inodes for kernel notify
	Notify     fs.Notifier   // optional: kernel cache invalidation
}

// SSE is the watcher service. It implements fs.Lifecycle: the chain starts
// it at mount and stops it at unmount.
type SSE struct {
	opts    Options
	root    string
	cancel  context.CancelFunc
	doneCh  chan struct{}
	current bool
}

func New(opts Options) (*SSE, error) {
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	return &SSE{opts: opts, root: root, doneCh: make(chan struct{})}, nil
}

// Attach wires the facade-owned identity and notifier services once the
// facade exists (plugins are constructed before it).
func (w *SSE) Attach(inodes fs.InodeTable, notify fs.Notifier) {
	w.opts.Inodes = inodes
	w.opts.Notify = notify
}

// Start connects the event stream in the background.
func (w *SSE) Start(ctx context.Context) error {
	if w.opts.Client == nil {
		return nil
	}
	ctx, w.cancel = context.WithCancel(context.Background())
	go func() {
		defer close(w.doneCh)
		w.opts.Client.WatchEventsWithLifecycle(ctx, w.opts.Actor, w.handleEvent, client.EventLifecycle{
			OnCurrent: func(uint64) { w.current = true },
		})
	}()
	return nil
}

// Drain is a no-op: the stream carries no durable work.
func (w *SSE) Drain(ctx context.Context) error { return nil }

// Stop disconnects and waits for the stream goroutine.
func (w *SSE) Stop(ctx context.Context) error {
	if w.cancel != nil {
		w.cancel()
		select {
		case <-w.doneCh:
		case <-time.After(5 * time.Second):
		}
	}
	return nil
}

// Do is a pass-through: this extension only observes.
func (w *SSE) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return next(ctx, op)
}

func (w *SSE) emit(e fs.Event) {
	if w.opts.Events != nil {
		w.opts.Events.Emit(e)
	}
}

// localPath maps a tenant-absolute remote path into this mount's view;
// false means the change is outside our root.
func (w *SSE) localPath(remote string) (string, bool) {
	if w.root == "/" {
		return remote, true
	}
	if remote == w.root {
		return "/", true
	}
	if strings.HasPrefix(remote, w.root+"/") {
		return remote[len(w.root):], true
	}
	return "", false
}

func (w *SSE) handleEvent(change *client.ChangeEvent, reset *client.ResetEvent) {
	if change != nil {
		if w.opts.Actor != "" && change.Actor == w.opts.Actor {
			return // our own mutation echoing back
		}
		w.handleChange(change)
		return
	}
	if reset != nil && reset.Reason != "" {
		if w.opts.Actor != "" && reset.Reason == "structural_change" && reset.Actor == w.opts.Actor {
			return
		}
		w.handleReset(reset)
	}
}

func (w *SSE) handleChange(ce *client.ChangeEvent) {
	if ce.Path == "" {
		return
	}
	p, ok := w.localPath(ce.Path)
	if !ok {
		return
	}
	kind := fs.EventInvalidated
	if strings.Contains(strings.ToLower(ce.Op), "delete") || strings.Contains(strings.ToLower(ce.Op), "remove") {
		kind = fs.EventDelete
	}
	w.emit(fs.Event{Kind: kind, Path: p})

	if w.opts.Notify != nil && w.opts.Inodes != nil {
		if ino, ok := w.opts.Inodes.Ino(p); ok {
			w.opts.Notify.InvalidateContent(ino, 0)
		}
		if parentIno, ok := w.opts.Inodes.Ino(pathutil.DirOf(p)); ok {
			w.opts.Notify.InvalidateEntry(parentIno, pathutil.BaseName(p))
		}
	}
}

func (w *SSE) handleReset(reset *client.ResetEvent) {
	w.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if w.opts.Notify != nil {
		w.opts.Notify.InvalidateAll()
	}
	w.current = true
}
