// Package layer is the fs-layer extension: a shadow view over the classic
// filesystem. Mounting a layer overlays /.drive9/layers/<id>/tree (real
// files, written through the ordinary commit pipeline) on the classic view,
// hides paths carrying blackout markers under blackouts/, and patches
// metadata-only changes from attrs/ markers. All mutations are copy-on-write:
// they land under the layer's namespace and never touch the classic view.
// Layer metadata (registry, states, checkpoints, events) stays on the server
// through the client's FSLayer APIs; commit remains server-side.
package layer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// errLayerRolledBack fails every overlay mutation after a rollback event.
var errLayerRolledBack = errors.New("fs layer has been rolled back; remount or checkout required")

// The /.drive9/layers namespace layout (all ordinary files):
//
//	/.drive9/layers/<layer-id>/
//	  tree/<path...>       overlay content (dirs, files, symlinks; mode is chmod)
//	  blackouts/<path...>  whiteout marker files, content "<kind>:<baseRev>"
//	  cas/<path...>        commit-CAS markers, content "<baseRev>" (0 = base must not exist)
//	  attrs/<path...>      metadata-only markers, content JSON {"mode":...,"mtime":...}
//	  checkpoints/<cp-id>/{tree,blackouts,cas,attrs}/...   checkpoint snapshots
const (
	layersNamespace   = "/.drive9/layers"
	treeSubdir        = "tree"
	blackoutSubdir    = "blackouts"
	blackoutDirSubdir = "blackout-dirs"
	casSubdir         = "cas"
	attrsSubdir       = "attrs"
)

// Options configures the layer view. LayerRef may be empty: the extension
// then starts in live (pass-through) mode and waits for a hot checkout.
// IO and Registry are the dependency seams (see io.go); production wires
// NewClientDeps(cl).
type Options struct {
	LayerRef      string
	CheckpointRef string // checkpoint views are read-only
	RemoteRoot    string
	IO            layerIO
	Registry      layerRegistry
	Events        *fs.EventBus
}

// Layer is the fs.Handler shadowing a server-side fs layer over the base
// view through filesystem-path rewrites.
type Layer struct {
	io     layerIO
	reg    layerRegistry
	events *fs.EventBus
	// root is the server-absolute prefix the mount presents as "/".
	root string
	// initial refs ("" = start live).
	layerRef    string
	checkpoint  string
	startedOnce sync.Once

	// mu guards cur and the rollback hooks; cur swaps atomically on
	// checkout.
	mu  sync.RWMutex
	cur *view

	// gen fences lookups issued against a view being switched away from.
	gen atomic.Uint64

	// notify invalidates kernel caches and inodes maps kernel paths; the
	// facade attaches both after assembly.
	notify fs.Notifier
	inodes fs.InodeTable
	// onRollback hooks run when a rollback event abandons the current
	// layer (the composition root wires the write plane's fence here).
	onRollback []func()
	// watcherCancel stops the poll loop; nil until Start.
	watcherCancel func()
	watcherDone   chan struct{}

	// mutLocks serialize the marker+content sequence of mutations per
	// path: a CoW materialization and a same-path rename never interleave
	// their ordering.
	mutLocks mutLockTable
}

// mutLockTable is a striped per-path mutex table.
type mutLockTable struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func (t *mutLockTable) lock(path string) func() {
	t.mu.Lock()
	if t.m == nil {
		t.m = map[string]*sync.Mutex{}
	}
	lk := t.m[path]
	if lk == nil {
		lk = &sync.Mutex{}
		t.m[path] = lk
	}
	t.mu.Unlock()
	lk.Lock()
	return lk.Unlock
}

// OnRollback registers a hook that runs exactly once, when a rollback event
// abandons the current layer.
func (l *Layer) OnRollback(f func()) {
	l.mu.Lock()
	l.onRollback = append(l.onRollback, f)
	l.mu.Unlock()
}

func New(opts Options) (*Layer, error) {
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	return &Layer{
		io:          opts.IO,
		reg:         opts.Registry,
		events:      opts.Events,
		root:        root,
		layerRef:    strings.TrimSpace(opts.LayerRef),
		checkpoint:  strings.TrimSpace(opts.CheckpointRef),
		cur:         newLiveView(),
		watcherDone: make(chan struct{}),
	}, nil
}

// serverAbs maps a view path to the server-absolute path it shadows.
func (l *Layer) serverAbs(p string) string {
	if l.root == "/" {
		return p
	}
	if p == "/" {
		return l.root
	}
	return l.root + p
}

// viewPathOf maps a server-absolute path back into the mount's view; ok is
// false for paths outside the remote root.
func (l *Layer) viewPathOf(serverPath string) (string, bool) {
	if l.root == "/" {
		return serverPath, true
	}
	if serverPath == l.root {
		return "/", true
	}
	if strings.HasPrefix(serverPath, l.root+"/") {
		return serverPath[len(l.root):], true
	}
	return "", false
}

// CurrentView reports the active shadow for control-plane replies.
func (l *Layer) CurrentView() (mode string, layerID, checkpointID string, readOnly bool) {
	l.mu.RLock()
	v := l.cur
	l.mu.RUnlock()
	return v.mode.String(), v.layerID, v.checkpoint, v.readOnly
}

func (l *Layer) emit(e fs.Event) {
	if l.events != nil {
		l.events.Emit(e)
	}
}

var (
	_ fs.Handler   = (*Layer)(nil)
	_ fs.Lifecycle = (*Layer)(nil)
)

// Start activates the initial view (if a layer ref was configured) and the
// event watcher. It blocks until the view index is built.
func (l *Layer) Start(ctx context.Context) error {
	var err error
	l.startedOnce.Do(func() {
		if l.layerRef != "" {
			cerr := l.Checkout(ctx, CheckoutTarget{LayerRef: l.layerRef, CheckpointRef: l.checkpoint})
			if cerr != nil {
				err = fmt.Errorf("layer: restore %s: %w", l.layerRef, cerr)
			}
		}
	})
	if err != nil {
		return err
	}
	l.startWatcher()
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
