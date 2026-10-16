// Package layer is the fs-layer extension: an in-memory overlay
// (whiteouts / file modes / dirs / symlinks) over the base view, persisted
// through the client's FSLayer entry API (UpsertFSLayerEntry) — never
// through fs-path rewriting. Mount restore replays the layer log
// (ReplayFSLayer) with the supersede rules; a rollback event abandons the
// layer: the overlay clears and later writes fail with the rolled-back
// error until remount.
package layer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// errLayerRolledBack fails every overlay mutation after a rollback event.
var errLayerRolledBack = fmt.Errorf("fs layer has been rolled back; remount required")

type symlinkState struct {
	Target string
	Mode   uint32
}

// Options configures the layer view.
type Options struct {
	LayerRef      string
	CheckpointRef string // checkpoint mounts are read-only
	RemoteRoot    string
	Client        *client.Client
	Events        *fs.EventBus
	// Policy is the mount's write policy ("write-sync" commits per write).
	Policy string
	// InlineMax overrides the inline-entry threshold (tests; 0 = default).
	InlineMax int64
}

// Layer is the fs.Handler overlaying a server-side fs layer on the base.
type Layer struct {
	cl         *client.Client
	layerRef   string
	checkpoint string
	root       string
	events     *fs.EventBus

	mu        sync.RWMutex
	whiteouts map[string]struct{}
	files     map[string]uint32 // overlay file modes
	// remoteSizes are uploaded-but-unmaterialized layer images (large
	// truncates): the authoritative body is the layer's stored object and
	// reads stream it back; contents must not claim a partial or empty
	// image for these paths.
	remoteSizes map[string]int64
	// baseModes are mode-only overrides for files whose content still
	// comes from the base object (a chmod of a base file).
	baseModes map[string]uint32
	dirs      map[string]uint32 // overlay dir modes
	symlinks  map[string]symlinkState
	// contents holds overlay file bodies inline, seeded from replay and
	// refreshed on every upsert.
	contents  map[string][]byte
	abandoned bool
	readOnly  bool
	// lastSeq is the replay cursor for the event refresh loop.
	lastSeq int64

	// notify invalidates kernel caches after a remote layer refresh (the
	// facade attaches it after assembly).
	notify fs.Notifier
	// watcherCancel stops the event poll loop; nil until Start.
	watcherCancel func()
	watcherDone   chan struct{}

	// mutLocks serialize overlay mutations per path: an upsert/whiteout
	// flush and a same-path rename never interleave their HTTP ordering
	// and map updates.
	mutLocks mutLockTable

	// onRollback hooks run when a rollback event abandons the layer (the
	// composition root wires the write plane's fence here).
	onRollback []func()

	// live indexes claimed layer handles by path (SetAttr interception,
	// unlink/rename retargeting).
	live    sync.Map // path -> newest live handle
	handles sync.Map // every live handle (survivor re-selection)
	// policy is the mount's write policy: write-sync commits per write.
	policy string
	// inlineMaxOverride bounds inline entry bodies in tests (0 = client
	// default).
	inlineMaxOverride int64
}

// track/untrack maintain the claimed-handle index.
func (l *Layer) liveStore(path string, h *layerHandle) {
	l.handles.Store(h, struct{}{})
	l.live.Store(path, h)
}

// untrackHandle removes exactly one handle's registration: the path slot
// may hold a NEWER claim (a second open overwrote it), and deleting the
// slot outright hid the surviving writer from rename retargeting — its
// pending edit then landed under the old name instead of following the
// rename. The path slot is cleared only while it still maps to this
// handle; a surviving older claim is re-selected.
func (l *Layer) untrackHandle(h *layerHandle) {
	h.mu.Lock()
	path := h.path
	h.released = true
	h.mu.Unlock()
	// The handle leaves the index entirely: a retired descriptor must not
	// be re-selected as the path's surviving claim (its Release already
	// published; dirtying it again stranded the change with no committer).
	l.handles.Delete(h)
	if cur, ok := l.live.Load(path); ok && cur == h {
		l.live.Delete(path)
		l.handles.Range(func(key, _ any) bool {
			if other, ok := key.(*layerHandle); ok && other != h && !other.unlinked && !other.released {
				other.mu.Lock()
				same := other.path == path
				other.mu.Unlock()
				if same {
					l.live.Store(path, other)
					return false
				}
			}
			return true
		})
	}
}

// ReexecRefusal ports the v1 hasLayerOverlayState gate: a layer mount with
// any overlay state (whiteouts, entries, symlinks, or inline contents)
// refuses re-exec — the in-memory overlay does not survive the handoff.
func (l *Layer) ReexecRefusal() (string, int) {
	if l == nil {
		return "", 0
	}
	l.mu.RLock()
	n := len(l.whiteouts) + len(l.files) + len(l.dirs) + len(l.symlinks) + len(l.contents)
	l.mu.RUnlock()
	if n > 0 {
		return "layer_overlay", n
	}
	return "", 0
}

func (l *Layer) handleFor(path string) *layerHandle {
	if v, ok := l.live.Load(path); ok {
		return v.(*layerHandle)
	}
	return nil
}

// serverLayerEntryMaxBytes is the server's per-entry transaction cap: an
// inline upsert larger than this fails the store transaction outright
// ("entry too large"). Larger images must take the object-upload path.
const serverLayerEntryMaxBytes int64 = 6 << 20

// inlineMax is the inline-entry threshold: larger images stream as object
// uploads (v1 routed its shadow-spilled layer commits the same way — the
// 95MiB client envelope limit never applied to the store's entry cap).
func (l *Layer) inlineMax() int64 {
	if l.inlineMaxOverride > 0 {
		return l.inlineMaxOverride
	}
	return serverLayerEntryMaxBytes
}

// OnRollback registers a hook that runs exactly once, when a rollback
// event abandons the layer.
func (l *Layer) OnRollback(f func()) {
	l.mu.Lock()
	l.onRollback = append(l.onRollback, f)
	l.mu.Unlock()
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

func New(opts Options) (*Layer, error) {
	if strings.TrimSpace(opts.LayerRef) == "" {
		return nil, fmt.Errorf("layer: LayerRef is required")
	}
	if strings.TrimSpace(opts.CheckpointRef) != "" {
		opts.CheckpointRef = strings.TrimSpace(opts.CheckpointRef)
	}
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	// v1 mount state gate: non-checkpoint mounts only accept active layers;
	// checkpoint mounts also accept sealed/committed ones. Mounting a
	// rolled-back or otherwise unusable layer fails fast instead of serving
	// a stale view.
	if opts.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		lr, lerr := opts.Client.GetFSLayer(ctx, strings.TrimSpace(opts.LayerRef))
		cancel()
		if lerr != nil {
			return nil, fmt.Errorf("layer: resolve %s: %w", opts.LayerRef, lerr)
		}
		state := ""
		if lr != nil {
			state = lr.State
		}
		allowed := state == "active"
		if opts.CheckpointRef != "" {
			allowed = allowed || state == "sealed" || state == "committed"
		}
		if !allowed {
			return nil, fmt.Errorf("layer: %s is in state %q (want active%s); remount requires a usable layer",
				opts.LayerRef, state, func() string {
					if opts.CheckpointRef != "" {
						return "/sealed/committed"
					}
					return ""
				}())
		}
	}
	return &Layer{
		cl:                opts.Client,
		layerRef:          strings.TrimSpace(opts.LayerRef),
		checkpoint:        opts.CheckpointRef,
		root:              root,
		events:            opts.Events,
		policy:            opts.Policy,
		inlineMaxOverride: opts.InlineMax,
		whiteouts:         map[string]struct{}{},
		files:             map[string]uint32{},
		baseModes:         map[string]uint32{},
		remoteSizes:       map[string]int64{},
		dirs:              map[string]uint32{},
		symlinks:          map[string]symlinkState{},
		contents:          map[string][]byte{},
		readOnly:          strings.TrimSpace(opts.CheckpointRef) != "",
	}, nil
}

func (l *Layer) remotePath(p string) string {
	if l.root == "/" {
		return p
	}
	if p == "/" {
		return l.root
	}
	return l.root + p
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

// localPathOf maps a remote (root-prefixed) event path back onto the
// mount-local vocabulary the overlay and live handle index use.
func (l *Layer) localPathOf(rp string) string {
	if l.root == "/" || rp == "/" {
		return rp
	}
	if rp == l.root {
		return "/"
	}
	return strings.TrimPrefix(rp, l.root)
}
