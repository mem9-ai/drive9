// The local test harness: a plain local-filesystem fs.Handler core, the
// layerIO/layerRegistry seams implemented over the same directory and an
// in-memory registry. Tests compose [layer → local core], wrap the chain
// with fs.HandlerFilesystem, and drive the shadow view through the
// path-addressed fs.Filesystem API — no HTTP, no server.
package layer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// localCore is an fs.Handler terminal serving one directory tree: the
// classic view of a layer stack under test.
type localCore struct{ root string }

func newLocalCore(t *testing.T) *localCore {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return &localCore{root: dir}
}

func (c *localCore) sysPath(p string) string {
	return filepath.Join(c.root, filepath.FromSlash(strings.TrimPrefix(p, "/")))
}

func (c *localCore) attrOf(info os.FileInfo) *fs.Attr {
	mode := info.Mode()
	kind := fs.KindFile
	switch {
	case mode.IsDir():
		kind = fs.KindDir
	case mode&os.ModeSymlink != 0:
		kind = fs.KindSymlink
	}
	rev := info.ModTime().UnixNano()
	if rev <= 0 {
		rev = 1
	}
	return &fs.Attr{
		Kind:  kind,
		Mode:  uint32(mode.Perm()),
		Size:  info.Size(),
		Rev:   rev,
		Mtime: info.ModTime(),
	}
}

// localHandle adapts an *os.File to fs.Handle.
type localHandle struct{ f *os.File }

func (h localHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	return h.f.ReadAt(dest, off)
}

func (h localHandle) Write(ctx context.Context, off int64, data []byte) error {
	_, err := h.f.WriteAt(data, off)
	return err
}

func (h localHandle) Sync(ctx context.Context) error { return h.f.Sync() }

func (h localHandle) Flush(ctx context.Context) error { return nil }

func (h localHandle) Release(ctx context.Context) error { return h.f.Close() }

// notFound maps os misses onto the chain sentinel.
func notFound(err error) error {
	if os.IsNotExist(err) {
		return fs.ErrNotFound
	}
	return err
}

func (c *localCore) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	// Data-plane ops carrying a claimed handle dispatch onto the handle.
	if op.Handle != nil {
		if h, ok := op.Handle.(fs.Handle); ok {
			switch op.Kind {
			case fs.OpRead:
				buf := make([]byte, limitOf(op))
				n, err := h.Read(ctx, op.Offset, buf)
				if err == io.EOF {
					err = nil
				}
				if err != nil {
					return fs.Result{}, err
				}
				return fs.Result{Data: buf[:n]}, nil
			case fs.OpWrite:
				if err := h.Write(ctx, op.Offset, op.Data); err != nil {
					return fs.Result{}, err
				}
				return fs.Result{Written: uint32(len(op.Data))}, nil
			case fs.OpFlush:
				return fs.Result{}, h.Flush(ctx)
			case fs.OpFsync:
				return fs.Result{}, h.Sync(ctx)
			case fs.OpRelease:
				return fs.Result{}, h.Release(ctx)
			}
		}
	}
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		info, err := os.Lstat(c.sysPath(op.Path))
		if err != nil {
			return fs.Result{}, notFound(err)
		}
		return fs.Result{Attr: c.attrOf(info)}, nil

	case fs.OpList:
		entries, err := os.ReadDir(c.sysPath(op.Path))
		if err != nil {
			return fs.Result{}, notFound(err)
		}
		out := make([]fs.Entry, 0, len(entries))
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				continue
			}
			name := e.Name()
			out = append(out, fs.Entry{Name: name, Attr: *c.attrOf(info)})
		}
		return fs.Result{Entries: out}, nil

	case fs.OpRead:
		data, err := os.ReadFile(c.sysPath(op.Path))
		if err != nil {
			return fs.Result{}, notFound(err)
		}
		if op.Offset > 0 {
			if op.Offset >= int64(len(data)) {
				return fs.Result{}, nil
			}
			data = data[op.Offset:]
		}
		if op.Limit > 0 && op.Limit < int64(len(data)) {
			data = data[:op.Limit]
		}
		return fs.Result{Data: data}, nil

	case fs.OpWrite:
		p := c.sysPath(op.Path)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return fs.Result{}, err
		}
		if op.Offset == 0 {
			if err := os.WriteFile(p, op.Data, 0o644); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Written: uint32(len(op.Data))}, nil
		}
		current, rerr := os.ReadFile(p)
		if rerr != nil && !os.IsNotExist(rerr) {
			return fs.Result{}, rerr
		}
		end := op.Offset + int64(len(op.Data))
		if end > int64(len(current)) {
			grown := make([]byte, end)
			copy(grown, current)
			current = grown
		}
		copy(current[op.Offset:], op.Data)
		if err := os.WriteFile(p, current, 0o644); err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Written: uint32(len(op.Data))}, nil

	case fs.OpCreate:
		p := c.sysPath(op.Path)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return fs.Result{}, err
		}
		flags := os.O_RDWR
		if op.Flags&uint32(syscall.O_WRONLY) != 0 {
			flags = os.O_WRONLY
		}
		if op.Flags&uint32(syscall.O_APPEND) != 0 {
			flags |= os.O_APPEND
		}
		if op.Flags&uint32(syscall.O_CREAT) != 0 {
			flags |= os.O_CREATE
		}
		if op.Flags&uint32(syscall.O_EXCL) != 0 {
			flags |= os.O_EXCL
		}
		if op.Flags&uint32(syscall.O_TRUNC) != 0 {
			flags |= os.O_TRUNC
		}
		mode := os.FileMode(0o644)
		if op.Mode&0o777 != 0 {
			mode = os.FileMode(op.Mode & 0o777)
		}
		f, err := os.OpenFile(p, flags, mode)
		if err != nil {
			if os.IsExist(err) {
				return fs.Result{}, syscall.EEXIST
			}
			return fs.Result{}, err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return fs.Result{}, err
		}
		return fs.Result{Attr: c.attrOf(info), Handle: localHandle{f: f}}, nil

	case fs.OpMkdir:
		mode := os.FileMode(0o755)
		if op.Mode&0o777 != 0 {
			mode = os.FileMode(op.Mode & 0o777)
		}
		if err := os.Mkdir(c.sysPath(op.Path), mode); err != nil {
			if os.IsExist(err) {
				return fs.Result{}, syscall.EEXIST
			}
			return fs.Result{}, notFound(err)
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: uint32(mode)}}, nil

	case fs.OpRmdir:
		if err := os.Remove(c.sysPath(op.Path)); err != nil {
			if os.IsNotExist(err) {
				return fs.Result{}, fs.ErrNotFound
			}
			return fs.Result{}, err
		}
		return fs.Result{}, nil

	case fs.OpUnlink:
		if err := os.Remove(c.sysPath(op.Path)); err != nil {
			if os.IsNotExist(err) {
				return fs.Result{}, fs.ErrNotFound
			}
			return fs.Result{}, err
		}
		return fs.Result{}, nil

	case fs.OpRename:
		if err := os.Rename(c.sysPath(op.Path), c.sysPath(op.Path2)); err != nil {
			return fs.Result{}, notFound(err)
		}
		return fs.Result{}, nil

	case fs.OpSymlink:
		if err := os.MkdirAll(filepath.Dir(c.sysPath(op.Path)), 0o755); err != nil {
			return fs.Result{}, err
		}
		if err := os.Symlink(op.Target, c.sysPath(op.Path)); err != nil {
			if os.IsExist(err) {
				return fs.Result{}, syscall.EEXIST
			}
			return fs.Result{}, err
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindSymlink, Mode: 0o777, Size: int64(len(op.Target))}}, nil

	case fs.OpReadlink:
		target, err := os.Readlink(c.sysPath(op.Path))
		if err != nil {
			return fs.Result{}, notFound(err)
		}
		return fs.Result{Data: []byte(target)}, nil

	case fs.OpSetAttr:
		p := c.sysPath(op.Path)
		if op.Mode != 0 {
			if err := os.Chmod(p, os.FileMode(op.Mode&0o7777)); err != nil {
				return fs.Result{}, notFound(err)
			}
		}
		if op.Size >= 0 {
			if err := os.Truncate(p, op.Size); err != nil {
				return fs.Result{}, notFound(err)
			}
		}
		if !op.Mtime.IsZero() {
			if err := os.Chtimes(p, op.Mtime, op.Mtime); err != nil {
				return fs.Result{}, notFound(err)
			}
		}
		info, err := os.Lstat(p)
		if err != nil {
			return fs.Result{}, notFound(err)
		}
		return fs.Result{Attr: c.attrOf(info)}, nil
	}
	return fs.Result{}, fs.ErrUnsupported
}

func limitOf(op *fs.Op) int64 {
	if op.Limit > 0 {
		return op.Limit
	}
	return 1 << 20
}

// localIO implements the layerIO seam over the same directory the core
// serves (markers, pump sources, view walks).
type localIO struct{ root string }

func (l localIO) sysPath(p string) string {
	return filepath.Join(l.root, filepath.FromSlash(strings.TrimPrefix(p, "/")))
}

func (l localIO) stat(p string) (*ioStat, error) {
	info, err := os.Lstat(l.sysPath(p))
	if err != nil {
		return nil, notFound(err)
	}
	rev := info.ModTime().UnixNano()
	if rev <= 0 {
		rev = 1
	}
	return &ioStat{
		IsDir:     info.Mode().IsDir(),
		IsSymlink: info.Mode()&os.ModeSymlink != 0,
		Mode:      uint32(info.Mode().Perm()),
		Size:      info.Size(),
		Rev:       rev,
	}, nil
}

func (l localIO) Stat(ctx context.Context, p string) (*ioStat, error) {
	st, err := l.stat(p)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return st, err
}

func (l localIO) List(ctx context.Context, dir string) ([]ioEntry, error) {
	entries, err := os.ReadDir(l.sysPath(dir))
	if err != nil {
		return nil, notFound(err)
	}
	out := make([]ioEntry, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, ioEntry{
			Name:      e.Name(),
			IsDir:     info.Mode().IsDir(),
			IsSymlink: info.Mode()&os.ModeSymlink != 0,
			Mode:      uint32(info.Mode().Perm()),
			Size:      info.Size(),
		})
	}
	return out, nil
}

func (l localIO) Read(ctx context.Context, p string) ([]byte, error) {
	data, err := os.ReadFile(l.sysPath(p))
	if err != nil {
		return nil, notFound(err)
	}
	return data, nil
}

func (l localIO) ReadStream(ctx context.Context, p string) (io.ReadCloser, error) {
	f, err := os.Open(l.sysPath(p))
	if err != nil {
		return nil, notFound(err)
	}
	return f, nil
}

func (l localIO) Write(ctx context.Context, p string, data []byte) error {
	sp := l.sysPath(p)
	if err := os.MkdirAll(filepath.Dir(sp), 0o755); err != nil {
		return err
	}
	return os.WriteFile(sp, data, 0o644)
}

func (l localIO) Delete(ctx context.Context, p string) error {
	if err := os.Remove(l.sysPath(p)); err != nil {
		return notFound(err)
	}
	return nil
}

func (l localIO) EnsureParent(ctx context.Context, dir string) error {
	return os.MkdirAll(l.sysPath(dir), 0o755)
}

// fakeRegistry is the in-memory metadata plane.
type fakeRegistry struct {
	mu          sync.Mutex
	layers      map[string]string    // layer id -> state
	checkpoints map[string]string    // checkpoint id -> layer id
	events      map[string][]ioEvent // layer id -> event log
	seq         int64
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		layers:      map[string]string{},
		checkpoints: map[string]string{},
		events:      map[string][]ioEvent{},
	}
}

func (r *fakeRegistry) GetLayer(ctx context.Context, ref string) (*ioLayer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.layers[ref]
	if !ok {
		return nil, fs.ErrNotFound
	}
	return &ioLayer{LayerID: ref, State: state}, nil
}

func (r *fakeRegistry) GetCheckpoint(ctx context.Context, checkpointID string) (*ioCheckpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	layerID, ok := r.checkpoints[checkpointID]
	if !ok {
		return nil, fs.ErrNotFound
	}
	return &ioCheckpoint{CheckpointID: checkpointID, LayerID: layerID}, nil
}

func (r *fakeRegistry) ListEvents(ctx context.Context, layerID string, since int64) ([]ioEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []ioEvent
	for _, e := range r.events[layerID] {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	return out, nil
}

// addLayer registers a layer in the given state (default active).
func (r *fakeRegistry) addLayer(id string) {
	r.mu.Lock()
	r.layers[id] = "active"
	r.mu.Unlock()
}

func (r *fakeRegistry) setState(id, state string) {
	r.mu.Lock()
	r.layers[id] = state
	r.mu.Unlock()
}

func (r *fakeRegistry) addCheckpoint(cpID, layerID string) {
	r.mu.Lock()
	r.checkpoints[cpID] = layerID
	r.mu.Unlock()
}

func (r *fakeRegistry) rollback(layerID string) {
	r.mu.Lock()
	r.seq++
	r.events[layerID] = append(r.events[layerID], ioEvent{Seq: r.seq, Op: rollbackEventOp})
	r.mu.Unlock()
}

// stack bundles one [layer → local core] composition with its seams.
type stack struct {
	fsys fs.Filesystem
	ch   *fs.Chain
	l    *Layer
	core *localCore
	reg  *fakeRegistry
	io   localIO
}

// newLocalStack builds the minimal composition: the layer extension over
// a local-filesystem core, exposed through the path-addressed
// fs.Filesystem API. ref may be "" for a live (pass-through) mount.
func newLocalStack(t *testing.T, ref, checkpoint string) *stack {
	t.Helper()
	core := newLocalCore(t)
	reg := newFakeRegistry()
	if ref != "" {
		reg.addLayer(ref)
		if checkpoint != "" {
			reg.addCheckpoint(checkpoint, ref)
		}
	}
	return newLocalStackOver(t, core, reg, ref, checkpoint)
}

// newLocalStackOver composes over an existing core and registry so tests
// can pre-materialize tree or checkpoint state before Start walks it.
func newLocalStackOver(t *testing.T, core *localCore, reg *fakeRegistry, ref, checkpoint string) *stack {
	t.Helper()
	dio := localIO{root: core.root}
	l, err := New(Options{LayerRef: ref, CheckpointRef: checkpoint, IO: dio, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	ch, err := fs.NewChain(l, core)
	if err != nil {
		t.Fatal(err)
	}
	return &stack{
		fsys: fs.HandlerFilesystem(ch.Dispatch),
		ch:   ch,
		l:    l,
		core: core,
		reg:  reg,
		io:   dio,
	}
}

// seed writes a classic-view file, bypassing the shadow.
func (s *stack) seed(t *testing.T, path string, data []byte) {
	t.Helper()
	p := s.core.sysPath(path)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("seed %s: %v", path, err)
	}
}

// treePath is the on-disk location of a view path's tree shadow.
func treePath(viewPath string) string { return layersNamespace + "/L1/" + treeSubdir + viewPath }
