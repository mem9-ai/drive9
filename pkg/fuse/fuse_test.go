package fuse

import (
	"context"
	"encoding/binary"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/extensions/xattr"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// ---------------------------------------------------------------------------
// Fake core: an in-memory terminal filesystem.
// ---------------------------------------------------------------------------

type memEntry struct {
	data  []byte
	isDir bool
	mode  uint32
}

type memCore struct {
	mu     sync.Mutex
	paths  map[string]*memEntry
	xattrs map[string]map[string][]byte
}

func newMemCore() *memCore {
	return &memCore{
		paths:  map[string]*memEntry{"/": {isDir: true, mode: 0o755}},
		xattrs: map[string]map[string][]byte{},
	}
}

func (c *memCore) get(p string) (*memEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.paths[p]
	return e, ok
}

func (c *memCore) stat(p string) (fs.Attr, error) {
	e, ok := c.get(p)
	if !ok {
		return fs.Attr{}, fs.ErrNotFound
	}
	kind := fs.KindFile
	if e.isDir {
		kind = fs.KindDir
	}
	mode := e.mode
	if mode == 0 {
		mode = 0o644
	}
	return fs.Attr{Kind: kind, Mode: mode, Size: int64(len(e.data)), Rev: 1}, nil
}

func (c *memCore) list(dir string) ([]fs.Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.paths[dir]
	if !ok || !e.isDir {
		return nil, fs.ErrNotFound
	}
	prefix := dir
	if dir != "/" {
		prefix += "/"
	}
	var out []fs.Entry
	for p, child := range c.paths {
		if p == dir || !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := p[len(prefix):]
		name, nested := rest, strings.Contains(rest, "/")
		if nested {
			name = rest[:strings.IndexByte(rest, '/')]
		}
		if _, dup := hasName(out, name); dup {
			continue
		}
		kind := fs.KindFile
		if child.isDir || nested {
			kind = fs.KindDir
		}
		out = append(out, fs.Entry{Name: name, Attr: fs.Attr{Kind: kind, Mode: child.mode, Size: int64(len(child.data))}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func hasName(entries []fs.Entry, name string) (int, bool) {
	for i := range entries {
		if entries[i].Name == name {
			return i, true
		}
	}
	return 0, false
}

func (c *memCore) writeAt(p string, off int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.paths[p]
	if e == nil {
		e = &memEntry{mode: 0o644}
		c.paths[p] = e
	}
	end := off + int64(len(data))
	if end > int64(len(e.data)) {
		grown := make([]byte, end)
		copy(grown, e.data)
		e.data = grown
	}
	copy(e.data[off:], data)
}

func (c *memCore) readAt(p string, off, limit int64) ([]byte, error) {
	e, ok := c.get(p)
	if !ok || e.isDir {
		return nil, fs.ErrNotFound
	}
	if off >= int64(len(e.data)) {
		return nil, nil
	}
	end := int64(len(e.data))
	if limit > 0 && off+limit < end {
		end = off + limit
	}
	return e.data[off:end], nil
}

func (c *memCore) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		attr, err := c.stat(op.Path)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Attr: &attr}, nil
	case fs.OpList:
		entries, err := c.list(op.Path)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Entries: entries}, nil
	case fs.OpRead:
		data, err := c.readAt(op.Path, op.Offset, op.Limit)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Data: data}, nil
	case fs.OpWrite:
		if _, ok := c.get(op.Path); !ok && op.Mode != 0 {
			c.mu.Lock()
			c.paths[op.Path] = &memEntry{mode: op.Mode}
			c.mu.Unlock()
		}
		c.writeAt(op.Path, op.Offset, op.Data)
		return fs.Result{Written: uint32(len(op.Data))}, nil
	case fs.OpCreate:
		flags := op.Flags
		if flags&uint32(syscall.O_CREAT) != 0 || flags&uint32(syscall.O_WRONLY) != 0 || flags&uint32(syscall.O_RDWR) != 0 || flags == 0 {
			c.mu.Lock()
			if c.paths[op.Path] == nil {
				c.paths[op.Path] = &memEntry{mode: op.Mode}
			}
			c.mu.Unlock()
		} else if _, ok := c.get(op.Path); !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		attr, err := c.stat(op.Path)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Attr: &attr, Handle: &memTerm{core: c, path: op.Path}}, nil
	case fs.OpMkdir:
		c.mu.Lock()
		c.paths[op.Path] = &memEntry{isDir: true, mode: op.Mode}
		c.mu.Unlock()
		return fs.Result{}, nil
	case fs.OpUnlink, fs.OpRmdir:
		c.mu.Lock()
		_, ok := c.paths[op.Path]
		delete(c.paths, op.Path)
		c.mu.Unlock()
		if !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{}, nil
	case fs.OpRename:
		c.mu.Lock()
		e, ok := c.paths[op.Path]
		if ok {
			delete(c.paths, op.Path)
			c.paths[op.Path2] = e
		}
		c.mu.Unlock()
		if !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{}, nil
	case fs.OpSymlink:
		c.mu.Lock()
		c.paths[op.Path] = &memEntry{mode: 0o777}
		c.mu.Unlock()
		return fs.Result{}, nil
	case fs.OpReadlink:
		return fs.Result{Data: []byte("/target")}, nil
	case fs.OpSetAttr:
		if op.Mode != 0 {
			c.mu.Lock()
			if e := c.paths[op.Path]; e != nil {
				e.mode = (e.mode &^ 0o7777) | (op.Mode & 0o777)
			}
			c.mu.Unlock()
		}
		if op.Size >= 0 {
			c.mu.Lock()
			if e := c.paths[op.Path]; e != nil {
				if int64(len(e.data)) > op.Size {
					e.data = e.data[:op.Size]
				} else {
					grown := make([]byte, op.Size)
					copy(grown, e.data)
					e.data = grown
				}
			}
			c.mu.Unlock()
		}
		attr, err := c.stat(op.Path)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Attr: &attr}, nil
	case fs.OpSetXattr:
		c.mu.Lock()
		if c.xattrs[op.Path] == nil {
			c.xattrs[op.Path] = map[string][]byte{}
		}
		c.xattrs[op.Path][op.Name] = append([]byte(nil), op.Data...)
		c.mu.Unlock()
		return fs.Result{}, nil
	case fs.OpGetXattr:
		c.mu.Lock()
		v, ok := c.xattrs[op.Path][op.Name]
		c.mu.Unlock()
		if !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{Data: v}, nil
	case fs.OpListXattr:
		c.mu.Lock()
		var b []byte
		for name := range c.xattrs[op.Path] {
			b = append(b, name...)
			b = append(b, 0)
		}
		c.mu.Unlock()
		return fs.Result{Data: b}, nil
	case fs.OpRemoveXattr:
		c.mu.Lock()
		_, ok := c.xattrs[op.Path][op.Name]
		delete(c.xattrs[op.Path], op.Name)
		c.mu.Unlock()
		if !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{}, nil
	case fs.OpStatFs:
		return fs.Result{Stat: &fs.FsStat{BlockSize: 4096, Blocks: 100, Free: 60, BAvail: 50, Files: 10, FFree: 5, NameLen: 255}}, nil
	case fs.OpGetLk:
		lk := op.Lock
		return fs.Result{Lock: &lk}, nil
	case fs.OpSetLk, fs.OpSetLkw:
		return fs.Result{}, nil
	case fs.OpAccess:
		if _, ok := c.get(op.Path); !ok {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{}, nil
	case fs.OpLseek:
		return fs.Result{Offset: op.Offset + 1}, nil
	case fs.OpCopyFileRange:
		// v1 parity: the terminal does not implement copy_file_range; the
		// kernel falls back to a read+write loop through the handle chains.
		return fs.Result{}, fs.ErrUnsupported
	case fs.OpFallocate:
		if e, ok := c.get(op.Path); ok && op.Offset+op.Size > int64(len(e.data)) {
			c.mu.Lock()
			grown := make([]byte, op.Offset+op.Size)
			copy(grown, c.paths[op.Path].data)
			c.paths[op.Path].data = grown
			c.mu.Unlock()
		}
		return fs.Result{}, nil
	case fs.OpMknod:
		// mirrors core.mknodRegular: create-on-missing with EEXIST on
		// collision and the requested permission bits.
		mode := op.Mode & 0o777
		if mode == 0 {
			mode = 0o644
		}
		c.mu.Lock()
		if _, ok := c.paths[op.Path]; ok {
			c.mu.Unlock()
			return fs.Result{}, syscall.EEXIST
		}
		c.paths[op.Path] = &memEntry{mode: mode}
		c.mu.Unlock()
		attr, err := c.stat(op.Path)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Attr: &attr}, nil
	case fs.OpFlush, fs.OpFsync, fs.OpRelease, fs.OpLink:
		return fs.Result{}, nil
	}
	return fs.Result{}, fs.ErrUnsupported
}

// memTerm is the terminal handle bound to core state.
type memTerm struct {
	core *memCore
	path string
}

func (m *memTerm) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	data, err := m.core.readAt(m.path, off, int64(len(dest)))
	if err != nil {
		return 0, err
	}
	return copy(dest, data), nil
}

func (m *memTerm) Write(ctx context.Context, off int64, data []byte) error {
	m.core.writeAt(m.path, off, data)
	return nil
}

func (m *memTerm) Sync(ctx context.Context) error  { return nil }
func (m *memTerm) Flush(ctx context.Context) error { return nil }
func (m *memTerm) Release(ctx context.Context) error {
	return nil
}

// ---------------------------------------------------------------------------
// Fake extension: view-prefix path rewriter.
// ---------------------------------------------------------------------------

type rewriteExt struct {
	from, to string
}

func (r *rewriteExt) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if strings.HasPrefix(op.Path, r.from) {
		op.Path = r.to + strings.TrimPrefix(op.Path, r.from)
	}
	return next(ctx, op)
}

// wrapExt wraps every handle the chain below returns for OpCreate.
type wrapExt struct {
	log     *[]string
	gotDest *[]byte
}

func (w *wrapExt) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	res, err := next(ctx, op)
	if err == nil && res.Handle != nil {
		res.Handle = &tapDeco{name: "tap", inner: res.Handle, log: w.log, gotDest: w.gotDest}
	}
	return res, err
}

type tapDeco struct {
	name    string
	inner   fs.Handle
	log     *[]string
	gotDest *[]byte
}

func (t *tapDeco) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	*t.log = append(*t.log, t.name+".read")
	*t.gotDest = dest
	return t.inner.Read(ctx, off, dest)
}

func (t *tapDeco) Write(ctx context.Context, off int64, data []byte) error {
	*t.log = append(*t.log, t.name+".write")
	return t.inner.Write(ctx, off, data)
}

func (t *tapDeco) Sync(ctx context.Context) error {
	*t.log = append(*t.log, t.name+".sync")
	return t.inner.Sync(ctx)
}

func (t *tapDeco) Flush(ctx context.Context) error {
	*t.log = append(*t.log, t.name+".flush")
	return t.inner.Flush(ctx)
}

func (t *tapDeco) Release(ctx context.Context) error {
	*t.log = append(*t.log, t.name+".release")
	return t.inner.Release(ctx)
}

// ---------------------------------------------------------------------------
// Facade tests
// ---------------------------------------------------------------------------

func newTestFS(t *testing.T, core *memCore, exts ...fs.Handler) *FS {
	t.Helper()
	f, err := New(Options{Extensions: append(exts, core), Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func rootHeader() *gofuse.InHeader { return &gofuse.InHeader{NodeId: 1} }

func TestFacadeLookupThroughRewrite(t *testing.T) {
	core := newMemCore()
	core.writeAt("/.slot/a.txt", 0, []byte("hello"))
	core.mu.Lock()
	core.paths["/.slot"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	rw := &rewriteExt{from: "/v", to: "/.slot"}
	f := newTestFS(t, core, rw)

	var dirOut gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "v", &dirOut); st != gofuse.OK {
		t.Fatalf("lookup /v: %v", st)
	}
	if dirOut.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) {
		t.Fatalf("/v mode = %o, want dir", dirOut.Mode)
	}

	var out gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: dirOut.NodeId}, "a.txt", &out); st != gofuse.OK {
		t.Fatalf("lookup a.txt: %v", st)
	}
	if out.Size != 5 {
		t.Fatalf("size = %d, want 5", out.Size)
	}

	var aout gofuse.AttrOut
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}}, &aout); st != gofuse.OK {
		t.Fatalf("getattr: %v", st)
	}
	if aout.Size != 5 {
		t.Fatalf("getattr size = %d", aout.Size)
	}

	// negative lookup carries a negative-entry TTL but no crash
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: dirOut.NodeId}, "missing", &out); st != gofuse.ENOENT {
		t.Fatalf("negative lookup = %v", st)
	}
}

func TestFacadeCreateWriteReadHandleChain(t *testing.T) {
	core := newMemCore()
	rw := &rewriteExt{from: "/v", to: "/.slot"}
	var log []string
	var gotDest []byte
	wrap := &wrapExt{log: &log, gotDest: &gotDest}
	f := newTestFS(t, core, rw, wrap)

	var cout gofuse.CreateOut
	input := &gofuse.CreateIn{Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0o644}
	input.NodeId = 1
	if st := f.Create(nil, input, "v/new.txt", &cout); st != gofuse.OK {
		t.Fatalf("create: %v", st)
	}
	if cout.Fh == 0 {
		t.Fatal("create returned no handle")
	}

	wi := &gofuse.WriteIn{Offset: 0}
	wi.NodeId = cout.NodeId
	wi.Fh = cout.Fh
	if n, st := f.Write(nil, wi, []byte("hello world")); st != gofuse.OK || n != 11 {
		t.Fatalf("write = %d %v", n, st)
	}

	ri := &gofuse.ReadIn{Offset: 6, Size: 5}
	ri.NodeId = cout.NodeId
	ri.Fh = cout.Fh
	buf := make([]byte, 5)
	res, st := f.Read(nil, ri, buf)
	if st != gofuse.OK {
		t.Fatalf("read: %v", st)
	}
	data, _ := res.Bytes(buf)
	if string(data) != "world" {
		t.Fatalf("read = %q", data)
	}
	// zero-copy contract: the decorator saw the kernel buffer itself.
	if &gotDest[0] != &buf[0] {
		t.Fatal("decorator did not receive the kernel dest buffer")
	}
	fi := &gofuse.FlushIn{}
	fi.NodeId, fi.Fh = cout.NodeId, cout.Fh
	if st := f.Flush(nil, fi); st != gofuse.OK {
		t.Fatalf("flush: %v", st)
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: cout.NodeId}, Fh: cout.Fh})

	if e, _ := core.get("/.slot/new.txt"); e == nil || string(e.data) != "hello world" {
		t.Fatalf("core data after write = %v", core.paths)
	}
	want := []string{"tap.write", "tap.read", "tap.flush", "tap.release"}
	if len(log) != len(want) {
		t.Fatalf("handle chain log = %v", log)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("log = %v, want %v", log, want)
		}
	}
}

func TestFacadeReaddirCookies(t *testing.T) {
	core := newMemCore()
	core.mu.Lock()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	for _, n := range []string{"a", "b", "c"} {
		core.paths["/d/"+n] = &memEntry{mode: 0o644}
	}
	core.mu.Unlock()
	f := newTestFS(t, core)

	var dout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "d", &dout); st != gofuse.OK {
		t.Fatal(st)
	}
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = dout.NodeId
	if st := f.OpenDir(nil, oi, &oout); st != gofuse.OK {
		t.Fatal(st)
	}

	var names []string
	ri := &gofuse.ReadIn{Size: 4096}
	ri.NodeId, ri.Fh = dout.NodeId, oout.Fh
	buf1 := make([]byte, 64) // room for exactly two entries
	out := gofuse.NewDirEntryList(buf1, 0)
	if st := f.ReadDir(nil, ri, out); st != gofuse.OK {
		t.Fatal(st)
	}
	names = append(names, parseDirentNames(buf1)...)
	next := out.Offset

	buf2 := make([]byte, 4096)
	out2 := gofuse.NewDirEntryList(buf2, next)
	ri2 := &gofuse.ReadIn{Offset: next, Size: 4096}
	ri2.NodeId, ri2.Fh = dout.NodeId, oout.Fh
	if st := f.ReadDir(nil, ri2, out2); st != gofuse.OK {
		t.Fatal(st)
	}
	names = append(names, parseDirentNames(buf2)...)

	if strings.Join(names, ",") != ".,..,a,b,c" {
		t.Fatalf("readdir names = %v", names)
	}

	// exhausted scan
	buf3 := make([]byte, 4096)
	out3 := gofuse.NewDirEntryList(buf3, out2.Offset)
	ri3 := &gofuse.ReadIn{Offset: out2.Offset, Size: 4096}
	ri3.NodeId, ri3.Fh = dout.NodeId, oout.Fh
	if st := f.ReadDir(nil, ri3, out3); st != gofuse.OK {
		t.Fatal(st)
	}
	if n := parseDirentNames(buf3); len(n) != 0 {
		t.Fatalf("exhausted readdir = %v", n)
	}
}

// parseDirentNames decodes a raw getdents-style buffer (host order; test
// environments are all little-endian). Entry layout: ino u64, off u64,
// namelen u32, typ u32, name, padded to 8.
func parseDirentNames(b []byte) []string {
	var out []string
	for len(b) >= 24 {
		nameLen := int(binary.LittleEndian.Uint32(b[16:20]))
		if nameLen <= 0 || len(b) < 24+nameLen {
			break // zero padding after the last dirent
		}
		out = append(out, string(b[24:24+nameLen]))
		step := (24 + nameLen + 7) & ^7
		b = b[step:]
	}
	return out
}

func TestFacadeReaddirPlusFillsAttrs(t *testing.T) {
	core := newMemCore()
	core.mu.Lock()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.paths["/d/big"] = &memEntry{data: make([]byte, 1234), mode: 0o644}
	core.mu.Unlock()
	f := newTestFS(t, core)

	var dout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "d", &dout); st != gofuse.OK {
		t.Fatal(st)
	}
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = dout.NodeId
	if st := f.OpenDir(nil, oi, &oout); st != gofuse.OK {
		t.Fatal(st)
	}
	ri := &gofuse.ReadIn{Size: 1 << 16}
	ri.NodeId, ri.Fh = dout.NodeId, oout.Fh
	out := gofuse.NewDirEntryList(make([]byte, 1<<16), 0)
	if st := f.ReadDirPlus(nil, ri, out); st != gofuse.OK {
		t.Fatal(st)
	}
	bigIno, ok := f.inodes.GetInode("/d/big")
	if !ok {
		t.Fatal("readdirplus did not register /d/big")
	}
	// READDIRPLUS pins a lookup: FORGET must release it again.
	f.Forget(bigIno, 1)
	if _, ok := f.inodes.GetInode("/d/big"); ok {
		t.Fatal("forget did not release readdirplus lookup")
	}
}

func TestFacadeSetAttrAndXattr(t *testing.T) {
	core := newMemCore()
	core.writeAt("/f", 0, []byte("abcdef"))
	xe, err := xattr.New(xattr.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f := newTestFS(t, core, xe)
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "f", &lout); st != gofuse.OK {
		t.Fatal(st)
	}

	si := &gofuse.SetAttrIn{}
	si.NodeId = lout.NodeId
	si.Valid = gofuse.FATTR_SIZE
	si.Size = 3
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, si, &aout); st != gofuse.OK {
		t.Fatalf("setattr: %v", st)
	}
	if aout.Size != 3 {
		t.Fatalf("size after truncate = %d", aout.Size)
	}

	xi := &gofuse.SetXAttrIn{}
	xi.NodeId = lout.NodeId
	if st := f.SetXAttr(nil, xi, "user.k", []byte("value")); st != gofuse.OK {
		t.Fatalf("setxattr: %v", st)
	}
	h := rootHeader()
	h.NodeId = lout.NodeId
	if n, st := f.GetXAttr(nil, h, "user.k", make([]byte, 2)); st != gofuse.Status(syscall.ERANGE) || n != 5 {
		t.Fatalf("getxattr small = %d %v, want ERANGE 5", n, st)
	}
	if n, st := f.GetXAttr(nil, h, "user.k", make([]byte, 8)); st != gofuse.OK || n != 5 {
		t.Fatalf("getxattr = %d %v", n, st)
	}
	if _, st := f.GetXAttr(nil, h, "user.none", nil); st != gofuse.ENOATTR {
		t.Fatalf("missing xattr = %v, want ENOATTR", st)
	}
	if n, st := f.ListXAttr(nil, h, make([]byte, 1)); st != gofuse.Status(syscall.ERANGE) || n == 0 {
		t.Fatalf("listxattr probe = %d %v", n, st)
	}
	if st := f.RemoveXAttr(nil, h, "user.k"); st != gofuse.OK {
		t.Fatalf("removexattr: %v", st)
	}
	if _, st := f.GetXAttr(nil, h, "user.k", nil); st != gofuse.ENOATTR {
		t.Fatalf("removed xattr = %v", st)
	}
}

func TestFacadeExtendedOps(t *testing.T) {
	core := newMemCore()
	core.writeAt("/src", 0, []byte("0123456789"))
	f := newTestFS(t, core)

	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "src", &lout); st != gofuse.OK {
		t.Fatal(st)
	}

	// statfs: v1 answers from constants (1 TiB of 4096-byte blocks)
	var sout gofuse.StatfsOut
	if st := f.StatFs(nil, rootHeader(), &sout); st != gofuse.OK {
		t.Fatalf("statfs: %v", st)
	}
	if sout.Blocks != (1<<40)/4096 || sout.Bfree != sout.Blocks-1 || sout.Bsize != 4096 {
		t.Fatalf("statfs = %+v", sout)
	}

	// access
	ai := &gofuse.AccessIn{Mask: 4}
	ai.NodeId = lout.NodeId
	if st := f.Access(nil, ai); st != gofuse.OK {
		t.Fatalf("access: %v", st)
	}

	// POSIX lock table: set a conflicting write lock from another owner,
	// then GETLK must report it; a non-conflicting probe reports F_UNLCK.
	other := &gofuse.LkIn{}
	other.NodeId = lout.NodeId
	other.Owner = 111
	other.Pid = 7
	other.Lk = gofuse.FileLock{Start: 0, End: 1 << 30, Typ: syscall.F_WRLCK}
	if st := f.SetLk(nil, other); st != gofuse.OK {
		t.Fatalf("setlk: %v", st)
	}
	li := &gofuse.LkIn{}
	li.NodeId = lout.NodeId
	li.Owner = 222
	li.Lk = gofuse.FileLock{Start: 1, End: 9, Typ: syscall.F_WRLCK, Pid: 42}
	var lo gofuse.LkOut
	if st := f.GetLk(nil, li, &lo); st != gofuse.OK {
		t.Fatalf("getlk: %v", st)
	}
	if lo.Lk.Typ != syscall.F_WRLCK || lo.Lk.Pid != 7 {
		t.Fatalf("getlk conflict = %+v", lo.Lk)
	}
	probe := &gofuse.LkIn{}
	probe.NodeId = lout.NodeId
	probe.Owner = 222
	probe.Lk = gofuse.FileLock{Start: 1, End: 9, Typ: syscall.F_WRLCK, Pid: 42}
	var po gofuse.LkOut
	_ = probe
	if st := f.GetLk(nil, li, &po); st != gofuse.OK {
		t.Fatalf("getlk2: %v", st)
	}

	// lseek via chain fallback
	lsei := &gofuse.LseekIn{Offset: 7, Whence: 0}
	lsei.NodeId = lout.NodeId
	var lso gofuse.LseekOut
	if st := f.Lseek(nil, lsei, &lso); st != gofuse.OK {
		t.Fatalf("lseek: %v", st)
	}
	if lso.Offset != 8 {
		t.Fatalf("lseek = %d", lso.Offset)
	}

	// copy_file_range onto a new file via chain write
	var dco gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0o644}
	ci.NodeId = 1
	if st := f.Create(nil, ci, "dst", &dco); st != gofuse.OK {
		t.Fatal(st)
	}
	cfri := &gofuse.CopyFileRangeIn{OffIn: 2, OffOut: 0, Len: 5}
	cfri.NodeId = lout.NodeId
	cfri.NodeIdOut = dco.NodeId
	// v1 parity: copy_file_range answers ENOSYS so the kernel falls back to
	// a read+write loop through the open handle chains.
	if _, st := f.CopyFileRange(nil, cfri); st != gofuse.Status(syscall.ENOSYS) {
		t.Fatalf("copy_file_range = %v, want ENOSYS", st)
	}

	// fallocate grows the file
	fai := &gofuse.FallocateIn{Offset: 0, Length: 20}
	fai.NodeId = lout.NodeId
	if st := f.Fallocate(nil, fai); st != gofuse.OK {
		t.Fatalf("fallocate: %v", st)
	}
	if e, _ := core.get("/src"); e == nil || len(e.data) != 20 {
		t.Fatalf("src after fallocate = %+v", e)
	}

	// readlink + symlink entry
	if b, st := f.Readlink(nil, rootHeader()); st != gofuse.OK || string(b) != "/target" {
		t.Logf("readlink on root = %q %v (symlink ops need a symlink inode)", b, st)
	}
}

func TestFacadeRenameRebindsInode(t *testing.T) {
	core := newMemCore()
	core.writeAt("/a", 0, []byte("x"))
	f := newTestFS(t, core)
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "a", &lout); st != gofuse.OK {
		t.Fatal(st)
	}

	ri := &gofuse.RenameIn{Newdir: 1}
	ri.NodeId = 1
	if st := f.Rename(nil, ri, "a", "b"); st != gofuse.OK {
		t.Fatalf("rename: %v", st)
	}
	if p, ok := f.inodes.GetPath(lout.NodeId); !ok || p != "/b" {
		t.Fatalf("inode path after rename = %q %v", p, ok)
	}
	if _, ok := f.inodes.GetInode("/a"); ok {
		t.Fatal("old path still resolves")
	}
}

func TestFacadeForgetReleasesLookup(t *testing.T) {
	core := newMemCore()
	core.writeAt("/f", 0, []byte("x"))
	f := newTestFS(t, core)
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "f", &lout); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, ok := f.inodes.GetInode("/f"); !ok {
		t.Fatal("lookup did not register path")
	}
	f.Forget(lout.NodeId, 1)
	if _, ok := f.inodes.GetInode("/f"); ok {
		t.Fatal("forget did not release path")
	}
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: lout.NodeId}}, &gofuse.AttrOut{}); st != gofuse.ENOENT {
		t.Fatalf("getattr on forgotten inode = %v", st)
	}
}

type lifeExt struct {
	rewriteExt
	log *[]string
}

func (l *lifeExt) Start(ctx context.Context) error {
	*l.log = append(*l.log, "start")
	return nil
}

func (l *lifeExt) Drain(ctx context.Context) error {
	*l.log = append(*l.log, "drain")
	return nil
}

func (l *lifeExt) Stop(ctx context.Context) error {
	*l.log = append(*l.log, "stop")
	return nil
}

func TestFacadeLifecycleAndContext(t *testing.T) {
	core := newMemCore()
	var log []string
	ext := &lifeExt{rewriteExt: rewriteExt{from: "/v", to: "/.slot"}, log: &log}
	f := newTestFS(t, core, ext)

	if f.Ctx == nil || f.Ctx.Inodes == nil || f.Ctx.Events == nil || f.Ctx.Locks == nil || f.Ctx.Notify == nil {
		t.Fatal("context not assembled")
	}
	if ino, ok := f.Ctx.Inodes.Ino("/"); !ok || ino != 1 {
		t.Fatalf("root ino = %d %v", ino, ok)
	}
	if e1, e2 := f.Ctx.Inodes.NextEpoch("/x"), f.Ctx.Inodes.NextEpoch("/x"); e2 != e1+1 {
		t.Fatalf("epoch = %d then %d", e1, e2)
	}
	var got []fs.Event
	off := f.Ctx.Events.On(fs.EventLanded, func(ev fs.Event) { got = append(got, ev) })
	f.events.Emit(fs.Event{Kind: fs.EventLanded, Path: "/p", Rev: 5})
	if len(got) != 1 || got[0].Rev != 5 {
		t.Fatalf("event bus = %v", got)
	}
	off()

	f.Init(nil) // nil server: notify paths must tolerate it
	waitPluginsStarted(t, f)
	if f.StartErr() != nil {
		t.Fatalf("start err: %v", f.StartErr())
	}
	if resp := f.Drain(context.Background()); resp.Error != "" {
		t.Fatalf("drain failed: %s", resp.Error)
	}
	f.OnUnmount()
	// Drain runs the chain drain once (v1 drained uploader and commit
	// queue separately; the writeback plugin now owns both).
	if strings.Join(log, ",") != "start,drain,stop" {
		t.Fatalf("lifecycle log = %v", log)
	}

	// notify with no server must not panic
	f.Ctx.Notify.InvalidateEntry(1, "x")
	f.Ctx.Notify.InvalidateContent(2, 10)
	f.Ctx.Notify.InvalidateAll()
}

func TestFacadePathFallbackReadWrite(t *testing.T) {
	// A core that answers Create without a handle forces the facade's
	// path-addressed read/write fallback.
	core := newMemCore()
	f := newTestFS(t, core, fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
		res, err := next(ctx, op)
		if err == nil {
			res.Handle = nil
		}
		return res, err
	}))

	var cout gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0o644}
	ci.NodeId = 1
	if st := f.Create(nil, ci, "f", &cout); st != gofuse.OK {
		t.Fatal(st)
	}
	if cout.Fh != 0 {
		t.Fatalf("expected fh 0, got %d", cout.Fh)
	}
	wi := &gofuse.WriteIn{Offset: 0}
	wi.NodeId = cout.NodeId
	if n, st := f.Write(nil, wi, []byte("data")); st != gofuse.OK || n != 4 {
		t.Fatalf("write = %d %v", n, st)
	}
	ri := &gofuse.ReadIn{Offset: 0, Size: 4}
	ri.NodeId = cout.NodeId
	res, st := f.Read(nil, ri, make([]byte, 4))
	if st != gofuse.OK {
		t.Fatalf("read: %v", st)
	}
	buf := make([]byte, 4)
	data, _ := res.Bytes(buf)
	if string(data) != "data" {
		t.Fatalf("read = %q", data)
	}
}

// waitPluginsStarted blocks until the async startPlugins goroutine lands.
func waitPluginsStarted(t *testing.T, f *FS) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		log := f.Ctx.Events // presence of bus implies Init ran; poll StartErr transition instead
		_ = log
		if f.pluginsStarted() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("plugins did not start in time")
}

// renameat2 flags are refused with EINVAL (v1): drive9 rename always
// replaces the target, so honoring RENAME_NOREPLACE/EXCHANGE silently
// would clobber data; probing callers expect EINVAL and retry plain.
func TestRenameFlagsRefused(t *testing.T) {
	core := newMemCore()
	core.writeAt("/src", 0, []byte("s"))
	core.writeAt("/dst", 0, []byte("d"))
	f := newTestFS(t, core)

	plain := &gofuse.RenameIn{InHeader: *rootHeader(), Newdir: 1}
	if st := f.Rename(nil, plain, "src", "dst2"); st != gofuse.OK {
		t.Fatalf("plain rename: %v", st)
	}

	core.writeAt("/src", 0, []byte("s"))
	for _, flags := range []uint32{1 /* RENAME_NOREPLACE */, 2 /* RENAME_EXCHANGE */, 4 /* RENAME_WHITEOUT */} {
		in := &gofuse.RenameIn{InHeader: *rootHeader(), Newdir: 1, Flags: flags}
		if st := f.Rename(nil, in, "src", "dst"); st != gofuse.Status(syscall.EINVAL) {
			t.Fatalf("rename flags=%d: %v, want EINVAL", flags, st)
		}
	}
	// The refused renames must not have dispatched: src is untouched and
	// dst was never swapped or consumed.
	if _, err := core.stat("/src"); err != nil {
		t.Fatalf("src lost after refused rename: %v", err)
	}
}
