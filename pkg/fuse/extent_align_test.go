package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestExtentPageCacheRoundTripChecksumIdle(t *testing.T) {
	c := newExtentReadCache(2 << 20)
	page := make([]byte, extentPageSize)
	for i := range page {
		page[i] = byte(i)
	}
	c.putBlock("blocks/a", 0, int64(len(page)), page)
	got, ok := c.getBlock("blocks/a", 0, int64(len(page)))
	if !ok {
		t.Fatal("64KB page must round-trip")
	}
	if len(got) != len(page) {
		t.Fatalf("got len=%d want %d", len(got), len(page))
	}
	for i := range page {
		if got[i] != page[i] {
			t.Fatalf("byte %d = %d want %d", i, got[i], page[i])
		}
	}

	c.mu.Lock()
	p := c.pages[extentPageKey("blocks/a", 0)]
	if p == nil {
		c.mu.Unlock()
		t.Fatal("missing stored page")
	}
	p.sum[0] ^= 0xff
	c.mu.Unlock()
	if _, ok := c.getBlock("blocks/a", 0, int64(len(page))); ok {
		t.Fatal("corrupt checksum must miss")
	}

	c.putBlock("blocks/b", 0, int64(len(page)), page)
	c.mu.Lock()
	for _, pg := range c.pages {
		pg.atime = time.Now().Add(-time.Hour)
	}
	c.mu.Unlock()
	c.reclaimIdle(time.Now())
	if _, ok := c.getBlock("blocks/b", 0, int64(len(page))); ok {
		t.Fatal("idle reclaim must drop the page")
	}
}

func TestExtentDiskCacheColonDirsHash(t *testing.T) {
	d1 := t.TempDir()
	d2 := t.TempDir()
	fs := &Dat9FS{opts: &MountOptions{CacheDir: d1 + ":" + d2, DiskReadCacheSize: 1 << 20}}
	for i := 0; i < 32; i++ {
		payload := []byte(fmt.Sprintf("block-%02d-data", i))
		key := fmt.Sprintf("blocks/%d", i)
		fs.extentDiskCachePut(key, 0, int64(len(payload)), payload)
		got, ok := fs.extentDiskCacheGetBlock(key, 0, int64(len(payload)))
		if !ok {
			t.Fatalf("round-trip miss key=%s", key)
		}
		if string(got) != string(payload) {
			t.Fatalf("key=%s got %q", key, got)
		}
	}
	n1 := countExtentRawFiles(t, d1)
	n2 := countExtentRawFiles(t, d2)
	if n1 == 0 || n2 == 0 {
		t.Fatalf("both cache dirs must receive hashed keys, n1=%d n2=%d", n1, n2)
	}
}

func countExtentRawFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(filepath.Join(root, "extent", "raw"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() || filepath.Ext(path) == ".tmp" {
			return nil
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return n
}

func newExtentCopyDst(t *testing.T) *FileHandle {
	t.Helper()
	return &FileHandle{
		Ino:           3,
		Path:          "/dst.db",
		Dirty:         NewWriteBuffer("/dst.db", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent,
		extentWriter:  newExtentFileWriter(),
		extentDirty:   &extentDirtySet{},
	}
}

func TestExtentCopyFileRangeUserspaceReadOnlySrc(t *testing.T) {
	payload := []byte("hello world")
	var gets int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		gets++
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		fileHandles: NewHandleTable[*FileHandle](),
		dirtyInodes: map[uint64]dirtyInodeState{},
		inodes:      NewInodeToPath(),
		opts:        &MountOptions{},
	}
	src := &FileHandle{
		Ino: 2, Path: "/src.txt", OrigSize: int64(len(payload)),
		ContentLayout: client.ContentLayoutSingle,
	}
	if src.Dirty != nil {
		t.Fatal("O_RDONLY source must have Dirty==nil")
	}
	dst := newExtentCopyDst(t)
	fhIn := fs.fileHandles.Allocate(src)
	fhOut := fs.fileHandles.Allocate(dst)
	written, st := fs.CopyFileRange(nil, &gofuse.CopyFileRangeIn{
		FhIn: fhIn, FhOut: fhOut, OffIn: 6, OffOut: 0, Len: 5,
	})
	if st != gofuse.OK {
		t.Fatalf("status=%v want OK (not ENOSYS)", st)
	}
	if written != 5 {
		t.Fatalf("written=%d want 5", written)
	}
	if gets == 0 {
		t.Fatal("read-only copy must hit ReadStreamRange, not skip the client")
	}
	got := make([]byte, 5)
	dst.extentWriter.readAt(0, got)
	if string(got) != "world" {
		t.Fatalf("dst=%q want world (got zeros if RO src was skipped)", got)
	}
}

func TestExtentCopyFileRangeUserspaceLazySrc(t *testing.T) {
	const partSize int64 = 8
	payload := []byte("xxxxxxxxworld")
	loadCalls := 0
	wb := NewWriteBuffer("/src.txt", 1<<20, partSize)
	wb.totalSize = int64(len(payload))
	wb.remoteSize = int64(len(payload))
	wb.LoadPart = func(partNum int) ([]byte, error) {
		loadCalls++
		start := int64(partNum-1) * partSize
		if start >= int64(len(payload)) {
			return nil, nil
		}
		end := start + partSize
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}
		return append([]byte(nil), payload[start:end]...), nil
	}
	if _, err := wb.Write(0, []byte("xxxxxxxx")); err != nil {
		t.Fatal(err)
	}
	if wb.IsPartLoaded(1) {
		t.Fatal("part 1 must stay unloaded so EnsureLoaded/LoadPart is the copy source")
	}

	fs := &Dat9FS{
		fileHandles: NewHandleTable[*FileHandle](),
		dirtyInodes: map[uint64]dirtyInodeState{},
		inodes:      NewInodeToPath(),
		opts:        &MountOptions{},
	}
	src := &FileHandle{Ino: 2, Path: "/src.txt", Dirty: wb, ContentLayout: client.ContentLayoutSingle}
	dst := newExtentCopyDst(t)
	fhIn := fs.fileHandles.Allocate(src)
	fhOut := fs.fileHandles.Allocate(dst)
	written, st := fs.CopyFileRange(nil, &gofuse.CopyFileRangeIn{
		FhIn: fhIn, FhOut: fhOut, OffIn: 8, OffOut: 0, Len: 5,
	})
	if st != gofuse.OK {
		t.Fatalf("status=%v want OK", st)
	}
	if written != 5 {
		t.Fatalf("written=%d want 5", written)
	}
	if loadCalls == 0 {
		t.Fatal("lazy copy must call LoadPart for the unloaded part")
	}
	got := make([]byte, 5)
	dst.extentWriter.readAt(0, got)
	if string(got) != "world" {
		t.Fatalf("dst=%q want world (got zeros if unloaded part was ReadAt without EnsureLoaded)", got)
	}
}

func TestExtentFallocateZeroRangeGrowsHole(t *testing.T) {
	var mu sync.Mutex
	var commit map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("commit-slices") {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("decode: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		mu.Lock()
		commit = decoded
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"revision": 2, "generation": 1, "size_bytes": 24,
		})
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		fileHandles: NewHandleTable[*FileHandle](),
		dirtyInodes: map[uint64]dirtyInodeState{},
		inodes:      NewInodeToPath(),
		opts:        &MountOptions{},
	}
	wb := NewWriteBuffer("/t.db", 1<<20, 0)
	if err := wb.SetSizeOnly(8); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           4,
		Path:          "/t.db",
		Dirty:         wb,
		OrigSize:      8,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
		extentWriter:  newExtentFileWriter(),
		extentDirty:   &extentDirtySet{},
	}
	id := fs.fileHandles.Allocate(fh)
	st := fs.Fallocate(nil, &gofuse.FallocateIn{
		Fh: id, Offset: 8, Length: 16, Mode: fallocZeroRange,
	})
	if st != gofuse.OK {
		t.Fatalf("status=%v", st)
	}
	if fh.Dirty.Size() != 24 {
		t.Fatalf("size=%d want 24", fh.Dirty.Size())
	}
	mu.Lock()
	got := commit
	mu.Unlock()
	if got == nil {
		t.Fatal("commit-slices not called")
	}
	tt, _ := got["truncate_to"].(float64)
	if int64(tt) != 24 {
		t.Fatalf("truncate_to=%v want 24", got["truncate_to"])
	}
	ops, _ := got["ops"].([]any)
	if len(ops) == 0 {
		t.Fatalf("ops empty: %+v", got)
	}
	for i, raw := range ops {
		op, _ := raw.(map[string]any)
		if op["kind"] != "hole" {
			t.Fatalf("op[%d] kind=%v want hole (no stored zeros)", i, op["kind"])
		}
		if key, _ := op["block_key"].(string); key != "" {
			t.Fatalf("op[%d] has block_key=%q, want hole without payload", i, key)
		}
	}
}

func TestExtentFallocateGrowDoesNotCommitHoles(t *testing.T) {
	// JuiceFS doFallocate grow updates length only. Hole slices on grow
	// last-write-wins over a btree page that SIZE_HINT had not yet rewritten
	// (wal-multiwrite page 5).
	var mu sync.Mutex
	var commit map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("commit-slices") {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("decode: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		mu.Lock()
		commit = decoded
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"revision": 2, "generation": 1, "size_bytes": 24,
		})
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		fileHandles: NewHandleTable[*FileHandle](),
		dirtyInodes: map[uint64]dirtyInodeState{},
		inodes:      NewInodeToPath(),
		opts:        &MountOptions{},
	}
	wb := NewWriteBuffer("/t.db", 1<<20, 0)
	if err := wb.SetSizeOnly(8); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           4,
		Path:          "/t.db",
		Dirty:         wb,
		OrigSize:      8,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
		extentWriter:  newExtentFileWriter(),
		extentDirty:   &extentDirtySet{},
	}
	id := fs.fileHandles.Allocate(fh)
	st := fs.Fallocate(nil, &gofuse.FallocateIn{
		Fh: id, Offset: 8, Length: 16, Mode: 0,
	})
	if st != gofuse.OK {
		t.Fatalf("status=%v", st)
	}
	if fh.Dirty.Size() != 24 {
		t.Fatalf("size=%d want 24", fh.Dirty.Size())
	}
	if fh.extentNeedTruncate {
		t.Fatal("fallocate grow must persist length, not defer truncate holes")
	}
	mu.Lock()
	got := commit
	mu.Unlock()
	if got == nil {
		t.Fatal("commit-slices not called")
	}
	if grow, _ := got["grow_length"].(bool); !grow {
		t.Fatalf("grow_length=%v want true: %+v", got["grow_length"], got)
	}
	ops, _ := got["ops"].([]any)
	for i, raw := range ops {
		op, _ := raw.(map[string]any)
		if op["kind"] == "hole" {
			t.Fatalf("op[%d] is a hole, JuiceFS fallocate grow must not punch: %+v", i, got)
		}
	}
}

func TestExtentReadOverlaysFrozenWithoutCommit(t *testing.T) {
	var mu sync.Mutex
	var ops []string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		switch {
		case r.URL.Path == "/obj":
			ops = append(ops, r.Method+" /obj")
			mu.Unlock()
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte("AAAA"))
			}
			return
		case q.Has("prepare-blocks"):
			ops = append(ops, "prepare-blocks")
			mu.Unlock()
			var req struct {
				Ranges []struct {
					FileOff int64 `json:"file_off"`
					Len     int64 `json:"len"`
				} `json:"ranges"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			blocks := make([]map[string]any, 0, len(req.Ranges))
			for _, rng := range req.Ranges {
				blocks = append(blocks, map[string]any{
					"file_off":  rng.FileOff,
					"len":       rng.Len,
					"block_key": "blocks/frozen",
					"put_url":   srv.URL + "/obj",
					"headers":   map[string]string{},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
			return
		case q.Has("commit-slices"):
			ops = append(ops, "commit-slices")
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 8,
			})
			return
		case q.Has("read-plan"):
			ops = append(ops, "read-plan")
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 2, Generation: 1, SizeBytes: 8,
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: 4, BlockKey: "blocks/frozen", BlockOff: 0,
					GetURL: srv.URL + "/obj",
				}},
			})
			return
		default:
			mu.Unlock()
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		openHandles: NewOpenHandleIndex(),
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
		opts:        &MountOptions{},
	}
	wb := NewWriteBuffer("/t.db", 1<<20, 0)
	if err := wb.SetSizeOnly(8); err != nil {
		t.Fatal(err)
	}
	w := newExtentFileWriter()
	w.writeAt(0, []byte("AAAA"))
	w.freezeAll()
	w.writeAt(4, []byte("BBBB"))
	fh := &FileHandle{
		Ino:           5,
		Path:          "/t.db",
		Dirty:         wb,
		OrigSize:      8,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
		extentWriter:  w,
		extentDirty:   &extentDirtySet{},
	}
	got, err := fs.readExtentRange(context.Background(), fh, 0, 8)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "AAAABBBB" {
		t.Fatalf("got %q want AAAABBBB", got)
	}
	if w.frozenCount() != 1 {
		t.Fatalf("frozenCount=%d want 1 (read overlays frozen; sync PUT on every read is not JuiceFS async flush)", w.frozenCount())
	}
	mu.Lock()
	seen := append([]string(nil), ops...)
	mu.Unlock()
	for _, op := range seen {
		if op == "prepare-blocks" || op == "commit-slices" {
			t.Fatalf("read must overlay frozen slices; ops=%v", seen)
		}
	}
}

func TestReadExtentRangeSparseGrowBeforeFlush(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write([]byte("ABCDEFGH"))
			return
		}
		if r.URL.Query().Has("read-plan") {
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 1, Generation: 0, SizeBytes: 8,
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: 8, BlockKey: "blocks/a", BlockOff: 0,
					GetURL: "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
		opts:        &MountOptions{},
	}
	wb := NewWriteBuffer("/sparse.bin", 1<<20, 0)
	if _, err := wb.Write(0, []byte("ABCDEFGH")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           7,
		Path:          "/sparse.bin",
		Dirty:         wb,
		OrigSize:      8,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
		extentWriter:  newExtentFileWriter(),
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 24); err != nil {
		t.Fatal(err)
	}
	got, err := fs.readExtentRange(context.Background(), fh, 8, 8)
	if err != nil {
		t.Fatalf("read grown region: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("len=%d want 8", len(got))
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("grown byte %d = %q, want 0 (not committed prefix)", i, got)
		}
	}
}

func TestReadExtentRangeTruncateDownThenUpIsHoles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write(bytesRepeat('X', 32))
			return
		}
		if r.URL.Query().Has("read-plan") {
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 1, Generation: 0, SizeBytes: 32,
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: 32, BlockKey: "blocks/a", BlockOff: 0,
					GetURL: "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	wb := NewWriteBuffer("/fsx.bin", 1<<20, 0)
	if _, err := wb.Write(0, bytesRepeat('X', 32)); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           11,
		Path:          "/fsx.bin",
		Dirty:         wb,
		OrigSize:      32,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
	}
	fs.attachExtentWriter(fh)
	fh.extentWriter.writeAt(0, bytesRepeat('X', 32))
	if _, err := fs.truncateWritableHandleLocked(fh, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 32); err != nil {
		t.Fatal(err)
	}
	got, err := fs.readExtentRange(context.Background(), fh, 16, 8)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("len=%d want 8", len(got))
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("byte %d=%q want 0 after truncate-down then up (not remote tail)", i, got)
		}
	}
}

func TestReadExtentRangeFrozenFlushKeepsZeroFromTail(t *testing.T) {
	var mu sync.Mutex
	var lastCommit map[string]any
	var commitCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write(bytesRepeat('X', 32))
			return
		}
		if r.URL.Query().Has("prepare-blocks") {
			var req struct {
				Ranges []struct {
					FileOff int64 `json:"file_off"`
					Len     int64 `json:"len"`
				} `json:"ranges"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			blocks := make([]map[string]any, 0, len(req.Ranges))
			for _, rng := range req.Ranges {
				blocks = append(blocks, map[string]any{
					"file_off": rng.FileOff, "len": rng.Len,
					"block_key": "blocks/frozen", "put_url": "http://" + r.Host + "/obj",
					"headers": map[string]string{},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
			return
		}
		if r.URL.Query().Has("commit-slices") {
			var decoded map[string]any
			_ = json.NewDecoder(r.Body).Decode(&decoded)
			mu.Lock()
			n := commitCount
			commitCount++
			lastCommit = decoded
			mu.Unlock()
			if n > 0 {
				http.Error(w, "busy", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 32,
			})
			return
		}
		if r.URL.Query().Has("read-plan") {
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 2, Generation: 1, SizeBytes: 32,
				Parts: []client.ExtentReadPart{{
					FileOff: 0, Len: 32, BlockKey: "blocks/a", BlockOff: 0,
					GetURL: "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	wb := NewWriteBuffer("/fsx.bin", 1<<20, 0)
	if _, err := wb.Write(0, bytesRepeat('X', 32)); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           12,
		Path:          "/fsx.bin",
		Dirty:         wb,
		OrigSize:      32,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
	}
	// Unbound writer: freeze must not start JuiceFS flushData. This case
	// covers hole/zeroFrom bookkeeping on a later frozen flush.
	of := newExtentOpenFile(32)
	of.rev = 1
	of.committed = 32
	fh.extentOpen = of
	fh.extentWriter = of.writer
	fh.extentWriter.writeAt(0, bytesRepeat('A', 4))
	fh.extentWriter.freezeAll()
	fh.extentWriter.writeAt(4, bytesRepeat('B', 4))
	if _, err := fs.truncateWritableHandleLocked(fh, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 16); err != nil {
		t.Fatal(err)
	}
	fh.Lock()
	st := fs.flushExtentFrozenLocked(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("frozen flush status=%v", st)
	}
	mu.Lock()
	commit := lastCommit
	mu.Unlock()
	if commit["truncate_to"] != nil {
		t.Fatalf("frozen flush with current must not send truncate_to: %+v", commit)
	}
	if extentOpenZeroFrom(fh) != 16 {
		t.Fatalf("zeroFrom=%d want 16 (punched end captured before unlock)", extentOpenZeroFrom(fh))
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 32); err != nil {
		t.Fatal(err)
	}
	got, err := fs.readExtentRange(context.Background(), fh, 16, 8)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("byte %d=%q want 0 after frozen flush then trunc-up", i, got)
		}
	}
}

func TestReadExtentRangeFlushHandleClearsZeroFromWhenHolesCoverSize(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Slice-Generation", "0")
			w.Header().Set("Content-Length", "32")
		case r.URL.Path == "/obj":
			w.WriteHeader(http.StatusOK)
		case r.URL.Query().Has("prepare-blocks"):
			var req struct {
				Ranges []struct {
					FileOff int64 `json:"file_off"`
					Len     int64 `json:"len"`
				} `json:"ranges"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			blocks := make([]map[string]any, 0, len(req.Ranges))
			for _, rng := range req.Ranges {
				blocks = append(blocks, map[string]any{
					"file_off": rng.FileOff, "len": rng.Len,
					"block_key": "blocks/w", "put_url": srv.URL + "/obj",
					"headers": map[string]string{},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
		case r.URL.Query().Has("commit-slices"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 32,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	fs := &Dat9FS{
		client:      newTestClient(srv.URL),
		inodes:      NewInodeToPath(),
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	wb := NewWriteBuffer("/fsx.bin", 1<<20, 0)
	if _, err := wb.Write(0, bytesRepeat('X', 32)); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           13,
		Path:          "/fsx.bin",
		Dirty:         wb,
		OrigSize:      32,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
	}
	fs.attachExtentWriter(fh)
	if _, err := fs.truncateWritableHandleLocked(fh, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 32); err != nil {
		t.Fatal(err)
	}
	fh.extentWriter.writeAt(12, []byte("CCCC"))
	fh.Lock()
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	fh.Unlock()
	if !handled || st != gofuse.OK {
		t.Fatalf("handled=%v status=%v", handled, st)
	}
	if z := extentOpenZeroFrom(fh); z != -1 {
		t.Fatalf("zeroFrom=%d want -1 after holes covering result size", z)
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
