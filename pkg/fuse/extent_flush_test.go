package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestCopyExtentPayloadsSkipsUnwrittenGrowZeros(t *testing.T) {
	wb := NewWriteBuffer("/grow.bin", 1<<20, 0)
	if err := wb.SetSizeOnly(32768); err != nil {
		t.Fatal(err)
	}
	got := copyExtentPayloads(wb, []byteRange{{0, 32768}})
	if len(got) != 0 {
		t.Fatalf("payloads=%d want 0 (JuiceFS grow is holes, not a zero-filled data object)", len(got))
	}
}

func TestFlushExtentEmptyNewFileIsOK(t *testing.T) {
	var mu sync.Mutex
	created := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			mu.Lock()
			created++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
		case strings.Contains(r.URL.RawQuery, "commit-slices"):
			t.Errorf("empty new file must not commit-slices")
			http.Error(w, "no commit", 500)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	wb := NewWriteBuffer("/empty.bin", 1<<20, 0)
	fh := &FileHandle{
		Ino:           3,
		Path:          "/empty.bin",
		Dirty:         wb,
		IsNew:         true,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
		extentWriter:  newExtentFileWriter(),
	}
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	if !handled || st != gofuse.OK {
		t.Fatalf("handled=%v status=%v", handled, st)
	}
	mu.Lock()
	n := created
	mu.Unlock()
	if n != 1 {
		t.Fatalf("creates=%d want 1", n)
	}
	if fh.IsNew {
		t.Fatal("IsNew must clear after create")
	}
}

func TestFlushExtentFrozenCreatesFileBeforeCommit(t *testing.T) {
	var mu sync.Mutex
	var seq []string
	created := false
	var putURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seq = append(seq, r.Method+"?"+r.URL.RawQuery)
		mu.Unlock()
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			mu.Lock()
			created = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
			mu.Lock()
			ok := created
			mu.Unlock()
			if !ok {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"blocks": []map[string]any{{
					"file_off":  0,
					"len":       4,
					"block_key": "blocks/1/a",
					"put_url":   putURL,
					"headers":   map[string]string{},
				}},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			mu.Lock()
			ok := created
			mu.Unlock()
			if !ok {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	putURL = ts.URL + "/put-block"
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	wb := NewWriteBuffer("/large.bin", 8<<20, 0)
	if err := wb.SetSizeOnly(4); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           4,
		Path:          "/large.bin",
		Dirty:         wb,
		IsNew:         true,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
		extentWriter:  newExtentFileWriter(),
	}
	fh.extentWriter.writeAt(0, []byte("abcd"))
	fh.extentWriter.freezeAll()
	fh.Lock()
	st := fs.flushExtentFrozenLocked(context.Background(), fh)
	fh.Unlock()
	if st != gofuse.OK {
		t.Fatalf("frozen flush status=%v seq=%v", st, seq)
	}
	if fh.IsNew {
		t.Fatal("IsNew must clear after freeze flush creates the file")
	}
	if fh.BaseRev != 2 {
		t.Fatalf("BaseRev=%d want 2 after commit", fh.BaseRev)
	}
	mu.Lock()
	got := append([]string(nil), seq...)
	mu.Unlock()
	if len(got) < 2 || !strings.Contains(got[0], "create=1") {
		t.Fatalf("request order %v, want create first", got)
	}
}

func TestFlushExtentFrozenDoesNotCreateWhenIdle(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("idle freeze flush must not hit the server: %s %s", r.Method, r.URL.RawQuery)
		http.Error(w, "no", 500)
	}))
	t.Cleanup(ts.Close)
	fs := &Dat9FS{
		client: client.New(ts.URL, ""),
		opts:   &MountOptions{ExtentPaths: []string{"*"}},
	}
	fh := &FileHandle{
		Path:          "/idle.bin",
		Dirty:         NewWriteBuffer("/idle.bin", 1<<20, 0),
		IsNew:         true,
		ContentLayout: client.ContentLayoutExtent,
		extentWriter:  newExtentFileWriter(),
		extentDirty:   &extentDirtySet{},
	}
	if st := fs.flushExtentFrozenLocked(context.Background(), fh); st != gofuse.OK {
		t.Fatalf("status=%v", st)
	}
	if !fh.IsNew {
		t.Fatal("idle flush must not create")
	}
}

func TestFlushExtentClearsDirtyAfterCommit(t *testing.T) {
	var putURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"blocks": []map[string]any{{
					"file_off":  0,
					"len":       4,
					"block_key": "blocks/1/a",
					"put_url":   putURL,
					"headers":   map[string]string{},
				}},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	putURL = ts.URL + "/put-block"
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	wb := NewWriteBuffer("/cleared.bin", 8<<20, 0)
	if err := wb.SetSizeOnly(4); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:           5,
		Path:          "/cleared.bin",
		Dirty:         wb,
		DirtySeq:      7,
		IsNew:         true,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
		extentWriter:  newExtentFileWriter(),
	}
	fh.extentDirty.add(0, 4)
	fh.extentWriter.writeAt(0, []byte("abcd"))
	fh.Lock()
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	fh.Unlock()
	if !handled || st != gofuse.OK {
		t.Fatalf("handled=%v status=%v", handled, st)
	}
	if fh.Dirty.HasDirtyParts() {
		t.Fatal("committed extent handle must clear dirty parts")
	}
	if fh.DirtySeq != 0 {
		t.Fatalf("DirtySeq=%d want 0", fh.DirtySeq)
	}
	if drainHandleHasDirtyStateLocked(fh) {
		t.Fatal("drain must not count a committed extent handle as dirty")
	}
}

func TestExtentFlushThenSiblingHandleSeesBytes(t *testing.T) {
	var putURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("Content-Length", "4")
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"blocks": []map[string]any{{
					"file_off": 0, "len": 4, "block_key": "blocks/1/a", "put_url": putURL, "headers": map[string]string{},
				}},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 2, "generation": 1, "size_bytes": 4})
		case r.URL.Query().Has("read-plan"):
			_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
				Revision: 2, Generation: 1, SizeBytes: 4,
				Parts: []client.ExtentReadPart{{FileOff: 0, Len: 4, BlockKey: "blocks/1/a", GetURL: putURL}},
			})
		case r.URL.Path == "/put-block":
			_, _ = w.Write([]byte("abcd"))
		default:
			http.NotFound(w, r)
		}
	}))
	putURL = ts.URL + "/put-block"
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		openHandles: NewOpenHandleIndex(),
		fileHandles: NewHandleTable[*FileHandle](),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
	}
	wb := NewWriteBuffer("/sib.bin", 8<<20, 0)
	_ = wb.SetSizeOnly(4)
	fh := &FileHandle{
		Ino: 8, Path: "/sib.bin", Dirty: wb, IsNew: true,
		ContentLayout: client.ContentLayoutExtent, extentDirty: &extentDirtySet{},
	}
	fs.attachExtentWriter(fh)
	fh.extentWriter.writeAt(0, []byte("abcd"))
	fh.Lock()
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	fh.Unlock()
	if !handled || st != gofuse.OK {
		t.Fatalf("flush handled=%v status=%v", handled, st)
	}
	sib := &FileHandle{
		Ino: 8, Path: "/sib.bin", Dirty: NewWriteBuffer("/sib.bin", 8<<20, 0),
		ContentLayout: client.ContentLayoutExtent, BaseRev: 2, OrigSize: 4,
	}
	fs.attachExtentWriter(sib)
	got, err := fs.readExtentRange(context.Background(), sib, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcd" {
		t.Fatalf("sibling after flush got %q", got)
	}
}

func TestFlushAfterWriteGrowDoesNotSendTruncateTo(t *testing.T) {
	var mu sync.Mutex
	var gotTruncate *int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Slice-Generation", "0")
			w.Header().Set("Content-Length", "0")
		case strings.Contains(r.URL.RawQuery, "commit-slices"):
			var body struct {
				TruncateTo *int64 `json:"truncate_to"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			gotTruncate = body.TruncateTo
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	wb := NewWriteBuffer("/grow-write.bin", 8<<20, 0)
	if err := wb.SetSizeOnly(4); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: 12, Path: "/grow-write.bin", Dirty: wb, OrigSize: 0, BaseRev: 1,
		ContentLayout: client.ContentLayoutExtent, extentDirty: &extentDirtySet{},
		extentWriter: newExtentFileWriter(),
	}
	fh.extentDirty.add(0, 4)
	fh.Lock()
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	fh.Unlock()
	if !handled || st != gofuse.OK {
		t.Fatalf("handled=%v status=%v", handled, st)
	}
	mu.Lock()
	got := gotTruncate
	mu.Unlock()
	if got != nil {
		t.Fatalf("JuiceFS Flush must not send truncate_to on write grow, got %d", *got)
	}
}

func TestFlushExtentGrowTruncateCommitsSize(t *testing.T) {
	var mu sync.Mutex
	var gotTruncate *int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Slice-Generation", "0")
			w.Header().Set("Content-Length", "0")
		case strings.Contains(r.URL.RawQuery, "commit-slices"):
			var body struct {
				TruncateTo *int64 `json:"truncate_to"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			gotTruncate = body.TruncateTo
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 1234567,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	wb := NewWriteBuffer("/grow.bin", 8<<20, 0)
	fh := &FileHandle{
		Ino:           9,
		Path:          "/grow.bin",
		Dirty:         wb,
		OrigSize:      0,
		BaseRev:       1,
		ContentLayout: client.ContentLayoutExtent,
		extentDirty:   &extentDirtySet{},
		extentWriter:  newExtentFileWriter(),
	}
	if _, err := fs.truncateWritableHandleLocked(fh, 1234567); err != nil {
		t.Fatal(err)
	}
	fh.Lock()
	handled, st := fs.flushExtentHandle(context.Background(), fh)
	fh.Unlock()
	if !handled || st != gofuse.OK {
		t.Fatalf("handled=%v status=%v", handled, st)
	}
	mu.Lock()
	got := gotTruncate
	mu.Unlock()
	if got == nil || *got != 1234567 {
		t.Fatalf("truncate_to=%v want 1234567", got)
	}
	if fh.OrigSize != 1234567 {
		t.Fatalf("OrigSize=%d want 1234567", fh.OrigSize)
	}
	if fh.extentNeedTruncate {
		t.Fatal("extentNeedTruncate must clear after commit")
	}
}

func TestExtentTruncateToZeroCommitsHolesBeforeLaterWrites(t *testing.T) {
	var mu sync.Mutex
	var commits []map[string]any
	rev := 1
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Slice-Generation", "0")
			w.Header().Set("Content-Length", "8")
		case strings.Contains(r.URL.RawQuery, "prepare-blocks"):
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
					"block_key": "blocks/z", "put_url": "http://127.0.0.1/obj",
					"headers": map[string]string{},
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
		case strings.Contains(r.URL.RawQuery, "commit-slices"):
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			commits = append(commits, body)
			rev++
			outRev := rev
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": outRev, "generation": 1, "size_bytes": 0,
			})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	wb := NewWriteBuffer("/vac.db", 1<<20, 0)
	if _, err := wb.Write(0, []byte("SQLite\x00\x00")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: 11, Path: "/vac.db", Dirty: wb, OrigSize: 8, BaseRev: 1,
		ContentLayout: client.ContentLayoutExtent, extentDirty: &extentDirtySet{},
	}
	fh.Lock()
	if _, err := fs.truncateWritableHandleLocked(fh, 0); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	if _, st := fs.flushExtentHandle(context.Background(), fh); st != gofuse.OK {
		fh.Unlock()
		t.Fatalf("truncate flush status=%v", st)
	}
	fh.Unlock()
	mu.Lock()
	n := len(commits)
	mu.Unlock()
	if n < 1 {
		t.Fatal("JuiceFS Truncate must commit holes before later writes")
	}
}

func TestExtentNoteCommitClearsZeroFromWhenSizeCoversFile(t *testing.T) {
	of := newExtentOpenFile(8)
	of.markZeroFrom(8)
	of.setSize(32)
	of.noteCommit(2, 1, 8)
	if of.zeroFrom != 8 {
		t.Fatalf("zeroFrom=%d want 8 while committed size is below logical", of.zeroFrom)
	}
	of.noteCommit(3, 1, 32)
	if of.zeroFrom != -1 {
		t.Fatalf("zeroFrom=%d want -1 after committed size covers the file", of.zeroFrom)
	}
}

func TestExtentReadAfterTruncateFlushDoesNotClipCommitted(t *testing.T) {
	var plannedOff, plannedLen int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "read-plan") {
			plannedOff, _ = strconv.ParseInt(r.URL.Query().Get("off"), 10, 64)
			plannedLen, _ = strconv.ParseInt(r.URL.Query().Get("len"), 10, 64)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 32, "parts": []any{},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	of := newExtentOpenFile(32)
	of.committed = 32
	of.markZeroFrom(8)
	fs := &Dat9FS{client: client.New(ts.URL, ""), opts: &MountOptions{ExtentPaths: []string{"*"}}}
	fh := &FileHandle{
		Path: "/t.bin", OrigSize: 32, BaseRev: 2,
		ContentLayout: client.ContentLayoutExtent,
		extentOpen:    of, extentNeedTruncate: false,
		Dirty: NewWriteBuffer("/t.bin", 1<<20, 0),
	}
	if err := fh.Dirty.SetSizeOnly(32); err != nil {
		t.Fatal(err)
	}
	got, err := fs.readExtentRange(context.Background(), fh, 16, 8)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if plannedLen == 0 && plannedOff == 0 {
		t.Fatal("JuiceFS meta.Read must Plan past zeroFrom after truncate is committed")
	}
	if int64(len(got)) != 8 {
		t.Fatalf("len=%d want 8", len(got))
	}
}

func TestExtentReadAfterFlushServesStagedCacheOnGet404(t *testing.T) {
	// JuiceFS writeback: Finish stages locally, Flush waits commitThread,
	// reader.Read uses chunk cache. Plan GET is pointed at a 404 URL so
	// a covering staged block must serve the Read (S3 PUT may still be
	// in flight in production).
	var (
		mu       sync.Mutex
		blockKey string
		gets     atomic.Int32
		commits  atomic.Int32
		putURL   string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Resource-ID", "inode1")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
		case r.Method == http.MethodPost && r.URL.RawQuery == "presign-put=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"put_url": putURL, "headers": map[string]string{},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			var body struct {
				Ops []struct {
					BlockKey string `json:"block_key"`
				} `json:"ops"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			if len(body.Ops) > 0 {
				blockKey = body.Ops[0].BlockKey
			}
			mu.Unlock()
			commits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
			})
		case strings.Contains(r.URL.RawQuery, "read-plan"):
			mu.Lock()
			key := blockKey
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
				"parts": []map[string]any{{
					"file_off": 0, "len": 4, "block_key": key, "block_off": 0,
					"get_url": "http://" + r.Host + "/missing",
				}},
			})
		case r.URL.Path == "/missing":
			gets.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	putURL = ts.URL + "/put-block"
	t.Cleanup(ts.Close)

	dir := t.TempDir()
	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		openHandles: NewOpenHandleIndex(),
		opts:        &MountOptions{ExtentPaths: []string{"*"}, CacheDir: dir, DiskReadCacheSize: 1 << 20},
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
	}
	wb := NewWriteBuffer("/cache.bin", 1<<20, 0)
	if _, err := wb.Write(0, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: 26, Path: "/cache.bin", Dirty: wb,
		ContentLayout: client.ContentLayoutExtent, BaseRev: 1,
	}
	fs.attachExtentWriter(fh)
	fh.extentWriter.writeAt(0, []byte("abcd"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if st := fh.extentWriter.flush(ctx); st != gofuse.OK {
		t.Fatalf("flush status=%v", st)
	}
	if commits.Load() < 1 {
		t.Fatal("Flush must commit-slices before reader.Read")
	}
	mu.Lock()
	key := blockKey
	mu.Unlock()
	if key == "" {
		t.Fatal("commit-slices did not record a block key")
	}
	if _, ok := fs.extentDiskCacheGetBlock(key, 0, 4); !ok {
		t.Fatal("writeback Finish must stage covering disk cache")
	}
	fs.extentCache.mu.Lock()
	fs.extentCache.pages = make(map[string]*extentMemPage)
	fs.extentCache.order = nil
	fs.extentCache.curBytes = 0
	fs.extentCache.mu.Unlock()

	got, err := fs.readExtentRange(context.Background(), fh, 0, 4)
	if err != nil {
		t.Fatalf("read err=%v, covering cache must serve writeback blocks before S3 PUT", err)
	}
	if string(got) != "abcd" {
		t.Fatalf("got %q want abcd", got)
	}
	if gets.Load() != 0 {
		t.Fatalf("GET 404s=%d want 0 (disk cache must cover before GET)", gets.Load())
	}
}

func TestExtentReadAfterFlushDoesNotOverlayConcurrentCurrent(t *testing.T) {
	// JuiceFS VFS.Read: Flush then reader.Read with no writer overlay.
	// A sibling Write after Flush must not paint into this Read (btree mix).
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write([]byte("AAAA"))
			return
		}
		if strings.Contains(r.URL.RawQuery, "read-plan") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
				"parts": []map[string]any{{
					"file_off": 0, "len": 4, "block_key": "blocks/a", "block_off": 0,
					"get_url": "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
	}
	wb := NewWriteBuffer("/page.bin", 1<<20, 0)
	if err := wb.SetSizeOnly(4); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: 21, Path: "/page.bin", Dirty: wb, OrigSize: 4, BaseRev: 2,
		ContentLayout: client.ContentLayoutExtent, extentDirty: &extentDirtySet{},
	}
	fs.attachExtentWriter(fh)
	fh.extentOpen.noteCommit(2, 1, 4)

	t.Cleanup(func() { testHookExtentReadAfterUnlock = nil })
	testHookExtentReadAfterUnlock = func(h *FileHandle) {
		if h == nil || h.extentWriter == nil {
			return
		}
		h.extentWriter.writeAt(1, []byte("BB"))
	}

	got, err := fs.readExtentRange(context.Background(), fh, 0, 4)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "AAAA" {
		t.Fatalf("got %q want AAAA (JuiceFS reader.Read must not overlay a concurrent current)", got)
	}
}

func TestExtentReadPlansWhenZeroBaseCommittedWatermarkIsStale(t *testing.T) {
	// VACUUM/truncate can leave ZeroBase set with committed=0 while slices
	// already exist. JuiceFS meta.Read still returns those slices; clipping
	// to committed zeros a btree page (decodeFlags → SQLITE_CORRUPT).
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			_, _ = w.Write([]byte("AAAA"))
			return
		}
		if strings.Contains(r.URL.RawQuery, "read-plan") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 3, "generation": 2, "size_bytes": 4,
				"parts": []map[string]any{{
					"file_off": 0, "len": 4, "block_key": "blocks/a", "block_off": 0,
					"get_url": "http://" + r.Host + "/obj",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
	}
	wb := NewWriteBuffer("/vac.db", 1<<20, 0)
	if err := wb.SetSizeOnly(4); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: 22, Path: "/vac.db", Dirty: wb, OrigSize: 0, BaseRev: 3,
		ZeroBase: true, ContentLayout: client.ContentLayoutExtent,
		extentDirty: &extentDirtySet{},
	}
	fs.attachExtentWriter(fh)
	fh.extentOpen.size = 4
	fh.extentOpen.committed = 0

	got, err := fs.readExtentRange(context.Background(), fh, 0, 4)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "AAAA" {
		t.Fatalf("got %q want AAAA (must Plan, not isLocalOnly zeros)", got)
	}
}

func TestExtentReadSlicesFullObjectWhenRangeIgnored(t *testing.T) {
	obj := []byte("AAAABBBB")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/obj" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(obj)
			return
		}
		if strings.Contains(r.URL.RawQuery, "read-plan") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 8,
				"parts": []map[string]any{{
					"file_off": 4, "len": 4, "block_key": "blocks/c", "block_off": 4,
					"get_url": "http://" + r.Host + "/obj",
					"headers": map[string]string{"Range": "bytes=4-7"},
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
	}
	wb := NewWriteBuffer("/p.bin", 1<<20, 0)
	if err := wb.SetSizeOnly(8); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: 23, Path: "/p.bin", Dirty: wb, OrigSize: 8, BaseRev: 2,
		ContentLayout: client.ContentLayoutExtent, extentDirty: &extentDirtySet{},
	}
	fs.attachExtentWriter(fh)
	fh.extentOpen.noteCommit(2, 1, 8)
	got, err := fs.readExtentRange(context.Background(), fh, 4, 4)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "BBBB" {
		t.Fatalf("got %q want BBBB (compacted block prefix must not replace this page)", got)
	}
}

func TestExtentReadUsesOpenFileChunkCache(t *testing.T) {
	// JuiceFS meta.Read: of.ReadChunk hit skips doRead until InvalidateChunk.
	var plans atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "read-plan") {
			plans.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 8,
				"layout": []map[string]any{{
					"file_off": 0, "len": 8, "block_key": "blocks/a", "block_off": 0, "kind": "data",
				}},
				"parts": []map[string]any{{
					"file_off": 0, "len": 8, "block_key": "blocks/a", "block_off": 0,
					"get_url": "http://" + r.Host + "/missing",
				}},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)

	fs := &Dat9FS{
		client:      client.New(ts.URL, ""),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		extentCache: newExtentReadCache(1 << 20),
	}
	fs.extentCache.putBlock("blocks/a", 0, 8, []byte("abcdefgh"))
	fh := &FileHandle{
		Ino: 31, Path: "/chunk-cache.bin", OrigSize: 8, BaseRev: 2,
		ContentLayout: client.ContentLayoutExtent,
	}
	got, err := fs.readExtentRange(context.Background(), fh, 0, 4)
	if err != nil {
		t.Fatalf("read1: %v", err)
	}
	if string(got) != "abcd" {
		t.Fatalf("read1=%q", got)
	}
	got, err = fs.readExtentRange(context.Background(), fh, 4, 4)
	if err != nil {
		t.Fatalf("read2: %v", err)
	}
	if string(got) != "efgh" {
		t.Fatalf("read2=%q", got)
	}
	if plans.Load() != 1 {
		t.Fatalf("plans=%d want 1 (JuiceFS of.ReadChunk skips doRead)", plans.Load())
	}
}
