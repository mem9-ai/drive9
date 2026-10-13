package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

type fakeNode struct {
	data  []byte
	isDir bool
	mode  uint32 // full mode, including S_IFMT for symlinks
	rev   int64
	mtime int64
}

type fakeBackend struct {
	mu      sync.Mutex
	files   map[string]*fakeNode
	nextRev int64
	// forceConflict makes the next N conditional writes to a path answer
	// 409, simulating a concurrent revision bump.
	forceConflict map[string]int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		files: map[string]*fakeNode{"/": {isDir: true, mode: 0o755}},
		// fixtures below assign revs 1..9; server-assigned revs start at 10
		nextRev:       10,
		forceConflict: map[string]int{},
	}
}

func (b *fakeBackend) node(p string) *fakeNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.files[p]
}

func (b *fakeBackend) writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (b *fakeBackend) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (b *fakeBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/status" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","max_upload_bytes":1073741824,"inline_threshold":1048576,"storage_capabilities":{"append_log_v1":true}}`))
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if p == "" {
		p = "/"
	}
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodHead:
		b.mu.Lock()
		n := b.files[p]
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		w.Header().Set("X-Dat9-IsDir", strconv.FormatBool(n.isDir))
		w.Header().Set("Content-Length", strconv.Itoa(len(n.data)))
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(n.rev, 10))
		w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(n.mode), 10))
		w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(n.mtime, 10))
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodGet && q.Has("list"):
		b.mu.Lock()
		n := b.files[p]
		prefix := "/"
		if p != "/" {
			prefix = p + "/"
		}
		var out []fakeEntry
		if n != nil && n.isDir {
			for cp, child := range b.files {
				if cp == p || !strings.HasPrefix(cp, prefix) {
					continue
				}
				rest := cp[len(prefix):]
				name := rest
				isDir := child.isDir
				if i := strings.IndexByte(rest, '/'); i >= 0 {
					name, isDir = rest[:i], true
				}
				if name == "" || hasName(out, name) {
					continue
				}
				mode := child.mode
				if mode == 0 {
					mode = 0o644
				}
				out = append(out, fakeEntry{Name: name, Size: int64(len(child.data)), IsDir: isDir, Revision: child.rev, Mtime: child.mtime, Mode: mode, HasMode: true})
			}
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		b.writeJSON(w, map[string]any{"entries": out})

	case r.Method == http.MethodGet:
		b.mu.Lock()
		n := b.files[p]
		b.mu.Unlock()
		if n == nil || n.isDir {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			lo, hi, ok := parseRange(rng, int64(len(n.data)))
			if !ok {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi-1, len(n.data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(n.data[lo:hi])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(n.data)

	case r.Method == http.MethodPut:
		body := make([]byte, r.ContentLength)
		if r.ContentLength > 0 {
			if _, err := io.ReadFull(r.Body, body); err != nil {
				b.writeErr(w, http.StatusBadRequest, "read body")
				return
			}
		}
		expected := r.Header.Get("X-Dat9-Expected-Revision")
		b.mu.Lock()
		n := b.files[p]
		if expected == "0" && n != nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "already exists")
			return
		}
		if expected != "" && expected != "0" {
			want, _ := strconv.ParseInt(expected, 10, 64)
			if n == nil || n.rev != want {
				b.mu.Unlock()
				b.writeErr(w, http.StatusConflict, "revision mismatch")
				return
			}
		}
		if b.forceConflict[p] > 0 {
			b.forceConflict[p]--
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "forced conflict")
			return
		}
		rev := b.nextRev
		b.nextRev++
		if n == nil {
			n = &fakeNode{mode: 0o644}
			b.files[p] = n
		}
		n.data = body
		n.rev = rev
		n.mtime = rev
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"revision": rev})

	case r.Method == http.MethodDelete:
		b.mu.Lock()
		n := b.files[p]
		delete(b.files, p)
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "not found")
			return
		}
		w.WriteHeader(http.StatusOK)

	case q.Has("create"):
		b.mu.Lock()
		if b.files[p] != nil {
			b.mu.Unlock()
			b.writeErr(w, http.StatusConflict, "already exists")
			return
		}
		rev := b.nextRev
		b.nextRev++
		b.files[p] = &fakeNode{mode: 0o644, rev: rev, mtime: rev}
		b.mu.Unlock()
		b.writeJSON(w, map[string]any{"revision": rev})

	case q.Has("mkdir"):
		mode := uint32(0o755)
		if m := q.Get("mode"); m != "" {
			if v, err := strconv.ParseUint(m, 10, 32); err == nil {
				mode = uint32(v)
			}
		}
		b.mu.Lock()
		rev := b.nextRev
		b.nextRev++
		b.files[p] = &fakeNode{isDir: true, mode: mode, rev: rev, mtime: rev}
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case q.Has("rename"):
		src := r.Header.Get("X-Dat9-Rename-Source")
		b.mu.Lock()
		n := b.files[src]
		if n != nil {
			delete(b.files, src)
			b.files[p] = n
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "source not found")
			return
		}
		w.WriteHeader(http.StatusOK)

	case q.Has("symlink"):
		var body struct {
			Target string `json:"target"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		b.mu.Lock()
		rev := b.nextRev
		b.nextRev++
		b.files[p] = &fakeNode{data: []byte(body.Target), mode: uint32(syscall.S_IFLNK) | 0o777, rev: rev, mtime: rev}
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case q.Has("hardlink"):
		src := r.Header.Get("X-Dat9-Hardlink-Source")
		b.mu.Lock()
		n := b.files[src]
		if n != nil {
			rev := b.nextRev
			b.nextRev++
			b.files[p] = &fakeNode{data: append([]byte(nil), n.data...), mode: n.mode, rev: rev, mtime: rev}
		}
		b.mu.Unlock()
		if n == nil {
			b.writeErr(w, http.StatusNotFound, "source not found")
			return
		}
		w.WriteHeader(http.StatusOK)

	case q.Has("chmod"):
		var body struct {
			Mode uint32 `json:"mode"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		b.mu.Lock()
		if n := b.files[p]; n != nil {
			n.mode = n.mode&uint32(syscall.S_IFMT) | (body.Mode & 0o777)
		}
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	default:
		b.writeErr(w, http.StatusBadRequest, "unhandled")
	}
}

type fakeEntry struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	IsDir    bool   `json:"isDir"`
	Revision int64  `json:"revision"`
	Mtime    int64  `json:"mtime"`
	Mode     uint32 `json:"mode"`
	HasMode  bool   `json:"hasMode"`
}

func hasName(entries []fakeEntry, name string) bool {
	for i := range entries {
		if entries[i].Name == name {
			return true
		}
	}
	return false
}

func parseRange(rng string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(rng, "bytes=") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(rng, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	lo, err1 := strconv.ParseInt(parts[0], 10, 64)
	hi, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || lo < 0 || hi < lo {
		return 0, 0, false
	}
	if lo >= size {
		return 0, 0, false
	}
	if hi >= size {
		hi = size - 1
	}
	return lo, hi + 1, true
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func newTestCore(t *testing.T, root string) (*Core, *fakeBackend) {
	t.Helper()
	b := newFakeBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{RemoteRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	return c, b
}

func dispatch(t *testing.T, c *Core, op *fs.Op) (fs.Result, error) {
	t.Helper()
	ch, err := fs.NewChain(c)
	if err != nil {
		t.Fatal(err)
	}
	return ch.Dispatch(context.Background(), op)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestStatListRead(t *testing.T) {
	c, b := newTestCore(t, "/")
	b.mu.Lock()
	b.files["/f"] = &fakeNode{data: []byte("hello"), mode: 0o644, rev: 4, mtime: 42}
	b.files["/d"] = &fakeNode{isDir: true, mode: 0o755, rev: 5, mtime: 42}
	b.mu.Unlock()

	res, err := dispatch(t, c, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 5 || res.Attr.Rev != 4 || res.Attr.Mode != 0o644 {
		t.Fatalf("attr = %+v", res.Attr)
	}

	res, err = dispatch(t, c, &fs.Op{Kind: fs.OpGetAttr, Path: "/d"})
	if err != nil || res.Attr == nil || !res.Attr.IsDir() {
		t.Fatalf("dir stat = %+v %v", res.Attr, err)
	}

	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpGetAttr, Path: "/none"}); !fs.IsNotFound(err) {
		t.Fatalf("missing stat = %v, want not-found", err)
	}

	res, err = dispatch(t, c, &fs.Op{Kind: fs.OpList, Path: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 2 {
		t.Fatalf("entries = %+v", res.Entries)
	}

	res, err = dispatch(t, c, &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 1, Limit: 3})
	if err != nil || string(res.Data) != "ell" {
		t.Fatalf("range read = %q %v", res.Data, err)
	}
	res, err = dispatch(t, c, &fs.Op{Kind: fs.OpRead, Path: "/f"})
	if err != nil || string(res.Data) != "hello" {
		t.Fatalf("whole read = %q %v", res.Data, err)
	}
}

func TestWriteSyncIsDurable(t *testing.T) {
	c, b := newTestCore(t, "/")
	b.mu.Lock()
	b.files["/f"] = &fakeNode{data: []byte("hello"), mode: 0o644, rev: 1}
	b.mu.Unlock()

	// patch in the middle: read-modify-write
	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/f", Offset: 5, Data: []byte(" world")}); err != nil {
		t.Fatal(err)
	}
	if n := b.node("/f"); n == nil || string(n.data) != "hello world" || n.rev <= 1 {
		t.Fatalf("backend after patch = %+v", n)
	}

	// overwrite at 0 shorter than the file: POSIX keeps the tail
	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/f", Offset: 0, Data: []byte("new!")}); err != nil {
		t.Fatal(err)
	}
	if n := b.node("/f"); string(n.data) != "new!o world" {
		t.Fatalf("backend after partial overwrite = %q", n.data)
	}

	// overwrite covering the whole file: replaces content outright
	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/f", Offset: 0, Data: []byte("brand new content")}); err != nil {
		t.Fatal(err)
	}
	if n := b.node("/f"); string(n.data) != "brand new content" {
		t.Fatalf("backend after full overwrite = %q", n.data)
	}

	// write onto a missing path at offset 0 creates it
	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/made", Offset: 0, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if n := b.node("/made"); n == nil || string(n.data) != "x" {
		t.Fatalf("created path = %+v", n)
	}

	// sparse write onto a missing path: zero-fill the gap
	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/sparse", Offset: 3, Data: []byte("z")}); err != nil {
		t.Fatal(err)
	}
	if n := b.node("/sparse"); n == nil || len(n.data) != 4 || n.data[3] != 'z' || n.data[0] != 0 {
		t.Fatalf("sparse = %+v", n)
	}
}

func TestWriteSyncConflictRetryAndExhaustion(t *testing.T) {
	c, b := newTestCore(t, "/")
	b.mu.Lock()
	b.files["/f"] = &fakeNode{data: []byte("a"), mode: 0o644, rev: 1}
	b.forceConflict["/f"] = 1 // one transient conflict, then success
	b.mu.Unlock()

	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/f", Offset: 1, Data: []byte("b")}); err != nil {
		t.Fatalf("retryable conflict failed: %v", err)
	}
	if n := b.node("/f"); string(n.data) != "ab" {
		t.Fatalf("backend = %q", n.data)
	}

	b.mu.Lock()
	b.forceConflict["/f"] = 99 // more conflicts than the retry budget
	b.mu.Unlock()
	_, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/f", Offset: 0, Data: []byte("c")})
	if err == nil || fs.StatusOf(err) != gofuse.EAGAIN {
		t.Fatalf("exhausted conflicts = %v, want EAGAIN", err)
	}
}

// TestListKeepsPrivilegeBits locks the readdir attr mask: setuid/sgid/sticky
// survive the listing projection (v1 listed 0o7777; a 0o777 mask made the
// kernel cache post-chmod modes without their privilege bits).
func TestListKeepsPrivilegeBits(t *testing.T) {
	c, b := newTestCore(t, "/")
	b.mu.Lock()
	b.files["/f"] = &fakeNode{data: []byte("x"), mode: 0o4755, rev: 1, mtime: 42}
	b.mu.Unlock()

	res, err := dispatch(t, c, &fs.Op{Kind: fs.OpList, Path: "/"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range res.Entries {
		if e.Name == "f" {
			found = true
			if e.Attr.Mode&0o7777 != 0o4755 {
				t.Fatalf("listed mode = %o, want 4755", e.Attr.Mode&0o7777)
			}
		}
	}
	if !found {
		t.Fatalf("f missing from listing: %+v", res.Entries)
	}
}

// The kept-prefix truncate: a shrink downloads only min(newSize, oldSize)
// bytes and a truncate-to-zero downloads nothing.
func TestSetAttrTruncateReadsOnlyPrefix(t *testing.T) {
	b := testbackend.NewBackend()
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	b.Seed("/f", big)
	ts := httptest.NewServer(b)
	defer ts.Close()
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Shrink to 10 bytes: exactly one ranged GET of the 10-byte prefix.
	if _, err := c.Do(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 10, Mtime: time.Unix(1, 0)}, nil); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); !ok || int64(len(n.Data)) != 10 || string(n.Data[:10]) != string(big[:10]) {
		t.Fatalf("shrunk node = %d bytes", len(n.Data))
	}

	// Truncate to zero: no GET at all.
	before := b.Requests()["GET /v1/fs/f"]
	if _, err := c.Do(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 0}, nil); err != nil {
		t.Fatal(err)
	}
	if after := b.Requests()["GET /v1/fs/f"]; after != before {
		t.Fatalf("truncate-to-zero issued %d GET(s), want 0", after-before)
	}
	if n, _ := b.Node("/f"); len(n.Data) != 0 {
		t.Fatalf("size after zero = %d", len(n.Data))
	}

	// Grow zero-extends from the prefix only.
	b.Seed("/g", []byte("hello"))
	if _, err := c.Do(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/g", Size: 12}, nil); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/g")
	if string(n.Data[:5]) != "hello" || !allZero(n.Data[5:]) || int64(len(n.Data)) != 12 {
		t.Fatalf("grown node = %q", n.Data)
	}
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// Truncating path writes replace the whole image: a document writer whose
// new body is shorter must not leave the previous longer body's tail
// behind (the git workspace store's pointer documents parse as JSON).
func TestWriteSyncTruncateReplacesWholeFile(t *testing.T) {
	c, b := newTestCore(t, "/")
	b.mu.Lock()
	b.files["/doc.json"] = &fakeNode{data: []byte(`{"version":1,"commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","updated_at":"2026-01-01T00:00:00Z"}`), mode: 0o644, rev: 1}
	b.mu.Unlock()

	if _, err := dispatch(t, c, &fs.Op{Kind: fs.OpWrite, Path: "/doc.json", Offset: 0, Data: []byte(`{"version":1}`), Flags: uint32(fs.FlagTruncate)}); err != nil {
		t.Fatal(err)
	}
	if n := b.node("/doc.json"); string(n.data) != `{"version":1}` {
		t.Fatalf("backend after truncate write = %q, want the exact shorter document", n.data)
	}
}
