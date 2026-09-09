package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestExtentFileWriterSplitsOnChunkAndBlock(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, make([]byte, client.ExtentMaxBlockSize+8))
	if w.sliceCount() != 2 {
		t.Fatalf("sliceCount=%d want 2 (4MiB cap)", w.sliceCount())
	}
	w2 := newExtentFileWriter()
	w2.writeAt(client.ExtentChunkSize-8, make([]byte, 16))
	if w2.sliceCount() != 2 {
		t.Fatalf("sliceCount=%d want 2 (64MiB chunk)", w2.sliceCount())
	}
}

func TestExtentFileWriterNonContiguousStartsNewSlice(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("abcdefgh"))
	w.writeAt(32, []byte("ijklmnop"))
	if w.sliceCount() != 2 {
		t.Fatalf("sliceCount=%d want 2", w.sliceCount())
	}
}

func TestExtentWriterFlushUnblocksWriteWaiters(t *testing.T) {
	// JuiceFS fileWriter.Write waits flushwaiting>0. Read used to call
	// flushExtentHandle (beginFlush + commitMu) and never drop flushwaiting
	// on the SELECT-after-build path; write waiters then hang until timeout.
	w := newExtentFileWriter()
	w.beginFlush()
	done := make(chan struct{})
	go func() {
		w.writeAt(0, []byte("ab"))
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		n := w.writewaiting
		w.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("write did not wait for flushwaiting")
		}
		time.Sleep(time.Millisecond)
	}
	w.endFlush()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("write stuck behind flushwaiting after endFlush")
	}
}

func TestExtentFileWriterFreezeKeepsInteriorZeros(t *testing.T) {
	// JuiceFS flushData uploads slen as written. A 32KiB write with
	// interior zero pages is one slice, not a KEEP_CACHE hole.
	buf := bytes.Repeat([]byte{'A'}, 32768)
	copy(buf[12288:16384], make([]byte, 4096))
	copy(buf[28672:32768], make([]byte, 4096))
	w := newExtentFileWriter()
	w.writeAt(0, buf)
	payloads := w.freezeAllPayloads()
	if len(payloads) != 1 {
		t.Fatalf("payloads=%d want 1 (JuiceFS one slen, including zeros)", len(payloads))
	}
	if payloads[0].FileOff != 0 || len(payloads[0].Data) != 32768 {
		t.Fatalf("payload off=%d len=%d want 0/32768", payloads[0].FileOff, len(payloads[0].Data))
	}
	if !bytes.Equal(payloads[0].Data[12288:16384], make([]byte, 4096)) {
		t.Fatal("interior zero page must be stored")
	}
}

func TestExtentCommitErrorStaysStickyAndKeepsOverlay(t *testing.T) {
	// JuiceFS commitThread sets f.err and does not clear it. Dropping the
	// failed slice let sibling Reads plan meta holes (btreeInitPage zeros).
	w := newExtentFileWriter()
	w.writeAt(0, []byte("abcd"))
	w.freezeAll()
	w.mu.Lock()
	if len(w.pending) != 1 {
		w.mu.Unlock()
		t.Fatalf("pending=%d want 1", len(w.pending))
	}
	w.pending[0].done = true
	w.pending[0].err = errors.New("put failed")
	w.mu.Unlock()
	w.commitLoop()
	if w.status() != gofuse.EIO {
		t.Fatalf("status=%v want EIO", w.status())
	}
	if w.frozenCount() != 1 {
		t.Fatalf("frozen=%d want 1 (keep overlay after failed commit)", w.frozenCount())
	}
	got := make([]byte, 4)
	w.readAt(0, got)
	if string(got) != "abcd" {
		t.Fatalf("overlay=%q want abcd", got)
	}
}

func TestExtentFileWriterSparsePagesDoNotZeroFillGap(t *testing.T) {
	// JuiceFS findWritableSlice rejects pos > off+slen. Filling the gap
	// with zeros and committing one span overlays later 4K sqlite pages
	// (wal-multiwrite pages 4 and 8 were durable zeros in a 729088 slice).
	w := newExtentFileWriter()
	page := make([]byte, 4096)
	for i := range page {
		page[i] = 'A'
	}
	w.writeAt(0, page)
	page4 := make([]byte, 4096)
	for i := range page4 {
		page4[i] = 'B'
	}
	w.writeAt(16384, page4)
	if w.sliceCount() != 2 {
		t.Fatalf("sliceCount=%d want 2 (page 0 and page 4, not one zero-filled span)", w.sliceCount())
	}
	payloads := w.freezeAllPayloads()
	if len(payloads) != 2 {
		t.Fatalf("payloads=%d want 2", len(payloads))
	}
	for _, p := range payloads {
		if int64(len(p.Data)) != 4096 {
			t.Fatalf("payload off=%d len=%d want 4096 (no interior zeros)", p.FileOff, len(p.Data))
		}
		if p.FileOff != 0 && p.FileOff != 16384 {
			t.Fatalf("unexpected payload off=%d", p.FileOff)
		}
	}
}

func TestExtentFileWriterOverlapFrozenStartsNewSlice(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("AAAAAAAA"))
	w.freezeAll()
	w.writeAt(0, []byte("BBBB"))
	if w.sliceCount() != 2 {
		t.Fatalf("sliceCount=%d want 2 (JuiceFS overlap with frozen starts a new slice)", w.sliceCount())
	}
	got := make([]byte, 8)
	w.readAt(0, got)
	if string(got) != "BBBBAAAA" {
		t.Fatalf("overlay=%q want BBBBAAAA", got)
	}
}

func TestExtentFileWriterOwnsBytesAndOverlays(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("hello"))
	w.writeAt(2, []byte("XY"))
	got := make([]byte, 5)
	w.readAt(0, got)
	if string(got) != "heXYo" {
		t.Fatalf("overlay=%q want heXYo", got)
	}
}

func TestExtentFileWriterInRangeOverwriteKeepsOneSlice(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("hello"))
	w.writeAt(2, []byte("XY"))
	if w.sliceCount() != 1 {
		t.Fatalf("sliceCount=%d want 1 after in-range overwrite", w.sliceCount())
	}
	got := make([]byte, 5)
	w.readAt(0, got)
	if string(got) != "heXYo" {
		t.Fatalf("readAt=%q want heXYo", got)
	}
	w.writeAt(5, []byte("!"))
	if w.sliceCount() != 1 {
		t.Fatalf("sliceCount=%d want 1 after append at current end", w.sliceCount())
	}
	got = make([]byte, 6)
	w.readAt(0, got)
	if string(got) != "heXYo!" {
		t.Fatalf("append=%q want heXYo!", got)
	}
}

func TestExtentFileWriterFreezeDueIdle(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("abcdefgh"))
	past := time.Now().Add(-2 * time.Second)
	w.chunks[0].current.lastMod = past
	w.chunks[0].current.started = past
	w.freezeDue(time.Now(), false)
	if w.frozenCount() != 1 {
		t.Fatalf("frozenCount=%d want 1", w.frozenCount())
	}
	ps := w.frozenPayloads()
	if len(ps) != 1 || string(ps[0].Data) != "abcdefgh" {
		t.Fatalf("payloads=%+v", ps)
	}
}

func TestExtentDiskCacheServesSubrangeOfStagedBlock(t *testing.T) {
	// JuiceFS chunk cache is keyed by block id; ReadAt slices the cached object.
	dir := t.TempDir()
	fs := &Dat9FS{opts: &MountOptions{CacheDir: dir, DiskReadCacheSize: 1 << 20}}
	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = byte('A' + i)
	}
	fs.extentDiskCachePut("blocks/wal", 0, int64(len(payload)), payload)
	got, ok := fs.extentDiskCacheGetBlock("blocks/wal", 0, 4)
	if !ok {
		t.Fatal("expected covering hit at start")
	}
	if string(got) != "ABCD" {
		t.Fatalf("start=%q want ABCD", got)
	}
	got, ok = fs.extentDiskCacheGetBlock("blocks/wal", 16, 4)
	if !ok {
		t.Fatal("expected covering hit at tail")
	}
	if string(got) != "QRST" {
		t.Fatalf("tail=%q want QRST", got)
	}
}

func TestExtentDiskCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs := &Dat9FS{opts: &MountOptions{CacheDir: dir, DiskReadCacheSize: 1 << 20}}
	payload := []byte("hello-block")
	fs.extentDiskCachePut("blocks/a/1", 0, int64(len(payload)), payload)
	got, ok := fs.extentDiskCacheGetBlock("blocks/a/1", 0, int64(len(payload)))
	if !ok {
		t.Fatal("expected disk cache hit")
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q", got)
	}
	raw := filepath.Join(dir, "extent", "raw")
	if _, err := os.Stat(raw); err != nil {
		t.Fatalf("raw dir: %v", err)
	}
}

func TestExtentDiskCacheRejectsBadChecksum(t *testing.T) {
	dir := t.TempDir()
	fs := &Dat9FS{opts: &MountOptions{CacheDir: dir, DiskReadCacheSize: 1 << 20}}
	payload := []byte("hello-block")
	fs.extentDiskCachePut("blocks/a/1", 0, int64(len(payload)), payload)
	c := fs.ensureExtentDiskCache()
	p := c.path(extentDiskCacheKey("blocks/a/1", 0, int64(len(payload))))
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := fs.extentDiskCacheGetBlock("blocks/a/1", 0, int64(len(payload))); ok {
		t.Fatal("corrupt checksum must miss")
	}
}

func TestExtentDiskCacheEvictsUnderCap(t *testing.T) {
	dir := t.TempDir()
	fs := &Dat9FS{opts: &MountOptions{CacheDir: dir, DiskReadCacheSize: 16}}
	a := []byte("aaaaaaaa")
	b := []byte("bbbbbbbb")
	c := []byte("cccccccc")
	fs.extentDiskCachePut("blocks/a", 0, 8, a)
	fs.extentDiskCachePut("blocks/b", 0, 8, b)
	fs.extentDiskCachePut("blocks/c", 0, 8, c)
	hits := 0
	for _, key := range []string{"blocks/a", "blocks/b", "blocks/c"} {
		if _, ok := fs.extentDiskCacheGetBlock(key, 0, 8); ok {
			hits++
		}
	}
	if hits > 2 {
		t.Fatalf("hits=%d want at most 2 under 16-byte cap", hits)
	}
}

func TestHoleOpsForRangeChunkSplit(t *testing.T) {
	ops := holeOpsForRange(client.ExtentChunkSize-4, 8)
	if len(ops) != 2 {
		t.Fatalf("ops=%d want 2", len(ops))
	}
	if ops[0].Kind != "hole" || ops[1].Kind != "hole" {
		t.Fatalf("ops=%+v", ops)
	}
}

func TestExtentCommitDropsFrozenOnly(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("old"))
	got := w.freezeAllPayloads()
	if len(got) != 1 {
		t.Fatalf("frozen payloads=%d", len(got))
	}
	w.writeAt(3, []byte("NEW"))
	w.dropFrozen()
	if w.frozenCount() != 0 {
		t.Fatalf("frozenCount=%d after dropFrozen", w.frozenCount())
	}
	cur := make([]byte, 3)
	w.readAt(3, cur)
	if string(cur) != "NEW" {
		t.Fatalf("current after dropFrozen=%q want NEW", cur)
	}
}

func TestExtentDetachFrozenKeepsLaterSlices(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("old"))
	held := w.detachAll()
	if len(held) != 1 || string(held[0].buf) != "old" {
		t.Fatalf("held=%+v", held)
	}
	if w.empty() {
		t.Fatal("inflight bytes must keep empty() false until dropInflight")
	}
	w.dropInflight(held)
	if !w.empty() {
		t.Fatal("writer must be empty after dropInflight")
	}
	w.inflight = append(w.inflight, held...)
	w.writeAt(3, []byte("NEW"))
	if w.frozenCount() != 0 {
		t.Fatalf("later writes must not reattach detached frozen slices, frozen=%d", w.frozenCount())
	}
	if string(held[0].buf) != "old" {
		t.Fatalf("detached payload mutated: %q", held[0].buf)
	}
	w.restoreFrozen(held)
	got := make([]byte, 6)
	w.readAt(0, got)
	if string(got) != "oldNEW" {
		t.Fatalf("restored overlay=%q want oldNEW", got)
	}
}

func TestExtentInflightReadSeesDetachedBytes(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("abcd"))
	held := w.detachAll()
	got := make([]byte, 4)
	w.readAt(0, got)
	if string(got) != "abcd" {
		t.Fatalf("inflight read=%q want abcd", got)
	}
	w.dropInflight(held)
	got = make([]byte, 4)
	w.readAt(0, got)
	if string(got) != "\x00\x00\x00\x00" {
		t.Fatalf("after drop inflight=%q", got)
	}
}

func TestExtentFileWriterFreezeAllPayloads(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("hello"))
	got := w.freezeAllPayloads()
	if len(got) != 1 || string(got[0].Data) != "hello" {
		t.Fatalf("payloads=%+v", got)
	}
	if w.frozenCount() != 1 {
		t.Fatalf("frozenCount=%d want 1", w.frozenCount())
	}
	if w.hasCurrent() {
		t.Fatal("current must be frozen")
	}
}

func TestWaitExtentInflightIgnoresCanceledContext(t *testing.T) {
	fs := &Dat9FS{}
	w := newExtentFileWriter()
	w.writeAt(0, []byte("abcd"))
	held := w.detachAll()
	fh := &FileHandle{extentWriter: w}
	fh.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- fs.waitExtentInflightLocked(ctx, fh)
	}()
	time.Sleep(30 * time.Millisecond)
	w.dropInflight(held)
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("status=%v, canceled FUSE ctx must not EINTR a running extent PUT", st)
		}
	case <-time.After(time.Second):
		t.Fatal("waitExtentInflight did not return after dropInflight")
	}
}

func TestExtentOpenSharesWriterAcrossHandles(t *testing.T) {
	fs := &Dat9FS{
		extentOpens: make(map[uint64]*extentOpenFile),
		fileHandles: NewHandleTable[*FileHandle](),
		openHandles: NewOpenHandleIndex(),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
	}
	a := &FileHandle{Ino: 7, Path: "/t.db", ContentLayout: client.ContentLayoutExtent, OrigSize: 0}
	b := &FileHandle{Ino: 7, Path: "/t.db", ContentLayout: client.ContentLayoutExtent, OrigSize: 0}
	fs.attachExtentWriter(a)
	fs.attachExtentWriter(b)
	if a.extentWriter == nil || a.extentWriter != b.extentWriter {
		t.Fatal("handles of the same inode must share one writer")
	}
	a.extentWriter.writeAt(0, []byte("abcd"))
	got := make([]byte, 4)
	b.extentWriter.readAt(0, got)
	if string(got) != "abcd" {
		t.Fatalf("sibling read %q", got)
	}
	fs.detachExtentWriter(a)
	if fs.extentOpens[7] == nil {
		t.Fatal("open file must stay while sibling is live")
	}
	fs.detachExtentWriter(b)
	if fs.extentOpens[7] != nil {
		t.Fatal("open file must drop on last handle")
	}
}

func TestExtentFileWriterClipOnlyCurrent(t *testing.T) {
	w := newExtentFileWriter()
	w.writeAt(0, []byte("abcdefghijklmnop"))
	w.clip(4)
	got := make([]byte, 16)
	w.readAt(0, got)
	if string(got[:4]) != "abcd" {
		t.Fatalf("clipped current prefix=%q", got[:4])
	}
	for i := 4; i < 16; i++ {
		if got[i] != 0 {
			t.Fatalf("current tail byte %d=%q after clip", i, got)
		}
	}

	w2 := newExtentFileWriter()
	w2.writeAt(0, []byte("abcdefghijklmnop"))
	held := w2.detachAll()
	w2.clip(4)
	got = make([]byte, 16)
	w2.readAt(0, got)
	if string(got) != "abcdefghijklmnop" {
		t.Fatalf("JuiceFS does not clip frozen/inflight, overlay=%q", got)
	}
	if string(held[0].buf) != "abcdefghijklmnop" {
		t.Fatalf("frozen buf mutated: %q", held[0].buf)
	}
}

func TestExtentProfileMismatchIsNotExtent(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*.db"}}}
	if fs.extentLayoutForCreate("/notes/readme.txt") != client.ContentLayoutSingle {
		t.Fatal("profile-mismatched path must stay single-blob")
	}
	if fs.extentLayoutForCreate("/data/app.db") != client.ContentLayoutExtent {
		t.Fatal("profile glob must select extent")
	}
}

func TestExtentDirtyPunch(t *testing.T) {
	d := &extentDirtySet{}
	d.add(0, 32)
	d.punch(8, 16)
	if len(d.ranges) != 2 || d.ranges[0].end != 8 || d.ranges[1].start != 16 {
		t.Fatalf("ranges=%+v", d.ranges)
	}
}

func newExtentAsyncTestServer(t *testing.T, putGate <-chan struct{}) (string, *atomic.Int32, *atomic.Int32) {
	return newExtentAsyncTestServerGates(t, putGate, nil)
}

func newExtentAsyncTestServerGates(t *testing.T, putGate <-chan struct{}, commitGate <-chan struct{}) (string, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var puts, commits atomic.Int32
	var putURL string
	var mu sync.Mutex
	rev := int64(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			if putGate != nil {
				<-putGate
			}
			puts.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Resource-ID", "inode1")
			mu.Lock()
			w.Header().Set("X-Dat9-Revision", "1")
			mu.Unlock()
			w.Header().Set("Content-Length", "0")
		case r.Method == http.MethodPost && r.URL.RawQuery == "presign-put=1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"put_url": putURL, "headers": map[string]string{}, "block_key": "blocks/inode1/x",
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
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
					"block_key": "blocks/a", "put_url": putURL,
					"headers": map[string]string{},
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			if commitGate != nil {
				<-commitGate
			}
			commits.Add(1)
			mu.Lock()
			rev++
			outRev := rev
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": outRev, "generation": 1, "size_bytes": 4,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	putURL = ts.URL + "/put-block"
	t.Cleanup(ts.Close)
	return ts.URL, &puts, &commits
}

func TestExtentFreezeStartsAsyncUpload(t *testing.T) {
	gate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	url, puts, commits := newExtentAsyncTestServer(t, gate)
	fs := &Dat9FS{
		client:      client.New(url, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		openHandles: NewOpenHandleIndex(),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	fh := &FileHandle{
		Ino: 21, Path: "/async.bin", Dirty: NewWriteBuffer("/async.bin", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent, OrigSize: 0, BaseRev: 1,
	}
	fs.attachExtentWriter(fh)
	started := time.Now()
	fh.extentWriter.writeAt(0, []byte("abcd"))
	fh.extentWriter.freezeAll()
	if time.Since(started) > 200*time.Millisecond {
		t.Fatalf("freezeAll blocked on PUT for %s", time.Since(started))
	}
	if puts.Load() != 0 {
		t.Fatal("PUT must not complete before the gate opens")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if st := fh.extentWriter.flush(ctx); st != gofuse.OK {
		t.Fatalf("flush status=%v with PUT still gated", st)
	}
	if commits.Load() < 1 {
		t.Fatal("expected commit-slices before PUT (JuiceFS writeback Finish)")
	}
	close(gate)
	deadline := time.Now().Add(5 * time.Second)
	for puts.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if puts.Load() < 1 {
		t.Fatal("expected async PUT after writeback commit")
	}
}

func TestExtentCommitOrderDespiteOutOfOrderPut(t *testing.T) {
	var mu sync.Mutex
	var commitOffs []int64
	firstPut := make(chan struct{})
	secondPut := make(chan struct{})
	close(secondPut)
	var putURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			off := r.URL.Query().Get("off")
			if off == "0" {
				<-firstPut
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.RawQuery == "create=1":
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
		case r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutExtent))
			w.Header().Set("X-Dat9-Resource-ID", "inode1")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "0")
		case r.Method == http.MethodPost && r.URL.RawQuery == "presign-put=1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"put_url": putURL, "headers": map[string]string{},
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
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
					"block_key": "blocks/a",
					"put_url":   putURL + "?off=" + itoaOff(rng.FileOff),
					"headers":   map[string]string{},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			var body struct {
				Ops []struct {
					FileOff int64 `json:"file_off"`
				} `json:"ops"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			for _, op := range body.Ops {
				commitOffs = append(commitOffs, op.FileOff)
			}
			n := len(commitOffs)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": n + 1, "generation": 1, "size_bytes": int64(n) * 4,
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
		openHandles: NewOpenHandleIndex(),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	fh := &FileHandle{
		Ino: 22, Path: "/order.bin", Dirty: NewWriteBuffer("/order.bin", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent, BaseRev: 1,
	}
	fs.attachExtentWriter(fh)
	fh.extentWriter.writeAt(0, []byte("aaaa"))
	fh.extentWriter.freezeAll()
	fh.extentWriter.writeAt(4, []byte("bbbb"))
	fh.extentWriter.freezeAll()
	time.Sleep(50 * time.Millisecond)
	close(firstPut)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if st := fh.extentWriter.flush(ctx); st != gofuse.OK {
		t.Fatalf("flush status=%v", st)
	}
	mu.Lock()
	got := append([]int64(nil), commitOffs...)
	mu.Unlock()
	if len(got) < 2 || got[0] != 0 || got[1] != 4 {
		t.Fatalf("commit order %v want [0 4] (JuiceFS commitThread FIFO)", got)
	}
}

func itoaOff(n int64) string {
	if n == 0 {
		return "0"
	}
	if n == 4 {
		return "4"
	}
	return "x"
}

func TestExtentWriteWaitsForFlush(t *testing.T) {
	commitGate := make(chan struct{})
	url, _, _ := newExtentAsyncTestServerGates(t, nil, commitGate)
	fs := &Dat9FS{
		client:      client.New(url, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		openHandles: NewOpenHandleIndex(),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	fh := &FileHandle{
		Ino: 23, Path: "/wait.bin", Dirty: NewWriteBuffer("/wait.bin", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent, BaseRev: 1,
	}
	fs.attachExtentWriter(fh)
	fh.extentWriter.writeAt(0, []byte("abcd"))
	started := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		close(started)
		done <- fh.extentWriter.flush(context.Background())
	}()
	<-started
	waitFlushing := false
	for i := 0; i < 200; i++ {
		fh.extentWriter.mu.Lock()
		n := fh.extentWriter.flushwaiting
		fh.extentWriter.mu.Unlock()
		if n > 0 {
			waitFlushing = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waitFlushing {
		t.Fatal("flush did not set flushwaiting")
	}
	writeDone := make(chan struct{})
	go func() {
		fh.extentWriter.writeAt(4, []byte("efgh"))
		close(writeDone)
	}()
	select {
	case <-writeDone:
		t.Fatal("write must wait while flushwaiting>0 (JuiceFS writecond)")
	case <-time.After(50 * time.Millisecond):
	}
	close(commitGate)
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("flush status=%v", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not finish")
	}
	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not resume after flush")
	}
}

func TestExtentReadFlushWaitsForCommitThenPlans(t *testing.T) {
	// JuiceFS VFS.Read: writer.Flush (commitThread) then reader.Read, no overlay.
	url, _, commits := newExtentAsyncTestServer(t, nil)
	fs := &Dat9FS{
		client:      client.New(url, ""),
		inodes:      NewInodeToPath(),
		extentOpens: make(map[uint64]*extentOpenFile),
		openHandles: NewOpenHandleIndex(),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
		extentCache: newExtentReadCache(1 << 20),
	}
	wb := NewWriteBuffer("/read-flush.bin", 1<<20, 0)
	fh := &FileHandle{
		Ino: 25, Path: "/read-flush.bin", Dirty: wb,
		ContentLayout: client.ContentLayoutExtent, BaseRev: 1,
	}
	fs.attachExtentWriter(fh)
	fh.extentWriter.writeAt(0, []byte("abcd"))
	if _, err := wb.Write(0, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if st := fh.extentWriter.flush(ctx); st != gofuse.OK {
		t.Fatalf("flush status=%v", st)
	}
	if commits.Load() < 1 {
		t.Fatal("JuiceFS VFS.Read Flush must commit-slices before reader.Read")
	}
}

func TestExtentCopyFileRangeFlushesSrcWriter(t *testing.T) {
	// JuiceFS copy_file_range always Flush(src) before cloning slice keys.
	var commits, clones atomic.Int32
	var putURL string
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
		case r.Method == http.MethodPost && r.URL.RawQuery == "prepare-blocks=1":
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
					"block_key": "blocks/a", "put_url": putURL,
					"headers": map[string]string{},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
		case r.Method == http.MethodPost && r.URL.RawQuery == "commit-slices=1":
			commits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 2, "generation": 1, "size_bytes": 4,
			})
		case r.Method == http.MethodPost && r.URL.RawQuery == "clone-range=1":
			clones.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"revision": 3, "generation": 1, "size_bytes": 4,
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
		openHandles: NewOpenHandleIndex(),
		fileHandles: NewHandleTable[*FileHandle](),
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		dirtyInodes: map[uint64]dirtyInodeState{},
	}
	src := &FileHandle{
		Ino: 40, Path: "/src.bin", Dirty: NewWriteBuffer("/src.bin", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent, BaseRev: 1,
	}
	dst := &FileHandle{
		Ino: 41, Path: "/dst.bin", Dirty: NewWriteBuffer("/dst.bin", 1<<20, 0),
		ContentLayout: client.ContentLayoutExtent, BaseRev: 1,
	}
	fs.attachExtentWriter(src)
	fs.attachExtentWriter(dst)
	src.extentWriter.writeAt(0, []byte("abcd"))
	fhIn := fs.fileHandles.Allocate(src)
	fhOut := fs.fileHandles.Allocate(dst)
	_, st := fs.CopyFileRange(nil, &gofuse.CopyFileRangeIn{
		FhIn: fhIn, FhOut: fhOut, OffIn: 0, OffOut: 0, Len: 4,
	})
	if st != gofuse.OK {
		t.Fatalf("status=%v", st)
	}
	if commits.Load() < 1 {
		t.Fatal("JuiceFS copy_file_range must Flush src writer before clone")
	}
	if clones.Load() < 1 {
		t.Fatal("expected clone-range after flush")
	}
}
