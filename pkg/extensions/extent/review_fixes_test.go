package extent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Finding #3: Splitting owner A's lock must never overwrite owner B's slot.
func TestLockSplitPreservesSubsequentOtherOwnerRanges(t *testing.T) {
	lt := newLockTable()
	ctx := context.Background()
	const ino = meta.Ino(101)

	if errno := lt.Setlk(ctx, ino, 1, 10, syscall.F_WRLCK, 0, 30, false); errno != 0 {
		t.Fatalf("owner 1 lock [0,30]: %v", errno)
	}
	if errno := lt.Setlk(ctx, ino, 2, 20, syscall.F_WRLCK, 40, 50, false); errno != 0 {
		t.Fatalf("owner 2 lock [40,50]: %v", errno)
	}
	// Owner 1 unlocks the middle [10,20], splitting its range into [0,9] and [21,30].
	if errno := lt.Setlk(ctx, ino, 1, 10, syscall.F_UNLCK, 10, 20, false); errno != 0 {
		t.Fatalf("owner 1 unlock [10,20]: %v", errno)
	}
	// Owner 3 must still be blocked on owner 2's [40,50].
	if errno := lt.Setlk(ctx, ino, 3, 30, syscall.F_WRLCK, 40, 50, false); errno != syscall.EAGAIN {
		t.Fatalf("owner 3 lock [40,50] errno = %v, want EAGAIN (owner 2 still holds it)", errno)
	}
}

// Findings #9 & #10: [0,0] is a 1-byte lock (not EOF), EOF round-trips as
// OFFSET_MAX, and engineHandle.SetLk honors request cancellation.
func TestEngineHandleByteZeroLockAndCancellation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/lk.bin", "0123456789")
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	eh1 := claimOpen(t, h, "/lk.bin")
	eh2 := claimOpen(t, h, "/lk.bin")
	t.Cleanup(func() {
		_ = eh1.Release(ctx)
		_ = eh2.Release(ctx)
	})

	// Lock single byte [0, 0] on owner 1; owner 2 locking [1, 1] must succeed.
	if err := eh1.SetLk(ctx, fs.FileLock{Owner: 1, Pid: 11, Type: syscall.F_WRLCK, Start: 0, End: 0}, false); err != nil {
		t.Fatalf("lock [0,0]: %v", err)
	}
	if err := eh2.SetLk(ctx, fs.FileLock{Owner: 2, Pid: 22, Type: syscall.F_WRLCK, Start: 1, End: 1}, false); err != nil {
		t.Fatalf("lock [1,1] should not conflict with [0,0]: %v", err)
	}
	// Locking to EOF on owner 1 from byte 2 onward reports OFFSET_MAX on GetLk.
	const fuseEOF = int64(lockVFSUnlockEOF)
	if err := eh1.SetLk(ctx, fs.FileLock{Owner: 1, Pid: 11, Type: syscall.F_WRLCK, Start: 2, End: fuseEOF}, false); err != nil {
		t.Fatalf("lock [2,EOF]: %v", err)
	}
	got, err := eh2.GetLk(ctx, fs.FileLock{Owner: 2, Pid: 22, Type: syscall.F_WRLCK, Start: 5, End: 8})
	if err != nil {
		t.Fatalf("GetLk: %v", err)
	}
	if got.Type != syscall.F_WRLCK || got.Start != 2 || got.End != fuseEOF {
		t.Fatalf("GetLk = %+v, want WRLCK [2, %d]", got, fuseEOF)
	}

	// Blocking SetLk on contended byte 0 cancels promptly with EINTR when ctx cancels.
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- eh2.SetLk(cctx, fs.FileLock{Owner: 2, Pid: 22, Type: syscall.F_WRLCK, Start: 0, End: 0}, true)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case lerr := <-done:
		if lerr != syscall.EINTR {
			t.Fatalf("canceled blocking SetLk err = %v, want EINTR", lerr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking SetLk did not wake on context cancellation")
	}
}

// Findings #4 & #8: CopyFileRange copies over op.Handle/op.Handle2 even when
// kernel Fh numbers differ from JuiceFS internal fh numbers.
func TestCopyFileRangeAndFallocateUseKernelHandleIdentity(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/src.bin", "HELLO-EXTENT-WORLD")
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	srcRes, err := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/d/src.bin", Flags: syscall.O_RDONLY}, nil)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	dstRes, err := h.Do(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/d/dst.bin",
		Flags: syscall.O_CREAT | syscall.O_TRUNC | syscall.O_RDWR, Mode: 0o644, HasMode: true,
		Kernel: &gofuse.CreateIn{},
	}, nil)
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}
	t.Cleanup(func() {
		_ = srcRes.Handle.Release(ctx)
		_ = dstRes.Handle.Release(ctx)
	})

	// Deliberately supply mismatched kernel Fh values (9001, 9002) while passing
	// the real handles via Handle/Handle2, as pkg/fuse/fuse.go does.
	cfrRes, err := h.Do(ctx, &fs.Op{
		Kind:    fs.OpCopyFileRange,
		Path:    "/d/src.bin",
		Fh:      9001,
		Handle:  srcRes.Handle,
		Offset:  6,
		Path2:   "/d/dst.bin",
		Fh2:     9002,
		Handle2: dstRes.Handle,
		Offset2: 0,
		Limit:   6,
	}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		t.Fatal("copy_file_range fell through instead of matching Handle/Handle2")
		return fs.Result{}, nil
	})
	if err != nil || cfrRes.Written != 6 {
		t.Fatalf("CopyFileRange = (%+v, %v), want Written=6", cfrRes, err)
	}
	if err := dstRes.Handle.Sync(ctx); err != nil {
		t.Fatalf("dst sync: %v", err)
	}
	if got := f.contentOf("/d/dst.bin"); got != "EXTENT" {
		t.Fatalf("dst content = %q, want EXTENT", got)
	}

	// Bridge.CopyFileRange itself returns ENOTSUP rather than 0/EOF.
	var copied, outLen uint64
	if errno := h.opts.Engine.Bridge.CopyFileRange(meta.Background(), 1, 0, 2, 0, 16, 0, &copied, &outLen); errno != syscall.ENOTSUP {
		t.Fatalf("Bridge.CopyFileRange = %v, want ENOTSUP", errno)
	}
}

// Finding #5: 404/416 from a lagging replica over acknowledged published bytes
// must fail closed with EIO instead of zero-filling.
func TestReadFailsClosedOn404Or416OverPublishedBytes(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusRequestedRangeNotSatisfiable)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"lagging"}`, int(code.Load()))
	}))
	defer srv.Close()

	br := NewBridge(Config{Client: client.New(srv.URL, ""), RemoteRoot: "/"})
	ino := inoFor("/ack.bin")
	br.remember(ino, "/ack.bin")
	br.recordLandedRevisionWithSize(ino, 4, 8192)

	var slices []meta.Slice
	if errno := br.Read(meta.Background(), ino, 0, &slices); errno != syscall.EIO {
		t.Fatalf("Read on 416 over published 8KiB = %v, want EIO", errno)
	}
	code.Store(http.StatusNotFound)
	if errno := br.Read(meta.Background(), ino, 0, &slices); errno != syscall.EIO {
		t.Fatalf("Read on 404 over published 8KiB = %v, want EIO", errno)
	}
}

// Finding #7: Fallocate rejects ZERO_RANGE, preserves length on interior /
// KEEP_SIZE calls, and publishes size extensions on fsync.
func TestFallocateZeroRangeKeepSizeAndExtendPublish(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/fa.bin", "ABCDEFGH")
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/d/fa.bin", Flags: syscall.O_RDWR}, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = res.Handle.Release(ctx) })

	// ZERO_RANGE (0x10) must return ENOTSUP instead of claiming bytes were zeroed.
	_, zerr := h.Do(ctx, &fs.Op{
		Kind: fs.OpFallocate, Path: "/d/fa.bin", Handle: res.Handle, Flags: 0x10, Offset: 0, Size: 4,
	}, nil)
	if zerr != syscall.ENOTSUP {
		t.Fatalf("ZERO_RANGE err = %v, want ENOTSUP", zerr)
	}

	// Interior allocation [0, 4) inside an 8-byte file must keep length 8.
	if _, ierr := h.Do(ctx, &fs.Op{
		Kind: fs.OpFallocate, Path: "/d/fa.bin", Handle: res.Handle, Flags: 0, Offset: 0, Size: 4,
	}, nil); ierr != nil {
		t.Fatalf("interior fallocate: %v", ierr)
	}
	if sz, ok := h.opts.Engine.LocalAttr(ctx, "/d/fa.bin"); !ok || sz != 8 {
		t.Fatalf("length after interior fallocate = %d (ok=%v), want 8", sz, ok)
	}

	// Extending allocation [0, 4096) followed by Sync must publish 4096 remotely.
	if _, eerr := h.Do(ctx, &fs.Op{
		Kind: fs.OpFallocate, Path: "/d/fa.bin", Handle: res.Handle, Flags: 0, Offset: 0, Size: 4096,
	}, nil); eerr != nil {
		t.Fatalf("extend fallocate: %v", eerr)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatalf("sync after extend fallocate: %v", err)
	}
	if got := len(f.contentOf("/d/fa.bin")); got != 4096 {
		t.Fatalf("remote length after fallocate+fsync = %d, want 4096", got)
	}
}

// Finding #12: Non-truncating kernel CREATE preserves existing extent contents
// and declines existing classic files.
func TestNonTruncatingKernelCreatePreservesExistingContentAndLayout(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/ext.db", "EXISTING-EXTENT")
	f.mu.Lock()
	f.classic["/d/classic.txt"] = "EXISTING-CLASSIC"
	f.ids["/d/classic.txt"] = "res-c1"
	f.mu.Unlock()

	h := newTestHandler(t, f, 0) // full extent profile
	ctx := context.Background()

	// Foreign existing classic file opened via kernel CreateIn without O_TRUNC must decline.
	var classicPassed bool
	_, err := h.Do(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/d/classic.txt",
		Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0o644, HasMode: true,
		Kernel: &gofuse.CreateIn{},
	}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		classicPassed = true
		return fs.Result{Handle: &stubCore{}}, nil
	})
	if err != nil || !classicPassed {
		t.Fatalf("existing classic file should pass through to classic plane: passed=%v err=%v", classicPassed, err)
	}

	// Foreign existing extent file opened via kernel CreateIn without O_TRUNC must read its bytes.
	extRes, err := h.Do(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/d/ext.db",
		Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0o644, HasMode: true,
		Kernel: &gofuse.CreateIn{},
	}, nil)
	if err != nil {
		t.Fatalf("open existing extent via kernel create: %v", err)
	}
	defer func() { _ = extRes.Handle.Release(ctx) }()
	if extRes.Attr == nil || extRes.Attr.Size != int64(len("EXISTING-EXTENT")) {
		t.Fatalf("returned attr = %+v, want Size=%d", extRes.Attr, len("EXISTING-EXTENT"))
	}
	if got := readFront(t, extRes.Handle.(*engineHandle), len("EXISTING-EXTENT")); got != "EXISTING-EXTENT" {
		t.Fatalf("first read = %q, want EXISTING-EXTENT", got)
	}
}

// Finding #16: NewBridge sweeps leftover staging files on startup.
func TestNewBridgeSweepsOrphanSpoolFiles(t *testing.T) {
	dir := t.TempDir()
	orphan := filepath.Join(dir, "extent-stage-99-0-deadbeef")
	keep := filepath.Join(dir, "normal-cache-entry")
	if err := os.WriteFile(orphan, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = NewBridge(Config{CacheDir: dir, RemoteRoot: "/"})
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan spool file survived startup sweep: err=%v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("unrelated cache file removed: %v", err)
	}
}

// Finding #18: Failed commit drains automatically once server recovers.
func TestFailedCommitDrainsViaBackgroundRetry(t *testing.T) {
	var failPatch atomic.Bool
	failPatch.Store(true)

	f := newFakeV2(t)
	f.seedExtent(t, "/retry.bin", "INIT")
	origHandler := f.Config.Handler
	f.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch && failPatch.Load() {
			http.Error(w, "temporary outage", http.StatusServiceUnavailable)
			return
		}
		origHandler.ServeHTTP(w, r)
	})

	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	t.Cleanup(func() { _ = eng.Bridge.Shutdown() })
	ino := inoFor("/retry.bin")
	eng.Bridge.remember(ino, "/retry.bin")
	eng.Bridge.mu.Lock()
	eng.Bridge.sizes[ino] = 4
	eng.Bridge.mu.Unlock()

	// Stage a slice directly through objectStorage.Put + Bridge.Write while batching is bypassed.
	prevBatch := commitBatchBytes
	commitBatchBytes = 0
	t.Cleanup(func() { commitBatchBytes = prevBatch })

	var sid uint64
	_ = eng.Bridge.NewSlice(meta.Background(), &sid)
	st := eng.Bridge.Storage()
	payload := []byte("ABCD")
	if err := st.Put(context.Background(), sliceKeyForTest(sid, 0, len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if errno := eng.Bridge.Write(meta.Background(), ino, 0, 0, meta.Slice{Id: sid, Size: 4, Len: 4}, time.Now()); errno != 0 {
		t.Fatalf("Bridge.Write: %v", errno)
	}
	if !eng.Bridge.HasPending(ino) {
		t.Fatal("expected pending slice retained during server outage")
	}

	// Recover server; kickPendingFlush's background worker drains without new writes.
	failPatch.Store(false)
	deadline := time.Now().Add(3 * time.Second)
	for eng.Bridge.HasPending(ino) && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if eng.Bridge.HasPending(ino) {
		t.Fatal("background retry did not drain pending slice after server recovery")
	}
	if got := f.contentOf("/retry.bin"); got != "ABCD" {
		t.Fatalf("remote content after background recovery = %q, want ABCD", got)
	}
}

func sliceKeyForTest(id uint64, indx int, size int) string {
	return "chunks/0/0/" + strconv.FormatUint(id, 10) + "_" + strconv.Itoa(indx) + "_" + strconv.Itoa(size)
}

// Finding #20: Batched repeated overwrites at offset 0 on an existing extent
// file compose into a single non-duplicate PATCH block with the newest bytes.
func TestBatchedRepeatedOverwritesComposeDisjointPatchBlocks(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/ow.bin", strings.Repeat("Z", 8192))
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	eh := claimOpen(t, h, "/ow.bin")
	defer func() { _ = eh.Release(ctx) }()

	// Two separate frozen writes at offset 0 (flush writer into bridge via Truncate or second handle).
	first := bytes.Repeat([]byte("A"), 4096)
	second := bytes.Repeat([]byte("B"), 2048)

	var sid1, sid2 uint64
	br := h.opts.Engine.Bridge
	_ = br.NewSlice(meta.Background(), &sid1)
	_ = br.NewSlice(meta.Background(), &sid2)
	if err := br.Storage().Put(ctx, sliceKeyForTest(sid1, 0, len(first)), bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	if err := br.Storage().Put(ctx, sliceKeyForTest(sid2, 0, len(second)), bytes.NewReader(second)); err != nil {
		t.Fatal(err)
	}
	if errno := br.Write(meta.Background(), eh.ino, 0, 0, meta.Slice{Id: sid1, Size: uint32(len(first)), Len: uint32(len(first))}, time.Now()); errno != 0 {
		t.Fatal(errno)
	}
	if errno := br.Write(meta.Background(), eh.ino, 0, 0, meta.Slice{Id: sid2, Size: uint32(len(second)), Len: uint32(len(second))}, time.Now()); errno != 0 {
		t.Fatal(errno)
	}

	blocks, err := br.assemblePatchBlocks(ctx, "/ow.bin", br.pending[eh.ino])
	if err != nil {
		t.Fatalf("assemblePatchBlocks: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("assembled %d blocks, want 2 disjoint spans ([0,2048) B + [2048,4096) A)", len(blocks))
	}
	if blocks[0].Off != 0 || !bytes.Equal(blocks[0].Data, second) {
		t.Fatalf("block 0 off=%d len=%d, want [0,2048) of B", blocks[0].Off, len(blocks[0].Data))
	}
	if blocks[1].Off != 2048 || !bytes.Equal(blocks[1].Data, first[2048:]) {
		t.Fatalf("block 1 off=%d len=%d, want [2048,4096) of A", blocks[1].Off, len(blocks[1].Data))
	}

	if err := eh.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	want := string(second) + string(first[2048:]) + strings.Repeat("Z", 4096)
	if got := f.contentOf("/ow.bin"); got != want {
		t.Fatalf("remote content mismatch: len=%d want=%d", len(got), len(want))
	}
}

// Finding #22: Truncate-only growth on an unbound local descriptor must NOT
// overwrite a foreign empty classic occupant.
func TestTruncateOnlyRefusesUnattributedForeignEmptyOccupant(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err := h.Do(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/d/race.bin",
		Flags: syscall.O_CREAT | syscall.O_TRUNC | syscall.O_RDWR, Mode: 0o644, HasMode: true,
		Kernel: &gofuse.CreateIn{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Handle.Release(ctx) }()

	// Competing client creates a 0-byte classic file at /d/race.bin.
	f.mu.Lock()
	f.classic["/d/race.bin"] = ""
	f.ids["/d/race.bin"] = "foreign-res-99"
	f.mu.Unlock()

	if _, terr := h.Do(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/d/race.bin", HasSize: true, Size: 4096}, nil); terr != nil {
		t.Fatalf("local truncate: %v", terr)
	}
	if serr := res.Handle.Sync(ctx); serr == nil {
		t.Fatal("Sync succeeded over foreign 0-byte classic occupant; must refuse")
	}
	f.mu.Lock()
	foreignSize := len(f.classic["/d/race.bin"])
	foreignID := f.ids["/d/race.bin"]
	f.mu.Unlock()
	if foreignSize != 0 || foreignID != "foreign-res-99" {
		t.Fatalf("foreign occupant mutated: size=%d id=%q", foreignSize, foreignID)
	}
}

// Finding #24: Fresh file write(12 KiB) -> truncate(5 KiB) -> truncate(9 KiB)
// -> Sync must zero [5 KiB, 9 KiB) on first publication.
func TestFreshFirstPublicationHonorsClippedPendingWriteSize(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err := h.Do(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/d/clip.bin",
		Flags: syscall.O_CREAT | syscall.O_TRUNC | syscall.O_RDWR, Mode: 0o644, HasMode: true,
		Kernel: &gofuse.CreateIn{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Handle.Release(ctx) }()

	orig := bytes.Repeat([]byte("X"), 12*1024)
	if err := res.Handle.Write(ctx, 0, orig); err != nil {
		t.Fatalf("write 12KiB: %v", err)
	}
	if _, err := h.Do(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/d/clip.bin", HasSize: true, Size: 5 * 1024}, nil); err != nil {
		t.Fatalf("truncate 5KiB: %v", err)
	}
	if _, err := h.Do(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/d/clip.bin", HasSize: true, Size: 9 * 1024}, nil); err != nil {
		t.Fatalf("truncate 9KiB: %v", err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}

	got := []byte(f.contentOf("/d/clip.bin"))
	want := append(bytes.Repeat([]byte("X"), 5*1024), make([]byte, 4*1024)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("published image len=%d; tail [5KiB,9KiB) non-zero=%v", len(got), !bytes.Equal(got[5*1024:], make([]byte, 4*1024)))
	}
}

// Finding #25: An older in-flight commit completing after a newer truncate
// must not clear b.truncs[ino].
func TestInFlightCommitDoesNotClearNewerTruncateObligation(t *testing.T) {
	patchEntered := make(chan struct{})
	releasePatch := make(chan struct{})
	var blockOnce sync.Once

	f := newFakeV2(t)
	f.seedExtent(t, "/race-trunc.bin", strings.Repeat("M", 12*1024))
	origHandler := f.Config.Handler
	f.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			var body struct {
				NewSize int64 `json:"new_size"`
			}
			buf, _ := os.ReadFile("/dev/null")
			_ = buf
			raw := new(bytes.Buffer)
			_, _ = raw.ReadFrom(r.Body)
			_ = json.Unmarshal(raw.Bytes(), &body)
			r.Body = http.NoBody
			// Re-attach body for fakeV2 handler.
			r2 := r.Clone(r.Context())
			r2.Body = ioNopCloser(raw.Bytes())
			if body.NewSize == 12*1024 {
				blockOnce.Do(func() {
					close(patchEntered)
					<-releasePatch
				})
			}
			origHandler.ServeHTTP(w, r2)
			return
		}
		origHandler.ServeHTTP(w, r)
	})

	h := newTestHandler(t, f, 0)
	ctx := context.Background()
	ehA := claimOpen(t, h, "/race-trunc.bin")
	ehB := claimOpen(t, h, "/race-trunc.bin")
	defer func() {
		_ = ehA.Release(ctx)
		_ = ehB.Release(ctx)
	}()

	if err := ehA.Write(ctx, 0, bytes.Repeat([]byte("P"), 1024)); err != nil {
		t.Fatal(err)
	}
	syncADone := make(chan error, 1)
	go func() {
		syncADone <- ehA.Sync(ctx)
	}()

	<-patchEntered
	// Simulate a truncate arriving while A's 12 KiB PATCH is in flight.
	h.opts.Engine.Bridge.recordTruncate(ehB.ino, 5*1024)
	close(releasePatch)

	if err := <-syncADone; err != nil {
		t.Fatalf("ehA.Sync: %v", err)
	}
	// ehB.Sync must still observe and publish the 5 KiB truncate!
	if err := ehB.Sync(ctx); err != nil {
		t.Fatalf("ehB.Sync: %v", err)
	}
	if got := len(f.contentOf("/race-trunc.bin")); got != 5*1024 {
		t.Fatalf("remote length after concurrent truncate+fsync = %d, want %d", got, 5*1024)
	}
}

type bytesReadCloser struct{ *bytes.Reader }

func (bytesReadCloser) Close() error { return nil }

func ioNopCloser(b []byte) bytesReadCloser {
	return bytesReadCloser{bytes.NewReader(b)}
}
