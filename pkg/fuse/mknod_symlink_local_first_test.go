package fuse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// localFirstBackend is a minimal fake backend that counts wire calls so
// tests can assert how many round trips an operation makes.
type localFirstBackend struct {
	put     atomic.Int64
	chmod   atomic.Int64
	head    atomic.Int64
	symPost atomic.Int64
	deletes atomic.Int64
	// putGate, when non-nil, blocks PUT handlers before they increment the
	// counter, so tests can assert counter values deterministically.
	putGate chan struct{}
	// chmodFails, when true, makes post-upload chmods answer 500.
	chmodFails bool
	// data records committed content by path when non-nil.
	data map[string][]byte
	// exists/revs track backend objects so HEAD/DELETE behave server-like.
	exists map[string]bool
	revs   map[string]int64
	mu2    sync.Mutex
}

func (b *localFirstBackend) handler() http.HandlerFunc {
	return b.handlerFunc()
}

func (b *localFirstBackend) handlerFunc() http.HandlerFunc {
	b.mu2.Lock()
	if b.exists == nil {
		b.exists = map[string]bool{}
		b.revs = map[string]int64{}
	}
	b.mu2.Unlock()
	return func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPut:
			if b.putGate != nil {
				<-b.putGate
			}
			b.put.Add(1)
			b.mu2.Lock()
			b.exists[p] = true
			b.revs[p]++
			b.mu2.Unlock()
			if b.data != nil {
				body, _ := io.ReadAll(r.Body)
				b.data[p] = body
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(b.revs[p], 10))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodDelete:
			b.deletes.Add(1)
			b.mu2.Lock()
			delete(b.exists, p)
			delete(b.revs, p)
			b.mu2.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && q.Has("chmod"):
			b.chmod.Add(1)
			if b.chmodFails {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodPost && q.Has("symlink"):
			if p == "/exists-link" {
				w.WriteHeader(http.StatusConflict)
				return
			}
			b.symPost.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodHead:
			b.head.Add(1)
			b.mu2.Lock()
			exists, rev := b.exists[p], b.revs[p]
			b.mu2.Unlock()
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"not found"}`))
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

// newLocalFirstTestFS wires a write-back Dat9FS with the full staging stack
// the way mount.go does, so local-first paths engage.
func newLocalFirstTestFS(t *testing.T, ts *httptest.Server) *Dat9FS {
	t.Helper()
	return newLocalFirstTestFSWithPolicy(t, ts, WritePolicyWriteBack)
}

func newLocalFirstTestFSWithPolicy(t *testing.T, ts *httptest.Server, policy WritePolicy) *Dat9FS {
	t.Helper()
	dir := t.TempDir()
	opts := &MountOptions{}
	opts.setDefaults()
	opts.WritePolicy = policy
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	pIdx, err := NewPendingIndex(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("pending index: %v", err)
	}
	if err := pIdx.RecoverFromDisk(); err != nil {
		t.Fatalf("pending index recovery: %v", err)
	}
	fs.pendingIndex = pIdx

	shadow, err := NewShadowStoreWithQuota(filepath.Join(dir, "shadow"), 0.1, 0)
	if err != nil {
		t.Fatalf("shadow store: %v", err)
	}
	fs.shadowStore = shadow
	pIdx.setShadowStore(shadow)

	jr, err := NewJournal(filepath.Join(dir, "journal.wal"))
	if err != nil {
		t.Fatalf("journal: %v", err)
	}
	fs.journal = jr
	pIdx.SetJournal(jr)

	wb, err := NewWriteBackCache(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("write-back cache: %v", err)
	}
	up := NewWriteBackUploader(newTestClient(ts.URL), wb, 2)
	up.OnSuccess = fs.onWriteBackUploadSuccess
	up.SnapshotStagingGens = fs.snapshotStagingGens
	fs.SetWriteBack(wb, up)

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pIdx, jr, 2, maxCommitQueuePending)
	cq.RecordCommittedRevision = fs.recordCommittedRevision
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnUploaded = fs.onCommitQueueUploaded
	cq.OnCleanup = fs.onCommitQueueCleanup
	cq.OnDiscard = fs.onCommitQueueDiscard
	cq.IsSuperseded = fs.commitEntrySuperseded
	cq.PathLock = fs.lockRemoteCommitPath
	cq.DurableWatermark = fs.latestCommittedRevision
	fs.commitQueue = cq

	t.Cleanup(func() {
		cq.DrainAll()
		up.DrainAll()
		_ = jr.Close()
	})
	return fs
}

// TestMknodRegularLocalFirstNoSyncRoundTrips verifies that mknod of a regular
// file on a staging-capable write-back mount answers from local state with
// zero synchronous backend round trips, and that the empty image + mode are
// committed asynchronously by the commit queue.
func TestMknodRegularLocalFirstNoSyncRoundTrips(t *testing.T) {
	backend := &localFirstBackend{}
	// Gate PUTs before the counter increments so a dispatched commit worker
	// cannot move the counters during the pre-drain assertions. The chmod
	// only fires after the PUT completes, so it is gated transitively.
	putGate := make(chan struct{})
	backend.putGate = putGate
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(putGate) }) }
	// Release before the FS cleanup (registered later, so it runs first):
	// a failed assertion must not leave DrainAll blocked on a gated worker.
	t.Cleanup(release)

	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newLocalFirstTestFS(t, ts)

	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "created-by-mknod.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	if got := backend.put.Load() + backend.chmod.Load() + backend.head.Load(); got != 0 {
		t.Fatalf("mknod issued %d synchronous backend calls, want 0", got)
	}
	if got := out.Mode & uint32(syscall.S_IFMT); got != uint32(syscall.S_IFREG) {
		t.Fatalf("mknod mode type = %o, want regular", got)
	}
	if got := out.Mode & 0o777; got != 0o600 {
		t.Fatalf("mknod mode = %o, want 0600", got)
	}

	// The staged entry must be visible as pending metadata before the
	// background commit lands.
	if _, ok := fs.pendingIndex.GetMeta("/created-by-mknod.txt"); !ok {
		t.Fatal("mknod did not stage pending index meta")
	}

	// Release the gate and let the commit queue drain the empty image
	// (direct PUT) and the mode.
	release()
	fs.commitQueue.DrainAll()
	if got := backend.put.Load(); got != 1 {
		t.Fatalf("async create PUTs = %d, want 1", got)
	}
	if got := backend.chmod.Load(); got != 1 {
		t.Fatalf("async chmod calls = %d, want 1", got)
	}

	// Staging is cleaned up after a successful commit.
	if _, ok := fs.pendingIndex.GetMeta("/created-by-mknod.txt"); ok {
		t.Fatal("pending meta survived commit")
	}
}

// TestMknodRegularLocalFirstEEXISTOnPendingState verifies mknod refuses to
// clobber locally staged state.
func TestMknodRegularLocalFirstEEXISTOnPendingState(t *testing.T) {
	backend := &localFirstBackend{}
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newLocalFirstTestFS(t, ts)

	if err := fs.writeBack.Put("/staged.txt", []byte("staged"), 6, PendingNew); err != nil {
		t.Fatalf("writeBack.Put: %v", err)
	}

	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "staged.txt", &out)
	if st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("Mknod status = %v, want EEXIST", st)
	}
}

// TestMknodRegularSyncFallbackWithoutStaging verifies mounts without staging
// infrastructure keep the legacy synchronous create.
func TestMknodRegularSyncFallbackWithoutStaging(t *testing.T) {
	var puts, chmods, heads atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			puts.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmods.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.Method == http.MethodHead:
			heads.Add(1)
			w.Header().Set("X-Dat9-Revision", "3")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "sync-mknod.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("create PUTs = %d, want 1", got)
	}
	if got := chmods.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if got := heads.Load(); got != 1 {
		t.Fatalf("stat calls = %d, want 1", got)
	}
}

// TestSymlinkSkipsPostCreateStat verifies symlink(2) issues exactly one
// backend round trip (the POST) and still returns a complete entry.
func TestSymlinkSkipsPostCreateStat(t *testing.T) {
	backend := &localFirstBackend{}
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newLocalFirstTestFS(t, ts)

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "/some/target", "created-link", &out)
	if st != gofuse.OK {
		t.Fatalf("Symlink status = %v, want OK", st)
	}
	if got := backend.symPost.Load(); got != 1 {
		t.Fatalf("symlink POSTs = %d, want 1", got)
	}
	if got := backend.head.Load(); got != 0 {
		t.Fatalf("post-create stats = %d, want 0", got)
	}
	if got := out.Mode & uint32(syscall.S_IFMT); got != uint32(syscall.S_IFLNK) {
		t.Fatalf("symlink mode type = %o, want symlink", got)
	}
	if got := out.Size; got != uint64(len("/some/target")) {
		t.Fatalf("symlink size = %d, want %d", got, len("/some/target"))
	}
}

// TestMknodLocalFirstOTruncOpenCommitsWrittenBytes covers open(O_TRUNC)
// racing the still-queued empty-file commit of a local-first mknod: Open must
// drain the queued commit first (so the inode carries the committed
// revision), and the bytes written through the handle must be committed as a
// well-based overwrite instead of being rejected for a missing base
// revision.
func TestMknodLocalFirstOTruncOpenCommitsWrittenBytes(t *testing.T) {
	backend := &localFirstBackend{data: map[string][]byte{}}
	putGate := make(chan struct{})
	backend.putGate = putGate
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(putGate) }) }
	t.Cleanup(release)

	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newLocalFirstTestFS(t, ts)

	var out gofuse.EntryOut
	if st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "trunced.txt", &out); st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}

	// Let the queued empty-file commit run, then open with O_TRUNC. The
	// handle's flush must commit the written bytes.
	release()
	openOut := gofuse.OpenOut{}
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: out.NodeId},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	written := []byte("truncation survives")
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: out.NodeId},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(written)),
	}, written); st != gofuse.OK {
		t.Fatalf("Write status = %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: out.NodeId},
		Fh:       openOut.Fh,
	})
	fs.commitQueue.DrainAll()

	if got, ok := backend.data["/trunced.txt"]; !ok {
		t.Fatal("written bytes were never committed to the backend")
	} else if string(got) != string(written) {
		t.Fatalf("committed bytes = %q, want %q", got, written)
	}
}

// TestMknodRegularSyncFallbackForCloseSync verifies close-sync mounts keep
// the synchronous remote create even though their staging components are
// wired: mknod must PUT (and chmod) before returning.
func TestMknodRegularSyncFallbackForCloseSync(t *testing.T) {
	backend := &localFirstBackend{}
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newLocalFirstTestFSWithPolicy(t, ts, WritePolicyCloseSync)

	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "sync-close-sync.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	if got := backend.put.Load(); got != 1 {
		t.Fatalf("sync PUTs = %d, want 1 before returning", got)
	}
	if got := backend.chmod.Load(); got != 1 {
		t.Fatalf("sync chmod calls = %d, want 1", got)
	}
	if _, ok := fs.pendingIndex.GetMeta("/sync-close-sync.txt"); ok {
		t.Fatal("close-sync mknod staged pending meta; want synchronous create")
	}
}

// TestMknodLocalFirstChmodFailureStillUnlinks covers the staged-failure path
// qiffang flagged on #997: the queue lands the zero-byte PUT, then the
// post-upload chmod fails. The durable pending state must be re-classified
// (PendingChmod) and the committed revision recorded, so a follow-up unlink
// still issues the remote DELETE instead of classifying the path as
// never-uploaded and leaking the just-created remote object.
func TestMknodLocalFirstChmodFailureStillUnlinks(t *testing.T) {
	backend := &localFirstBackend{chmodFails: true}
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newLocalFirstTestFS(t, ts)

	var out gofuse.EntryOut
	st := fs.Mknod(nil, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "chmod-fail.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	fs.commitQueue.DrainAll()

	if got := backend.put.Load(); got != 1 {
		t.Fatalf("create PUTs = %d, want 1", got)
	}
	if got := backend.chmod.Load(); got == 0 {
		t.Fatal("post-upload chmod was never attempted")
	}
	if !backend.exists["/chmod-fail.txt"] {
		t.Fatal("remote object missing after PUT (data must have landed)")
	}
	// The durable pending state must no longer claim "never uploaded".
	meta, ok := fs.pendingIndex.GetMeta("/chmod-fail.txt")
	if !ok {
		t.Fatal("pending meta missing after chmod failure")
	}
	if meta.Kind != PendingChmod {
		t.Fatalf("pending kind = %v, want PendingChmod", meta.Kind)
	}
	if rev := fs.latestCommittedRevision("/chmod-fail.txt"); rev <= 0 {
		t.Fatalf("committed revision tracker = %d, want > 0", rev)
	}

	// Unlink must still remove the landed remote object.
	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "chmod-fail.txt"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	if got := backend.deletes.Load(); got != 1 {
		t.Fatalf("DELETEs = %d, want 1 (remote object must not leak)", got)
	}
	if _, exists := backend.exists["/chmod-fail.txt"]; exists {
		t.Fatal("remote object leaked after unlink")
	}
}
