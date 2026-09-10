package fuse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// zombieTestServer records PUT bodies per path and serves the revision
// header the conditional-write client expects.
type zombieTestServer struct {
	mu     sync.Mutex
	bodies map[string][][]byte
	rev    int64
}

func (s *zombieTestServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
			body, _ := io.ReadAll(r.Body)
			s.mu.Lock()
			p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			s.bodies[p] = append(s.bodies[p], body)
			s.rev++
			rev := s.rev
			s.mu.Unlock()
			w.Header().Set("X-Dat9-Revision", itoa(rev))
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})
}

func (s *zombieTestServer) bodiesFor(path string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.bodies[path]...)
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}

// newZombieTestFS builds a Dat9FS with the kernel-writeback-cache gate forced
// on (the zombie lifecycle is gated on it; the kernel cap itself is
// Linux-only).
func newZombieTestFS(t *testing.T, opts *MountOptions) (*Dat9FS, *zombieTestServer) {
	t.Helper()
	srv := &zombieTestServer{bodies: make(map[string][][]byte)}
	ts := httptest.NewServer(srv.handler())
	if opts == nil {
		opts = &MountOptions{}
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.kernelWritebackCache = true
	t.Cleanup(func() {
		fs.stopZombieJanitor()
		ts.Close()
	})
	return fs, srv
}

// newZombieWritableHandle registers a writable classic handle for path with a
// dirty buffer, mimicking the state Open/Create leave behind.
func newZombieWritableHandle(fs *Dat9FS, path string, origSize, baseRev int64) (*FileHandle, uint64) {
	ino := fs.inodes.Lookup(path, false, origSize, time.Now())
	fh := &FileHandle{
		Ino:      ino,
		Path:     path,
		Flags:    uint32(syscall.O_RDWR),
		Dirty:    fs.newWriteBuffer(path, 0, 0),
		OrigSize: origSize,
		BaseRev:  baseRev,
	}
	fhID := fs.allocateFileHandle(fh)
	return fh, fhID
}

func TestZombieReleaseKeepsHandleUntilForget(t *testing.T) {
	fs, srv := newZombieTestFS(t, nil)
	fh, fhID := newZombieWritableHandle(fs, "/z.txt", 0, 0)
	fh.IsNew = true

	// Write "hello" through the live handle and release it: the release
	// commit PUTs the content synchronously (no write-back cache configured
	// in this harness).
	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
		Offset:   0,
	}, []byte("hello")); st != gofuse.OK || n != 5 {
		t.Fatalf("Write = n=%d st=%v, want 5/OK", n, st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	})

	if !fh.Zombie {
		t.Fatal("handle was not zombified at Release with the kernel writeback cache on")
	}
	if _, ok := fs.fileHandles.Get(fhID); !ok {
		t.Fatal("zombie handle missing from fileHandles after Release")
	}
	if got := fs.zombieHandleForNode(fh.Ino); got != fh {
		t.Fatalf("zombieHandleForNode = %p, want the released handle", got)
	}
	if puts := srv.bodiesFor("/z.txt"); len(puts) != 1 || string(puts[0]) != "hello" {
		t.Fatalf("release commit PUTs = %q, want exactly [hello]", puts)
	}

	// Late kernel writeback after Release, addressed by the released fh.
	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: fh.Ino},
		Fh:         fhID,
		Offset:     5,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("late!")); st != gofuse.OK || n != 5 {
		t.Fatalf("zombie Write = n=%d st=%v, want 5/OK", n, st)
	}
	fh.Lock()
	timerArmed := fh.zombieTimer != nil
	dirtySize := fh.Dirty.Size()
	fh.Unlock()
	if !timerArmed {
		t.Fatal("zombie write did not schedule the background commit")
	}
	if dirtySize != 10 {
		t.Fatalf("zombie dirty size = %d, want 10", dirtySize)
	}

	// The debounced background commit uploads the full content.
	deadline := time.Now().Add(5 * time.Second)
	for {
		puts := srv.bodiesFor("/z.txt")
		if len(puts) >= 2 && string(puts[len(puts)-1]) == "hellolate!" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("zombie commit never landed; PUTs = %q", puts)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// FORGET retires the zombie.
	fs.Forget(fh.Ino, 1)
	if _, ok := fs.fileHandles.Get(fhID); ok {
		t.Fatal("zombie handle survived Forget")
	}
	if fs.zombieHandleForNode(fh.Ino) != nil {
		t.Fatal("zombie still indexed after Forget")
	}
}

func TestZombieLateWritebackResolvedByNode(t *testing.T) {
	fs, srv := newZombieTestFS(t, nil)
	fh, fhID := newZombieWritableHandle(fs, "/z2.txt", 0, 0)
	fh.IsNew = true
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	}, []byte("aa")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID})
	if !fh.Zombie {
		t.Fatal("handle was not zombified")
	}

	// Late writeback with an fh the daemon never saw (kernel may drop the
	// released fh): resolved through the inode.
	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: fh.Ino},
		Fh:         0,
		Offset:     2,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("bb")); st != gofuse.OK || n != 2 {
		t.Fatalf("node-routed zombie Write = n=%d st=%v, want 2/OK", n, st)
	}
	fh.Lock()
	got := string(fh.Dirty.bytesView())
	fh.Unlock()
	if got != "aabb" {
		t.Fatalf("zombie buffer = %q, want aabb", got)
	}

	fs.Forget(fh.Ino, 1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		puts := srv.bodiesFor("/z2.txt")
		if len(puts) >= 2 && string(puts[len(puts)-1]) == "aabb" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("zombie purge did not commit; PUTs = %q", puts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestZombieNotCreatedWhenCacheOff(t *testing.T) {
	srv := &zombieTestServer{bodies: make(map[string][][]byte)}
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()
	opts := &MountOptions{WritebackCache: WritebackCacheOff}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	fh, fhID := newZombieWritableHandle(fs, "/plain.txt", 0, 0)
	fh.IsNew = true
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	}, []byte("x")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID})
	if fh.Zombie {
		t.Fatal("handle zombified with the kernel writeback cache off")
	}
	if _, ok := fs.fileHandles.Get(fhID); ok {
		t.Fatal("handle still registered after Release with cache off")
	}
}

func TestWriteMissingHandleNeverDiscardsClassicBytes(t *testing.T) {
	// Mixed profile: extent patterns configured, kernel cache on.
	opts := &MountOptions{ExtentPaths: []string{"*.db"}}
	fs, _ := newZombieTestFS(t, opts)

	// A classic file's late writeback with no live handle and no zombie must
	// fail loudly (ENOENT), never be silently discarded — the pre-fix
	// extentEnabled() branch returned OK here.
	classicIno := fs.inodes.Lookup("/notes.txt", false, 0, time.Now())
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: classicIno},
		Fh:         0,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("x")); st != gofuse.ENOENT {
		t.Fatalf("classic missing-handle Write = %v, want ENOENT", st)
	}

	// An extent file's orphan writeback (mapping gone, e.g. unlink + forget)
	// is still discarded — it has nowhere to land — but only for nodeids
	// that provably belonged to an extent file.
	extentIno := fs.inodes.Lookup("/x.db", false, 0, time.Now())
	fs.inodes.SetExtentIno(extentIno, 42)
	n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: extentIno},
		Fh:         0,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("x"))
	if st != gofuse.OK || n != 1 {
		t.Fatalf("extent orphan discard Write = n=%d st=%v, want 1/OK", n, st)
	}
}

func TestZombieJanitorEvictsExpired(t *testing.T) {
	fs, _ := newZombieTestFS(t, nil)
	fh, fhID := newZombieWritableHandle(fs, "/old.txt", 0, 0)
	fh.IsNew = true
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	}, []byte("x")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID})
	if !fh.Zombie {
		t.Fatal("handle was not zombified")
	}

	// Backdate past the TTL and sweep: the janitor force-purges the zombie
	// even though the kernel never sent FORGET.
	fh.Lock()
	fh.ZombifiedAt = time.Now().Add(-2 * zombieMaxAge)
	fh.Unlock()
	fs.sweepExpiredZombies()
	if _, ok := fs.fileHandles.Get(fhID); ok {
		t.Fatal("expired zombie survived the janitor sweep")
	}
	if fs.zombieHandleForNode(fh.Ino) != nil {
		t.Fatal("expired zombie still indexed")
	}
}

func TestZombieFsyncFromReopenedHandleCommits(t *testing.T) {
	fs, srv := newZombieTestFS(t, nil)
	fh, fhID := newZombieWritableHandle(fs, "/z3.txt", 0, 0)
	fh.IsNew = true
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	}, []byte("aa")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID})

	// Late writeback absorbed by the zombie.
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: fh.Ino},
		Fh:         fhID,
		Offset:     2,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("bb")); st != gofuse.OK {
		t.Fatalf("zombie Write: %v", st)
	}

	// The app reopens the file and fsyncs it: the fsync must commit the
	// zombie's absorbed data, not return while it sits in the dead handle.
	fh2 := &FileHandle{
		Ino:      fh.Ino,
		Path:     "/z3.txt",
		Flags:    uint32(syscall.O_RDWR),
		OrigSize: 4,
		BaseRev:  1,
	}
	fhID2 := fs.allocateFileHandle(fh2)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID2}); st != gofuse.OK {
		t.Fatalf("Fsync = %v, want OK", st)
	}
	puts := srv.bodiesFor("/z3.txt")
	if len(puts) < 2 || string(puts[len(puts)-1]) != "aabb" {
		t.Fatalf("fsync did not commit zombie data; PUTs = %q", puts)
	}
}

func TestValidateMountOptionsRejectsWriteSyncWithForcedWritebackCache(t *testing.T) {
	opts := &MountOptions{WritePolicy: WritePolicyWriteSync, WritebackCache: WritebackCacheOn}
	opts.setDefaults()
	if err := validateMountOptionsProfile(opts); err == nil ||
		!strings.Contains(err.Error(), "incompatible with write-sync") {
		t.Fatalf("validateMountOptionsProfile = %v, want write-sync incompatibility", err)
	}

	// auto keeps the cache off for write-sync (kernelWritebackCacheEnabled),
	// so it must stay valid.
	opts = &MountOptions{WritePolicy: WritePolicyWriteSync, WritebackCache: WritebackCacheAuto}
	opts.setDefaults()
	if err := validateMountOptionsProfile(opts); err != nil {
		t.Fatalf("validateMountOptionsProfile(write-sync, auto) = %v, want nil", err)
	}

	// Forcing the cache on for writeback-policy mounts stays valid.
	opts = &MountOptions{WritePolicy: WritePolicyWriteBack, WritebackCache: WritebackCacheOn}
	opts.setDefaults()
	if err := validateMountOptionsProfile(opts); err != nil {
		t.Fatalf("validateMountOptionsProfile(writeback, on) = %v, want nil", err)
	}
}

// zombieDeleteServer records the ordered verb sequence per path so tests can
// assert a zombie commit never lands after the path was deleted.
type zombieDeleteServer struct {
	zombieTestServer
	mu   sync.Mutex
	ops  []string
	gone map[string]bool
}

func (s *zombieDeleteServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/fs/") {
			http.NotFound(w, r)
			return
		}
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		s.mu.Lock()
		s.ops = append(s.ops, r.Method+" "+p)
		if s.gone == nil {
			s.gone = make(map[string]bool)
		}
		switch r.Method {
		case http.MethodPut:
			s.gone[p] = false
		case http.MethodDelete:
			s.gone[p] = true
		}
		gone := s.gone[p]
		s.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			s.mu.Lock()
			s.bodies[p] = append(s.bodies[p], body)
			s.rev++
			rev := s.rev
			s.mu.Unlock()
			w.Header().Set("X-Dat9-Revision", itoa(rev))
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			if gone {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
}

func (s *zombieDeleteServer) opsFor(path string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, op := range s.ops {
		if strings.HasSuffix(op, " "+path) {
			out = append(out, op)
		}
	}
	return out
}

func TestZombieUnlinkedDiscardsLateWriteback(t *testing.T) {
	srv := &zombieDeleteServer{zombieTestServer: zombieTestServer{bodies: make(map[string][][]byte)}}
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.kernelWritebackCache = true
	t.Cleanup(fs.stopZombieJanitor)

	fh, fhID := newZombieWritableHandle(fs, "/zu.txt", 0, 0)
	fh.IsNew = true
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	}, []byte("aa")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID})
	if !fh.Zombie {
		t.Fatal("handle was not zombified")
	}

	// The unlink flow funnels through markOpenHandlesUnlinked; call it
	// directly (the wiring point under test) instead of the full Unlink RPC
	// flow, which needs much more fake-server surface.
	if _, _, err := fs.markOpenHandlesUnlinked(context.TODO(), "/zu.txt", false); err != nil {
		t.Fatalf("markOpenHandlesUnlinked: %v", err)
	}
	if !fh.Unlinked {
		t.Fatal("zombie was not marked unlinked by the unlink flow")
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: fh.Ino},
		Fh:         fhID,
		Offset:     2,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("zz")); st != gofuse.OK || n != 2 {
		t.Fatalf("unlinked zombie Write = n=%d st=%v, want 2/OK", n, st)
	}

	// Give any (incorrect) commit a chance to fire, then purge at Forget.
	time.Sleep(3 * zombieCommitDebounce)
	fs.Forget(fh.Ino, 1)
	for _, op := range srv.opsFor("/zu.txt") {
		if strings.HasPrefix(op, "PUT") && strings.Contains(strings.Join(srvBodies(srv, "/zu.txt"), "|"), "zz") {
			t.Fatalf("unlinked zombie writeback was uploaded: %v", srv.opsFor("/zu.txt"))
		}
	}
	if strings.Contains(strings.Join(srvBodies(srv, "/zu.txt"), "|"), "zz") {
		t.Fatal("unlinked zombie writeback reached the server")
	}
}

func srvBodies(srv *zombieDeleteServer, path string) []string {
	var out []string
	for _, b := range srv.bodiesFor(path) {
		out = append(out, string(b))
	}
	return out
}

func TestZombieRenameRetargetsCommit(t *testing.T) {
	fs, srv := newZombieTestFS(t, nil)
	fh, fhID := newZombieWritableHandle(fs, "/zr.txt", 0, 0)
	fh.IsNew = true
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       fhID,
	}, []byte("aa")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: fhID})
	if !fh.Zombie {
		t.Fatal("handle was not zombified")
	}

	// Rename while the zombie is live: the pending commit must land on the
	// new path, not resurrect the old one.
	fs.retargetOpenHandlesForRename("/zr.txt", "/zr2.txt")
	if fh.Path != "/zr2.txt" {
		t.Fatalf("zombie path = %q, want /zr2.txt", fh.Path)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: fh.Ino},
		Fh:         fhID,
		Offset:     2,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("bb")); st != gofuse.OK {
		t.Fatalf("zombie Write after rename: %v", st)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		puts := srv.bodiesFor("/zr2.txt")
		if len(puts) > 0 && string(puts[len(puts)-1]) == "aabb" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("zombie commit did not land on /zr2.txt; zr2 PUTs=%q zr PUTs=%q",
				srv.bodiesFor("/zr2.txt"), srv.bodiesFor("/zr.txt"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The kernel writeback cache sends O_APPEND writes as WRITE_CACHE requests
// carrying absolute offsets, but Go rejects os.File.WriteAt on an O_APPEND
// fd. This was the `git clone` reflog EIO regression: the first writeback to
// git's O_APPEND-reflog failed with EIO at "update refs/remotes/origin/HEAD".
func TestWriteLocalFileForHandleAppendWriteCache(t *testing.T) {
	openAppend := func(t *testing.T) (*FileHandle, func()) {
		t.Helper()
		f, err := os.OpenFile(filepath.Join(t.TempDir(), "reflog"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		fh := &FileHandle{Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND), LocalFile: f}
		return fh, func() { _ = f.Close() }
	}

	fh, cleanup := openAppend(t)
	defer cleanup()

	// Plain O_APPEND syscall write: lands at the current end.
	if n, err := writeLocalFileForHandle(fh, 0, []byte("abc"), 0); err != nil || n != 3 {
		t.Fatalf("plain append write: n=%d err=%v", n, err)
	}
	// WRITE_CACHE [0,6) — the whole dirty range at absolute offsets: must
	// replace, not duplicate ("abcabc" would be the plain-append bug).
	if n, err := writeLocalFileForHandle(fh, 0, []byte("abcXYZ"), gofuse.WRITE_CACHE); err != nil || n != 6 {
		t.Fatalf("writeback whole-range: n=%d err=%v", n, err)
	}
	// WRITE_CACHE [6,9) — a tail range: must append after the existing end.
	if n, err := writeLocalFileForHandle(fh, 6, []byte("123"), gofuse.WRITE_CACHE); err != nil || n != 3 {
		t.Fatalf("writeback tail range: n=%d err=%v", n, err)
	}
	data, err := os.ReadFile(fh.LocalFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcXYZ123" {
		t.Fatalf("content = %q, want abcXYZ123", data)
	}

	// WRITE_CACHE beyond the current end zero-fills the gap, like WriteAt.
	if _, err := writeLocalFileForHandle(fh, 12, []byte("zz"), gofuse.WRITE_CACHE); err != nil {
		t.Fatalf("writeback with gap: %v", err)
	}
	data, _ = os.ReadFile(fh.LocalFile.Name())
	if len(data) != 14 || string(data[12:]) != "zz" || string(data[:9]) != "abcXYZ123" {
		t.Fatalf("content after gap fill = %q (len %d)", data, len(data))
	}
}

func TestWriteAppendLocalHandleWriteCacheStatus(t *testing.T) {
	srv := &zombieTestServer{bodies: make(map[string][][]byte)}
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.kernelWritebackCache = true
	t.Cleanup(fs.stopZombieJanitor)

	f, err := os.OpenFile(filepath.Join(t.TempDir(), "reflog"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	ino := fs.inodes.Lookup("/logs/refs/remotes/origin/HEAD", false, 0, time.Now())
	fh := &FileHandle{
		Ino:       ino,
		Path:      "/logs/refs/remotes/origin/HEAD",
		Layer:     PathLayerLocalOnly,
		Flags:     uint32(syscall.O_WRONLY | syscall.O_APPEND),
		LocalFile: f,
	}
	fhID := fs.allocateFileHandle(fh)

	// The exact reflog scenario: O_APPEND handle, first WRITE_CACHE at off=0
	// must not fail with EIO.
	n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader:   gofuse.InHeader{NodeId: ino},
		Fh:         fhID,
		Offset:     0,
		WriteFlags: gofuse.WRITE_CACHE,
	}, []byte("reflog-entry"))
	if st != gofuse.OK || n != 12 {
		t.Fatalf("O_APPEND WRITE_CACHE Write = n=%d st=%v, want 12/OK", n, st)
	}
	data, _ := os.ReadFile(f.Name())
	if string(data) != "reflog-entry" {
		t.Fatalf("content = %q, want reflog-entry", data)
	}
}

// A path truncate through one hardlink alias must refresh the base revision
// of handles opened through the sibling aliases. Before the fix, only the
// canonical path's handles were updated: a writer on the alias kept the
// stale base, its commit conflicted forever, and the content never landed
// remotely (remote stayed at the truncated 0 bytes).
func TestPathTruncateRefreshesBaseRevisionAcrossHardlinkAliases(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), opts)
	const pid = 4321

	now := time.Now()
	ino := fs.inodes.LookupWithIdentity("/alpha/text.txt", "rid-1", 2, false, 29, now)
	fs.inodes.AddAlias(ino, "/alpha/text-hardlink.txt", "rid-1", 2, false, 29, now)

	writer := &FileHandle{
		Ino:     ino,
		Path:    "/alpha/text-hardlink.txt",
		Flags:   uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		OpenPID: pid,
		Dirty:   fs.newWriteBuffer("/alpha/text-hardlink.txt", 0, 0),
		BaseRev: 2,
	}
	_, _ = writer.Dirty.Write(0, []byte("hardlink-x"))
	fs.allocateFileHandle(writer)

	// The truncate commits through the canonical path (server revision 3).
	fs.updateOpenHandleBaseRevision("/alpha/text.txt", 3, pid, 0)

	writer.Lock()
	baseRev := writer.BaseRev
	bufferSize := writer.Dirty.Size()
	writer.Unlock()
	if baseRev != 3 {
		t.Fatalf("alias writer BaseRev = %d, want 3 (post-truncate revision)", baseRev)
	}
	// The O_TRUNC adoption truncates the alias writer's buffer in step with
	// the remote truncate, so no stale tail can leak into the next commit.
	if bufferSize != 0 {
		t.Fatalf("alias writer buffer size = %d, want 0 after truncate adoption", bufferSize)
	}
}

// The full O_TRUNC-on-hardlink-alias regression: printf-style truncate+write
// through a hardlink alias must end with the caller's content on the shared
// remote object — never stuck at 0 bytes because the remote zero-truncate
// raced the content commit and won.
func TestPathTruncateOnHardlinkAliasCommitsCallerContent(t *testing.T) {
	const pid = 7777

	// Revision-checking server seeded at rev 2 for the shared object.
	var mu sync.Mutex
	rev := int64(2)
	content := []byte("overwrite-x-append-0123456789")
	puts := make(map[string][][]byte)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if p != "/alpha/text.txt" && p != "/alpha/text-hardlink.txt" {
				w.Header().Set("X-Dat9-IsDir", "true")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(content)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			if exp := r.Header.Get("X-Dat9-Expected-Revision"); exp != "" && exp != strconv.FormatInt(rev, 10) {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			body, _ := io.ReadAll(r.Body)
			content = append([]byte(nil), body...)
			rev++
			puts[p] = append(puts[p], body)
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			_, _ = w.Write(content)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.kernelWritebackCache = true
	t.Cleanup(fs.stopZombieJanitor)

	// text.txt committed at revision 2 with 29 bytes, aliased as
	// text-hardlink.txt (same inode).
	ino := fs.inodes.LookupWithIdentity("/alpha/text.txt", "rid-1", 2, false, 29, time.Now())
	fs.inodes.AddAlias(ino, "/alpha/text-hardlink.txt", "rid-1", 2, false, 29, time.Now())
	fs.inodes.UpdateRevision(ino, 2)
	fs.replaceCommittedRevision("/alpha/text.txt", 2)

	// The caller opens the alias with O_TRUNC (printf ">"): writable handle,
	// BaseRev from the open-time stat.
	writer := &FileHandle{
		Ino:      ino,
		Path:     "/alpha/text-hardlink.txt",
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		OpenPID:  pid,
		Dirty:    fs.newWriteBuffer("/alpha/text-hardlink.txt", 0, 0),
		OrigSize: 29,
		BaseRev:  2,
	}
	fhID := fs.allocateFileHandle(writer)

	// The O_TRUNC SetAttr arrives without FATTR_FH (path-based, at open).
	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: pid}},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr(size=0): %v", st)
	}
	writer.Lock()
	bufSize := writer.Dirty.Size()
	baseRev := writer.BaseRev
	writer.Unlock()
	if bufSize != 0 {
		t.Fatalf("caller buffer size = %d, want 0 after truncate adoption", bufSize)
	}
	// The caller is the inode's only live writer, so the truncate is folded
	// into its own handle: no remote zero is committed and the base stays at
	// the open value — the caller's next commit CAS-succeeds at it.
	if baseRev != 2 {
		t.Fatalf("caller BaseRev = %d, want 2 (folded truncate)", baseRev)
	}

	// printf's write + close commits the content — with the refreshed base,
	// no revision conflict is possible.
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	}, []byte("hardlink-x")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID})

	mu.Lock()
	final := string(content)
	mu.Unlock()
	if final != "hardlink-x" {
		t.Fatalf("final remote content = %q, want hardlink-x", final)
	}
}
