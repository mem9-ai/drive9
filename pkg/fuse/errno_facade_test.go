package fuse

// Ports of the v1 errno alignment rows (d39a6d1b3 pkg/fuse/
// errno_alignment_test.go / errno_namespace_test.go) that bind at the FUSE
// facade. The HTTP-side quota table and the transport matrix live in
// pkg/fusecomp/httperr; these tests pin what the facade itself owns:
//
//   - LocalErrors: chain errors — os sentinels, syscall errnos (bare,
//     PathError/LinkError-wrapped, double-wrapped) and fs.StatusError —
//     surface through Lookup as their canonical kernel statuses
//     (fs.StatusOf, the successor of v1 localErrToFuseStatus).
//   - LocalDirectoryOperations: rmdir/rename of a non-empty local-only
//     directory answer ENOTEMPTY from the local overlay without a single
//     remote call (localize + coding-agent builtin node_modules pattern).
//   - ShadowUploadFailures: strict-fsync and close-sync-flush upload
//     failures surface the mapped errno (EDQUOT/EFBIG/EIO/EAGAIN) while
//     the uncommitted handle state survives for the retry.
//   - XAttrNamespaces: unknown xattr namespaces are EOPNOTSUPP (linux),
//     known ones round-trip, and every xattr op on a missing inode is
//     ENOENT before the chain is consulted.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// ---------------------------------------------------------------------------
// LocalErrors (v1 localErrToFuseStatus rows, driven through fs.StatusOf and
// the facade's Lookup dispatch)
// ---------------------------------------------------------------------------

// errCore answers every op with a fixed error, so the facade's error
// mapping is exercised without any I/O.
type errCore struct{ err error }

func (c *errCore) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return fs.Result{}, c.err
}

func TestErrnoAlignmentLocalErrors(test *testing.T) {
	testCases := []struct {
		name string
		err  error
		want gofuse.Status
	}{
		{"eof", io.EOF, gofuse.OK},
		{"permission_sentinel", os.ErrPermission, gofuse.EACCES},
		{"exist_sentinel", os.ErrExist, gofuse.Status(syscall.EEXIST)},
		{"not_exist_sentinel", os.ErrNotExist, gofuse.ENOENT},
		{"status_error", fs.StatusErrorOf(gofuse.Status(syscall.ESTALE)), gofuse.Status(syscall.ESTALE)},
		{"unsupported", fs.ErrUnsupported, gofuse.Status(syscall.ENOSYS)},
		{"unknown", errors.New("unknown local error"), gofuse.EIO},
	}
	for _, errno := range []syscall.Errno{syscall.ENOTEMPTY, syscall.EPERM, syscall.EACCES, syscall.EEXIST, syscall.ENOENT, syscall.ENOTDIR, syscall.EISDIR, syscall.EXDEV, syscall.EFBIG, syscall.EDQUOT, syscall.ENOSPC, syscall.EIO} {
		testCases = append(testCases, struct {
			name string
			err  error
			want gofuse.Status
		}{errno.Error(), errno, gofuse.Status(errno)})
	}
	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			variants := []error{testCase.err}
			if testCase.err != nil {
				variants = append(variants,
					&os.PathError{Op: "remove", Path: "/local", Err: testCase.err},
					&os.LinkError{Op: "rename", Old: "/old", New: "/new", Err: testCase.err},
					fmt.Errorf("outer: %w", &os.PathError{Op: "remove", Path: "/local", Err: testCase.err}),
				)
			}
			for _, variant := range variants {
				if got := fs.StatusOf(variant); got != testCase.want {
					test.Errorf("fs.StatusOf(%v) = %v, want %v", variant, got, testCase.want)
				}
			}
			// The facade must answer with the same status: Lookup dispatches
			// through the chain and maps with fs.StatusOf (v1 dispatch).
			f, err := New(Options{Extensions: []fs.Handler{&errCore{err: testCase.err}}, Uid: 1000, Gid: 1000})
			if err != nil {
				test.Fatal(err)
			}
			var out gofuse.EntryOut
			if got := f.Lookup(nil, rootHeader(), "f", &out); got != testCase.want {
				test.Errorf("facade Lookup = %v, want %v", got, testCase.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// LocalDirectoryOperations (v1: local-only ENOTEMPTY rows, zero remote calls)
// ---------------------------------------------------------------------------

func TestErrnoAlignmentLocalDirectoryOperations(test *testing.T) {
	var remoteCalls atomic.Int32
	b := testbackend.NewBackend()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		remoteCalls.Add(1)
		b.ServeHTTP(writer, request)
	}))
	test.Cleanup(server.Close)
	cl := client.New(server.URL, "")
	cl.Warm(context.Background())

	// coding-agent profile: **/node_modules/** is local-only (v1 parity;
	// builtin defaults apply for library callers, as in MountWithConfig).
	loc, err := localize.New(localize.Options{LocalRoot: test.TempDir(), Profile: "coding-agent", ApplyBuiltinDefaults: true})
	if err != nil {
		test.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	f, err := New(Options{Extensions: []fs.Handler{loc, terminal}, Uid: 1000, Gid: 1000})
	if err != nil {
		test.Fatal(err)
	}
	f.Init(nil)

	// Create /node_modules/{source,target} and target/child through the
	// facade; every path is claimed by the local overlay.
	mkdir := func(parent uint64, name string) uint64 {
		test.Helper()
		mi := &gofuse.MkdirIn{Mode: 0o755}
		mi.NodeId = parent
		var eout gofuse.EntryOut
		if st := f.Mkdir(nil, mi, name, &eout); st != gofuse.OK {
			test.Fatalf("mkdir %s: %v", name, st)
		}
		return eout.NodeId
	}
	nmIno := mkdir(1, "node_modules")
	srcIno := mkdir(nmIno, "source")
	tgtIno := mkdir(nmIno, "target")
	ci := &gofuse.CreateIn{Flags: uint32(os.O_CREATE | os.O_RDWR), Mode: 0o644}
	ci.NodeId = tgtIno
	var cout gofuse.CreateOut
	if st := f.Create(nil, ci, "child", &cout); st != gofuse.OK {
		test.Fatalf("create target/child: %v", st)
	}

	// Client warm-up (tenant status probe) is setup, not mount traffic.
	remoteCalls.Store(0)

	if got := f.Rmdir(nil, &gofuse.InHeader{NodeId: nmIno}, "target"); got != gofuse.Status(syscall.ENOTEMPTY) {
		test.Errorf("Rmdir nonempty local directory = %v, want ENOTEMPTY", got)
	}
	ri := &gofuse.RenameIn{Newdir: nmIno}
	ri.NodeId = nmIno
	if got := f.Rename(nil, ri, "source", "target"); !nonemptyDirRenameErrno(got) {
		test.Errorf("Rename onto nonempty local directory = %v, want ENOTEMPTY (darwin may answer EEXIST)", got)
	}
	// Failed operations changed nothing.
	for _, ino := range []uint64{srcIno, cout.NodeId} {
		if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &gofuse.AttrOut{}); st != gofuse.OK {
			test.Errorf("failed operation changed inode %d: %v", ino, st)
		}
	}
	if got := remoteCalls.Load(); got != 0 {
		test.Fatalf("remote calls = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// ShadowUploadFailures (v1 fsync/flush tiers: upload errno surfaces, state
// survives). The single-shot transport rows are pinned in
// pkg/fusecomp/httperr; these rows bind the facade tiers.
// ---------------------------------------------------------------------------

// failingUploadBackend wraps the test backend, failing the write endpoints
// (PUT /v1/fs/<path> and POST /v2/uploads/initiate) with a fixed code and
// JSON error body until cleared.
type failingUploadBackend struct {
	inner    *testbackend.Backend
	code     atomic.Int32
	message  atomic.Value // string
	failPut  atomic.Bool
	requests atomic.Int32
}

func (b *failingUploadBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.requests.Add(1)
	if !b.failPut.Load() {
		b.inner.ServeHTTP(w, r)
		return
	}
	if (r.Method == http.MethodPut && len(r.URL.Path) > len("/v1/fs/") && r.URL.Path[:len("/v1/fs/")] == "/v1/fs/") ||
		(r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(int(b.code.Load()))
		_ = json.NewEncoder(w).Encode(map[string]string{"error": b.message.Load().(string)})
		return
	}
	b.inner.ServeHTTP(w, r)
}

func (b *failingUploadBackend) fail(code int, message string) {
	b.code.Store(int32(code))
	b.message.Store(message)
	b.failPut.Store(true)
}

func (b *failingUploadBackend) recover() { b.failPut.Store(false) }

// newUploadFailureFS assembles [writeback, core] over the failing backend.
func newUploadFailureFS(test *testing.T, policy writeback.Policy, threshold int64) (*FS, *failingUploadBackend) {
	test.Helper()
	b := &failingUploadBackend{inner: testbackend.NewBackend()}
	server := httptest.NewServer(b)
	test.Cleanup(server.Close)
	cl := client.New(server.URL, "")
	cl.Warm(context.Background())
	cl.SetSmallFileThresholdForTests(threshold)
	wb, err := writeback.New(writeback.Options{Policy: policy, Client: cl, SyncMode: "strict"})
	if err != nil {
		test.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, Uid: 1000, Gid: 1000})
	if err != nil {
		test.Fatal(err)
	}
	f.Init(nil)
	return f, b
}

// writeQuotaFile creates /quota.bin and writes a small payload, returning
// the node id and file handle.
func writeQuotaFile(test *testing.T, f *FS) (ino, fh uint64) {
	test.Helper()
	ci := &gofuse.CreateIn{Flags: uint32(os.O_CREATE | os.O_RDWR), Mode: 0o644}
	ci.NodeId = 1
	var cout gofuse.CreateOut
	if st := f.Create(nil, ci, "quota.bin", &cout); st != gofuse.OK {
		test.Fatalf("create: %v", st)
	}
	wi := &gofuse.WriteIn{Offset: 0}
	wi.NodeId, wi.Fh = cout.NodeId, cout.Fh
	if n, st := f.Write(nil, wi, []byte("payload")); st != gofuse.OK || n != 7 {
		test.Fatalf("write = %d %v", n, st)
	}
	return cout.NodeId, cout.Fh
}

func TestErrnoAlignmentShadowUploadFailures(test *testing.T) {
	cases := []struct {
		name    string
		code    int
		message string
		want    gofuse.Status
	}{
		{"storage", 507, "tenant storage quota exceeded: server limit=8 used=8 reserved=0 pending=0 delta=1", gofuse.Status(syscall.EDQUOT)},
		{"count", 507, "tenant file count quota exceeded: server limit=8 used=8 pending=0 delta=1", gofuse.Status(syscall.EDQUOT)},
		{"file_size", 507, "tenant file size quota exceeded: server limit=8 requested=9", gofuse.Status(syscall.EFBIG)},
		{"unknown_507", 507, "tenant storage quota exceeded: backend unavailable", gofuse.EIO},
		{"internal_error", 500, "backend unavailable", gofuse.EAGAIN},
		// A persistent revision conflict exhausts the commit retry ladder
		// and surfaces its EAGAIN terminal (the single-shot 409 → EIO row
		// is pinned in pkg/fusecomp/httperr).
		{"revision_conflict", 409, "revision conflict", gofuse.EAGAIN},
	}
	ops := []struct {
		name      string
		policy    writeback.Policy
		closeSync bool // flush (close-sync) tier instead of fsync
	}{
		{"fsync", writeback.PolicyWriteBack, false},
		{"flush_close_sync", writeback.PolicyCloseSync, true},
	}
	transports := []struct {
		name      string
		threshold int64
	}{
		{"put", 1 << 20},
		{"multipart", 1},
	}
	for _, op := range ops {
		for _, transport := range transports {
			for _, testCase := range cases {
				test.Run(op.name+"/"+transport.name+"/"+testCase.name, func(test *testing.T) {
					f, b := newUploadFailureFS(test, op.policy, transport.threshold)
					ino, fh := writeQuotaFile(test, f)
					b.fail(testCase.code, testCase.message)

					var got gofuse.Status
					if op.closeSync {
						got = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh})
					} else {
						got = f.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh})
					}
					if got != testCase.want {
						test.Fatalf("%s = %v, want %v", op.name, got, testCase.want)
					}
					if b.requests.Load() == 0 {
						test.Fatal("operation did not attempt remote upload")
					}
					// The uncommitted image survives the failed upload: once
					// the backend recovers, the same handle commits.
					b.recover()
					if op.closeSync {
						if got := f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh}); got != gofuse.OK {
							test.Fatalf("flush after recovery = %v, want OK (state was lost)", got)
						}
					} else {
						if got := f.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fh}); got != gofuse.OK {
							test.Fatalf("fsync after recovery = %v, want OK (state was lost)", got)
						}
					}
				})
			}
		}
	}
}

// ---------------------------------------------------------------------------
// XAttrNamespaces (v1 namespace matrix through the xattr extension)
// ---------------------------------------------------------------------------

func TestErrnoAlignmentXAttrNamespaces(test *testing.T) {
	for _, attr := range []string{"unknown.test", "user", "", "User.test", "prefix.user.test", "com.apple.test", "user.test", "security.test", "trusted.test", "system.test"} {
		test.Run(attr, func(test *testing.T) {
			f := newXattrFS(test)
			var lout gofuse.EntryOut
			if st := f.Lookup(nil, rootHeader(), "f", &lout); st != gofuse.OK {
				test.Fatal(st)
			}
			header := &gofuse.InHeader{NodeId: lout.NodeId}
			known := attr == "user.test" || attr == "security.test" || attr == "trusted.test" || attr == "system.test"
			reject := linuxPlatform() && !known
			want := gofuse.OK
			if reject {
				want = gofuse.Status(syscall.EOPNOTSUPP)
			}
			if got := f.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *header}, attr, []byte("value")); got != want {
				test.Errorf("SetXAttr = %v, want %v", got, want)
			}
			// nil dest is a size probe in the kernel; the facade answers
			// ERANGE for a too-small buffer, so probe with a real buffer.
			if _, got := f.GetXAttr(nil, header, attr, make([]byte, 16)); got != want {
				test.Errorf("GetXAttr = %v, want %v", got, want)
			}
			if got := f.RemoveXAttr(nil, header, attr); got != want {
				test.Errorf("RemoveXAttr = %v, want %v", got, want)
			}
			if reject {
				// Flagged variants of an unknown namespace are refused too.
				const xattrCreateFlag = 1
				const xattrReplaceFlag = 2
				if got := f.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *header, Flags: xattrCreateFlag}, attr, nil); got != want {
					test.Errorf("unknown namespace with create flag = %v, want %v", got, want)
				}
				if got := f.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *header, Flags: xattrReplaceFlag}, attr, nil); got != want {
					test.Errorf("unknown namespace with replace flag = %v, want %v", got, want)
				}
				if _, got := f.GetXAttr(nil, header, attr, nil); got != want {
					test.Errorf("missing unknown xattr = %v, want %v", got, want)
				}
				if got := f.RemoveXAttr(nil, header, attr); got != want {
					test.Errorf("remove missing unknown xattr = %v, want %v", got, want)
				}
			} else if _, got := f.GetXAttr(nil, header, attr, nil); got != gofuse.ENOATTR {
				test.Errorf("removed known xattr = %v, want ENOATTR", got)
			}
			// A missing inode answers ENOENT before the chain runs.
			missing := &gofuse.InHeader{NodeId: lout.NodeId + 1000}
			if got := f.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *missing}, attr, nil); got != gofuse.ENOENT {
				test.Errorf("missing inode SetXAttr = %v, want ENOENT", got)
			}
			if _, got := f.GetXAttr(nil, missing, attr, nil); got != gofuse.ENOENT {
				test.Errorf("missing inode GetXAttr = %v, want ENOENT", got)
			}
			if got := f.RemoveXAttr(nil, missing, attr); got != gofuse.ENOENT {
				test.Errorf("missing inode RemoveXAttr = %v, want ENOENT", got)
			}
		})
	}
}

// nonemptyDirRenameErrno accepts the errno rename(2) uses for a non-empty
// destination directory: Linux answers ENOTEMPTY, darwin EEXIST — POSIX
// permits both.
func nonemptyDirRenameErrno(st gofuse.Status) bool {
	return st == gofuse.Status(syscall.ENOTEMPTY) || st == gofuse.Status(syscall.EEXIST)
}

func linuxPlatform() bool { return runtime.GOOS == "linux" }
