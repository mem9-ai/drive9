//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/promotion"
	"golang.org/x/sys/unix"
)

func TestSynchronousPromotionPublishesClosedLocalTree(t *testing.T) {
	for _, responseLoss := range []bool{false, true} {
		name := "normal"
		if responseLoss {
			name = "commit_response_lost"
		}
		t.Run(name, func(t *testing.T) {
			var committed atomic.Pointer[promotion.Result]
			var publishCalls, getCalls, acknowledgeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && r.URL.Path == "/v1/fs/project/site" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if r.URL.Path != "/v1/fs:promotion" {
					http.NotFound(w, r)
					return
				}
				switch r.Method {
				case http.MethodPost:
					publishCalls.Add(1)
					var request promotion.PublishRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Fatalf("decode promotion: %v", err)
					}
					if request.Target != "/project/site/" || len(request.Entries) != 3 {
						t.Fatalf("request target=%q entries=%d", request.Target, len(request.Entries))
					}
					result := &promotion.Result{OperationID: request.OperationID, Target: request.Target, ManifestSHA256: request.ManifestSHA256, Committed: true}
					committed.Store(result)
					if responseLoss {
						panic(http.ErrAbortHandler)
					}
					_ = json.NewEncoder(w).Encode(result)
				case http.MethodGet:
					getCalls.Add(1)
					result := committed.Load()
					if result == nil {
						http.NotFound(w, r)
						return
					}
					_ = json.NewEncoder(w).Encode(result)
				case http.MethodDelete:
					acknowledgeCalls.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, "method", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			localRoot := t.TempDir()
			opts := &MountOptions{
				CacheSize:                  1 << 20,
				Profile:                    MountProfileCodingAgent,
				LocalRoot:                  localRoot,
				EnableSynchronousPromotion: true,
			}
			opts.setDefaults()
			c := client.New(server.URL, "")
			c.SetSmallFileThresholdForTests(50_000)
			fs := NewDat9FS(c, opts)
			if err := fs.localOverlay.EnsureRoot(); err != nil {
				t.Fatal(err)
			}
			if err := fs.localOverlay.Mkdir("/project/dist/.site-next", 0o755); err != nil {
				t.Fatal(err)
			}
			if err := fs.localOverlay.Mkdir("/project/dist/.site-next/assets", 0o755); err != nil {
				t.Fatal(err)
			}
			file, err := fs.localOverlay.OpenFile("/project/dist/.site-next/assets/app.js", uint32(os.O_CREATE|os.O_WRONLY), 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteString("console.log('ok')"); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			projectIno := fs.inodes.Lookup("/project", true, 0, time.Now())
			distIno := fs.inodes.Lookup("/project/dist", true, 0, time.Now())
			fs.inodes.Lookup("/project/dist/.site-next", true, 0, time.Now())
			status := fs.Rename(nil, &gofuse.RenameIn{
				InHeader: gofuse.InHeader{NodeId: distIno},
				Newdir:   projectIno,
			}, ".site-next", "site")
			if status != gofuse.OK {
				t.Fatalf("Rename status = %v, want OK", status)
			}
			if _, err := fs.localOverlay.Lstat("/project/dist/.site-next"); !os.IsNotExist(err) {
				t.Fatalf("source still exists: %v", err)
			}
			waitForPromotionCleanup(t, fs)
			if publishCalls.Load() != 1 || acknowledgeCalls.Load() != 1 {
				t.Fatalf("calls publish=%d get=%d ack=%d", publishCalls.Load(), getCalls.Load(), acknowledgeCalls.Load())
			}
			if responseLoss && getCalls.Load() == 0 {
				t.Fatal("lost response was not resolved by operation_id lookup")
			}
		})
	}
}

func TestPromotionResponseLossLookupOutlivesCanceledCaller(t *testing.T) {
	postStarted := make(chan struct{})
	committed := make(chan struct{})
	var result atomic.Pointer[promotion.Result]
	var getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var request promotion.PublishRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode promotion: %v", err)
				return
			}
			close(postStarted)
			<-r.Context().Done()
			result.Store(&promotion.Result{
				OperationID: request.OperationID, Target: request.Target,
				ManifestSHA256: request.ManifestSHA256, Committed: true,
			})
			close(committed)
		case http.MethodGet:
			getCalls.Add(1)
			<-committed
			_ = json.NewEncoder(w).Encode(result.Load())
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	fs.client.SetSmallFileThresholdForTests(50_000)
	createPromotionTestTree(t, fs)
	request, err := fs.scanPromotionTree(context.Background(), "/project/dist/.site-next", "/project/site", "/project/site/",
		"12345678-90ab-cdef-1234-567890abcdef")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var got *promotion.Result
	var definitive bool
	var publishErr error
	go func() {
		got, definitive, publishErr = fs.publishPromotionWithRecovery(ctx, request)
		close(done)
	}()
	<-postStarted
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("detached result lookup did not finish")
	}
	if publishErr != nil || !definitive || got == nil || !matchingPromotionResult(request, got) {
		t.Fatalf("publish result = %+v definitive=%v err=%v", got, definitive, publishErr)
	}
	if getCalls.Load() == 0 {
		t.Fatal("canceled caller skipped durable result lookup")
	}
}

func TestPromotionOutcomeUnknownBlocksNamespaceReadsAndFlushes(t *testing.T) {
	var remoteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	fs.promotionBlocked.Store(true)
	for name, run := range map[string]func() gofuse.Status{
		"lookup": func() gofuse.Status {
			return fs.Lookup(nil, &gofuse.InHeader{}, "x", &gofuse.EntryOut{})
		},
		"getattr": func() gofuse.Status {
			return fs.GetAttr(nil, &gofuse.GetAttrIn{}, &gofuse.AttrOut{})
		},
		"access": func() gofuse.Status {
			return fs.Access(nil, &gofuse.AccessIn{})
		},
		"readlink": func() gofuse.Status {
			_, status := fs.Readlink(nil, &gofuse.InHeader{})
			return status
		},
		"opendir": func() gofuse.Status {
			return fs.OpenDir(nil, &gofuse.OpenIn{}, &gofuse.OpenOut{})
		},
		"readdir": func() gofuse.Status {
			return fs.ReadDir(nil, &gofuse.ReadIn{}, nil)
		},
		"readdirplus": func() gofuse.Status {
			return fs.ReadDirPlus(nil, &gofuse.ReadIn{}, nil)
		},
		"fsyncdir": func() gofuse.Status {
			return fs.FsyncDir(nil, &gofuse.FsyncIn{})
		},
		"open": func() gofuse.Status {
			return fs.Open(nil, &gofuse.OpenIn{}, &gofuse.OpenOut{})
		},
		"read": func() gofuse.Status {
			_, status := fs.Read(nil, &gofuse.ReadIn{}, nil)
			return status
		},
		"flush": func() gofuse.Status {
			return fs.Flush(nil, &gofuse.FlushIn{})
		},
		"fsync": func() gofuse.Status {
			return fs.Fsync(nil, &gofuse.FsyncIn{})
		},
		"getxattr": func() gofuse.Status {
			_, status := fs.GetXAttr(nil, &gofuse.InHeader{}, "user.test", nil)
			return status
		},
		"listxattr": func() gofuse.Status {
			_, status := fs.ListXAttr(nil, &gofuse.InHeader{}, nil)
			return status
		},
		"mkdir": func() gofuse.Status {
			return fs.Mkdir(nil, &gofuse.MkdirIn{}, "x", &gofuse.EntryOut{})
		},
		"setattr": func() gofuse.Status {
			return fs.SetAttr(nil, &gofuse.SetAttrIn{}, &gofuse.AttrOut{})
		},
		"mknod": func() gofuse.Status {
			return fs.Mknod(nil, &gofuse.MknodIn{}, "x", &gofuse.EntryOut{})
		},
		"symlink": func() gofuse.Status {
			return fs.Symlink(nil, &gofuse.InHeader{}, "target", "x", &gofuse.EntryOut{})
		},
		"link": func() gofuse.Status {
			return fs.Link(nil, &gofuse.LinkIn{}, "x", &gofuse.EntryOut{})
		},
		"unlink": func() gofuse.Status {
			return fs.Unlink(nil, &gofuse.InHeader{}, "x")
		},
		"rmdir": func() gofuse.Status {
			return fs.Rmdir(nil, &gofuse.InHeader{}, "x")
		},
		"rename": func() gofuse.Status {
			return fs.Rename(nil, &gofuse.RenameIn{}, "old", "new")
		},
		"create": func() gofuse.Status {
			return fs.Create(nil, &gofuse.CreateIn{}, "x", &gofuse.CreateOut{})
		},
		"write": func() gofuse.Status {
			_, status := fs.Write(nil, &gofuse.WriteIn{}, []byte("x"))
			return status
		},
		"setxattr": func() gofuse.Status {
			return fs.SetXAttr(nil, &gofuse.SetXAttrIn{}, "user.test", []byte("x"))
		},
		"removexattr": func() gofuse.Status {
			return fs.RemoveXAttr(nil, &gofuse.InHeader{}, "user.test")
		},
	} {
		t.Run(name, func(t *testing.T) {
			if status := run(); status != gofuse.EIO {
				t.Fatalf("status = %v, want EIO", status)
			}
		})
	}
	if remoteCalls.Load() != 0 {
		t.Fatalf("remote calls = %d, want 0", remoteCalls.Load())
	}
}

func TestPromotionOutcomeUnknownReleaseDropsHandleWithoutCommit(t *testing.T) {
	var remoteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 2})
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	ino := fs.inodes.Lookup("/blocked-release.txt", false, 0, time.Now())
	dirty := NewWriteBuffer("/blocked-release.txt", 1024, 0)
	if _, err := dirty.Write(0, []byte("must not be published by Release")); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino:         ino,
		Path:        "/blocked-release.txt",
		Dirty:       dirty,
		IsNew:       true,
		WritePolicy: WritePolicyCloseSync,
	}
	fh.DirtySeq = fs.markDirtySize(ino, dirty.Size())
	fhID := fs.allocateFileHandle(fh)
	fs.promotionBlocked.Store(true)

	fs.Release(nil, &gofuse.ReleaseIn{Fh: fhID})

	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls after blocked Release = %d, want 0", got)
	}
	if _, ok := fs.fileHandles.Get(fhID); ok {
		t.Fatal("blocked Release left the file handle registered")
	}
	if fs.openHandles.Has(ino, fh.Path) {
		t.Fatal("blocked Release left the handle in the open-handle index")
	}
	if _, ok := fs.dirtyHandleSize(ino); ok {
		t.Fatal("blocked Release left authoritative dirty-size state")
	}
	if fh.Dirty.HasDirtyParts() {
		t.Fatal("blocked Release left dirty bytes eligible for a later hidden flush")
	}
}

func TestSynchronousPromotionRejectsOpenSourceBeforeRemoteSideEffect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	opts := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: t.TempDir(), EnableSynchronousPromotion: true}
	opts.setDefaults()
	fs := NewDat9FS(client.New(server.URL, ""), opts)
	fs.openHandles.Add(&FileHandle{Path: "/project/dist/.site-next/file"})
	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("promoteLocalTree status = %v, want EXDEV", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("remote calls = %d, want 0", calls.Load())
	}
}

func TestSynchronousPromotionRejectsNonRemoteFinalPathBeforeRemoteSideEffect(t *testing.T) {
	var promotionCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path == "/v1/fs:promotion" {
			promotionCalls.Add(1)
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	if err := fs.localOverlay.Mkdir("/project/dist/.site-next", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fs.localOverlay.Mkdir("/project/dist/.site-next/node_modules", 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := fs.localOverlay.OpenFile("/project/dist/.site-next/node_modules/x.js", uint32(os.O_CREATE|os.O_WRONLY), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("module.exports = 1"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("promotion status = %v, want EXDEV", status)
	}
	if promotionCalls.Load() != 0 {
		t.Fatalf("promotion endpoint calls = %d, want 0", promotionCalls.Load())
	}
	if _, err := fs.localOverlay.Lstat("/project/dist/.site-next/node_modules/x.js"); err != nil {
		t.Fatalf("source changed after target-policy rejection: %v", err)
	}
	if _, err := os.Stat(fs.promotionRecordPath()); !os.IsNotExist(err) {
		t.Fatalf("journal after target-policy rejection: %v", err)
	}
}

func TestPromotionScanKeepsRootBeforeLexicallyEarlierNames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()
	fs := newPromotionTestFS(t, server.URL)
	for _, dir := range []string{
		"/project/dist/.site-next",
		"/project/dist/.site-next/#cache",
		"/project/dist/.site-next/-backup",
	} {
		if err := fs.localOverlay.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	request, err := fs.scanPromotionTree(
		context.Background(), "/project/dist/.site-next", "/project/site", "/project/site/",
		"01234567-89ab-cdef-0123-456789abcdef",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.Entries[0].RelativePath; got != "." {
		t.Fatalf("first entry = %q, want root", got)
	}
	if err := promotion.Validate(request, 50_000); err != nil {
		t.Fatalf("Validate scanner output: %v", err)
	}
}

func TestSynchronousPromotionRejectsOversizedInlineFileBeforeJournalOrReceiptLookup(t *testing.T) {
	var promotionCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path == "/v1/fs:promotion" {
			promotionCalls.Add(1)
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	fs.client.SetSmallFileThresholdForTests(50_000)
	if err := fs.localOverlay.Mkdir("/project/dist/.site-next", 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := fs.localOverlay.OpenFile("/project/dist/.site-next/big.bin", uint32(os.O_CREATE|os.O_WRONLY), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(make([]byte, 100_000)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("promotion status = %v, want EXDEV", status)
	}
	if promotionCalls.Load() != 0 {
		t.Fatalf("promotion endpoint calls = %d, want 0", promotionCalls.Load())
	}
	if _, err := fs.localOverlay.Lstat("/project/dist/.site-next/big.bin"); err != nil {
		t.Fatalf("source changed after local request rejection: %v", err)
	}
	if _, err := os.Stat(fs.promotionRecordPath()); !os.IsNotExist(err) {
		t.Fatalf("journal after local request rejection: %v", err)
	}
	if fs.promotionBlocked.Load() {
		t.Fatal("local request rejection blocked the mount")
	}
}

func TestPromotionLocalRootLockIsExclusive(t *testing.T) {
	root := t.TempDir()
	first, err := acquirePromotionRootLock(root)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer releasePromotionRootLock(first)
	second, err := acquirePromotionRootLock(root)
	if err == nil {
		releasePromotionRootLock(second)
		t.Fatal("second lock succeeded, want exclusive failure")
	}
	if _, statErr := os.Stat(filepath.Join(root, ".drive9", "promotion", "local-root.lock")); statErr != nil {
		t.Fatalf("lock file: %v", statErr)
	}
	if err := os.Remove(filepath.Join(root, ".drive9", "promotion", "local-root.lock")); err != nil {
		t.Fatalf("unlink held lock: %v", err)
	}
	if err := validatePromotionRootLock(first); err == nil {
		t.Fatal("unlinked held lock remained valid")
	}
	var remoteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()
	fs := newPromotionTestFSWithRoot(t, server.URL, root)
	fs.promotionRootLock = first
	createPromotionTestTree(t, fs)
	if status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site"); status != gofuse.EIO {
		t.Fatalf("promotion with unlinked lock = %v, want EIO", status)
	}
	if remoteCalls.Load() != 0 {
		t.Fatalf("remote calls with unlinked lock = %d, want 0", remoteCalls.Load())
	}
}

func TestPromotionDirectoryChainIsSyncedAsItIsCreated(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, ".drive9", "promotion", "quarantine")
	var synced []string
	testHookBeforePromotionDirSync = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	t.Cleanup(func() { testHookBeforePromotionDirSync = nil })

	if err := ensurePromotionDirDurable(target, 0o700); err != nil {
		t.Fatalf("ensurePromotionDirDurable: %v", err)
	}
	want := []string{root, filepath.Join(root, ".drive9"), filepath.Join(root, ".drive9", "promotion")}
	if len(synced) != len(want) {
		t.Fatalf("synced dirs = %v, want %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Fatalf("synced[%d] = %q, want %q", i, synced[i], want[i])
		}
	}
}

func TestSynchronousPromotionStopsBeforePublishWhenJournalParentSyncFails(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path == "/v1/fs:promotion" {
			publishCalls.Add(1)
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	injected := errors.New("injected parent fsync failure")
	testHookBeforePromotionDirSync = func(dir string) error {
		if dir == fs.opts.LocalRoot {
			return injected
		}
		return nil
	}
	t.Cleanup(func() { testHookBeforePromotionDirSync = nil })

	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status == gofuse.OK {
		t.Fatal("promotion succeeded despite journal parent fsync failure")
	}
	if publishCalls.Load() != 0 {
		t.Fatalf("publish calls = %d, want 0", publishCalls.Load())
	}
}

func TestSynchronousPromotionQuarantineDurabilityFailuresRecoverForward(t *testing.T) {
	for _, test := range []struct {
		name              string
		inject            func(fs *Dat9FS, dir string) bool
		wantSourceAtError bool
	}{
		{
			name: "quarantine_parent_after_rename",
			inject: func(fs *Dat9FS, dir string) bool {
				sourceAbs, err := fs.localOverlay.abs("/project/dist/.site-next")
				if err != nil {
					return false
				}
				_, sourceErr := os.Lstat(sourceAbs)
				return dir == filepath.Join(fs.promotionStateDir(), "quarantine") && os.IsNotExist(sourceErr)
			},
			wantSourceAtError: false,
		},
		{
			name: "source_parent_after_rename",
			inject: func(fs *Dat9FS, dir string) bool {
				sourceAbs, err := fs.localOverlay.abs("/project/dist/.site-next")
				return err == nil && dir == filepath.Dir(sourceAbs)
			},
			wantSourceAtError: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const operationID = "ignored-server-operation-id"
			var committed atomic.Pointer[promotion.Result]
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				switch r.Method {
				case http.MethodPost:
					var request promotion.PublishRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Fatal(err)
					}
					result := &promotion.Result{OperationID: request.OperationID, Target: request.Target, ManifestSHA256: request.ManifestSHA256, Committed: true}
					committed.Store(result)
					_ = json.NewEncoder(w).Encode(result)
				case http.MethodGet:
					result := committed.Load()
					if result == nil {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_ = json.NewEncoder(w).Encode(result)
				case http.MethodDelete:
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, operationID, http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			fs := newPromotionTestFS(t, server.URL)
			createPromotionTestTree(t, fs)
			injected := false
			t.Cleanup(func() { testHookBeforePromotionDirSync = nil })
			testHookBeforePromotionDirSync = func(dir string) error {
				if !injected && committed.Load() != nil && test.inject(fs, dir) {
					injected = true
					return errors.New("injected directory fsync failure")
				}
				return nil
			}
			status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
			if status != gofuse.EIO {
				t.Fatalf("promotion status = %v, want EIO", status)
			}
			if !injected {
				t.Fatal("durability failure was not injected")
			}
			_, sourceErr := fs.localOverlay.Lstat("/project/dist/.site-next")
			if got := sourceErr == nil; got != test.wantSourceAtError {
				t.Fatalf("source exists after failure = %v, want %v", got, test.wantSourceAtError)
			}
			record, err := fs.readPromotionRecord()
			if err != nil || record == nil || record.Phase != promotionPhaseCommitted {
				t.Fatalf("record after failure = %+v, %v; want committed", record, err)
			}

			testHookBeforePromotionDirSync = nil
			restarted := newPromotionTestFSWithRoot(t, server.URL, fs.opts.LocalRoot)
			if err := restarted.recoverSynchronousPromotion(context.Background()); err != nil {
				t.Fatalf("recover: %v", err)
			}
			waitForPromotionCleanup(t, restarted)
			if _, err := restarted.localOverlay.Lstat("/project/dist/.site-next"); !os.IsNotExist(err) {
				t.Fatalf("source after recovery: %v", err)
			}
			if _, err := os.Stat(restarted.promotionRecordPath()); !os.IsNotExist(err) {
				t.Fatalf("record after recovery: %v", err)
			}
		})
	}
}

func TestSynchronousPromotionKnownFailureCannotReplayWhenJournalRemoveFails(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:promotion" {
			publishCalls.Add(1)
			http.Error(w, "target changed", http.StatusConflict)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs:promotion" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.Error(w, "unexpected recovery request", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	testHookBeforePromotionRecordRemove = func() error { return errors.New("injected remove failure") }
	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.EIO {
		t.Fatalf("promotion status = %v, want EIO", status)
	}
	record, err := fs.readPromotionRecord()
	if err != nil {
		t.Fatal(err)
	}
	if record == nil || record.Phase != promotionPhaseAborted {
		t.Fatalf("record = %+v, want durable aborted phase", record)
	}
	testHookBeforePromotionRecordRemove = nil
	t.Cleanup(func() { testHookBeforePromotionRecordRemove = nil })

	restarted := newPromotionTestFSWithRoot(t, server.URL, fs.opts.LocalRoot)
	if err := restarted.recoverSynchronousPromotion(context.Background()); err != nil {
		t.Fatalf("recover aborted record: %v", err)
	}
	if publishCalls.Load() != 1 {
		t.Fatalf("publish calls after recovery = %d, want 1", publishCalls.Load())
	}
	if _, err := os.Stat(restarted.promotionRecordPath()); !os.IsNotExist(err) {
		t.Fatalf("aborted record after recovery: %v", err)
	}
}

func TestSynchronousPromotionAbortDirectorySyncFailureCannotReplayPreparedRecord(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path != "/v1/fs:promotion" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPost:
			publishCalls.Add(1)
			http.Error(w, "target changed", http.StatusConflict)
		default:
			http.Error(w, "unexpected recovery request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	var recordWrites atomic.Int32
	testHookBeforeAtomicWriteDirSync = func(recordPath string) error {
		if recordPath == fs.promotionRecordPath() && recordWrites.Add(1) == 2 {
			return errors.New("injected aborted-record directory fsync failure")
		}
		return nil
	}
	t.Cleanup(func() { testHookBeforeAtomicWriteDirSync = nil })

	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.EIO {
		t.Fatalf("promotion status = %v, want EIO", status)
	}
	stored, err := fs.readPromotionRecord()
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || stored.Phase != promotionPhaseAborted {
		t.Fatalf("live record = %+v, want renamed aborted phase", stored)
	}
	if publishCalls.Load() != 1 {
		t.Fatalf("publish calls before crash = %d, want 1", publishCalls.Load())
	}

	// Model the permitted crash outcome of the failed parent fsync: the older
	// durable prepared directory entry reappears instead of the renamed aborted
	// entry. Startup must query only; it must never re-POST this operation.
	testHookBeforeAtomicWriteDirSync = nil
	stored.Phase = promotionPhasePrepared
	if err := fs.writePromotionRecord(*stored); err != nil {
		t.Fatal(err)
	}
	restarted := newPromotionTestFSWithRoot(t, server.URL, fs.opts.LocalRoot)
	recoveryErr := restarted.recoverSynchronousPromotion(context.Background())
	if recoveryErr == nil {
		t.Fatal("recovery replayed or accepted prepared record without durable result")
	}
	var refusal *promotionRecoveryRefusal
	if !errors.As(recoveryErr, &refusal) {
		t.Fatalf("recovery error = %T %v, want permanent refusal", recoveryErr, recoveryErr)
	}
	if refusal.JournalPath != restarted.promotionRecordPath() || refusal.Phase != promotionPhasePrepared || refusal.OperationID != stored.OperationID {
		t.Fatalf("recovery refusal = %+v, want exact journal/phase/operation", refusal)
	}
	exitErr := promotionRecoveryMountExit(recoveryErr)
	if exitErr.Code != ExitStartupPermanent || exitErr.Reason != ExitReasonStartupPermanent {
		t.Fatalf("mount exit = %+v, want permanent startup", exitErr)
	}
	retryExit := promotionRecoveryMountExit(context.DeadlineExceeded)
	if retryExit.Code != ExitStartupTransient || retryExit.Reason != ExitReasonStartupTransient {
		t.Fatalf("retryable mount exit = %+v, want transient startup", retryExit)
	}
	if publishCalls.Load() != 1 {
		t.Fatalf("publish calls after crash recovery = %d, want 1", publishCalls.Load())
	}
	after, err := restarted.readPromotionRecord()
	if err != nil {
		t.Fatal(err)
	}
	if after == nil || after.Phase != promotionPhasePrepared || after.OperationID != stored.OperationID {
		t.Fatalf("journal after refused recovery = %+v, want unchanged prepared record", after)
	}
	if _, err := restarted.localOverlay.Lstat(stored.Source); err != nil {
		t.Fatalf("source changed during refused recovery: %v", err)
	}
}

func TestSynchronousPromotionRejectsUnknownJournalPhaseBeforeRemoteSideEffect(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*localPromotionRecord)
	}{
		{name: "missing_phase", mutate: func(record *localPromotionRecord) { record.Phase = "" }},
		{name: "unknown_phase", mutate: func(record *localPromotionRecord) { record.Phase = "publishing" }},
		{name: "noncanonical_phase", mutate: func(record *localPromotionRecord) { record.Phase = "PREPARED" }},
		{name: "released_operation_id_traversal", mutate: func(record *localPromotionRecord) {
			record.Phase = promotionPhaseReleased
			record.OperationID = "../../../overlay/project"
		}},
		{name: "committed_operation_id_traversal", mutate: func(record *localPromotionRecord) {
			record.Phase = promotionPhaseCommitted
			record.OperationID = "../../../overlay/project"
		}},
		{name: "noncanonical_source", mutate: func(record *localPromotionRecord) { record.Source = "/project/dist/../site-next" }},
		{name: "relative_local_target", mutate: func(record *localPromotionRecord) { record.TargetLocal = "project/site" }},
		{name: "remote_target_mismatch", mutate: func(record *localPromotionRecord) { record.TargetRemote = "/other/site/" }},
		{name: "noncanonical_digest", mutate: func(record *localPromotionRecord) { record.ManifestSHA256 = strings.Repeat("A", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var remoteCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				remoteCalls.Add(1)
				http.Error(w, "unexpected", http.StatusInternalServerError)
			}))
			defer server.Close()

			fs := newPromotionTestFS(t, server.URL)
			createPromotionTestTree(t, fs)
			record := localPromotionRecord{
				OperationID: "01234567-89ab-cdef-0123-456789abcdef", Source: "/project/dist/.site-next",
				TargetLocal: "/project/site", TargetRemote: "/project/site/",
				RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(),
				ManifestSHA256:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Phase: promotionPhasePrepared,
			}
			test.mutate(&record)
			if err := ensurePromotionDirDurable(fs.promotionStateDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := atomicWrite(fs.promotionRecordPath(), body); err != nil {
				t.Fatal(err)
			}

			recoveryErr := fs.recoverSynchronousPromotion(context.Background())
			if recoveryErr == nil {
				t.Fatal("recovery accepted invalid journal phase")
			}
			var refusal *promotionRecoveryRefusal
			if !errors.As(recoveryErr, &refusal) {
				t.Fatalf("recovery error = %T %v, want permanent refusal", recoveryErr, recoveryErr)
			}
			if refusal.JournalPath != fs.promotionRecordPath() || refusal.Phase != "unknown" || refusal.OperationID != "unknown" {
				t.Fatalf("recovery refusal = %+v, want actionable journal path with untrusted identifiers", refusal)
			}
			if exitErr := promotionRecoveryMountExit(recoveryErr); exitErr.Code != ExitStartupPermanent {
				t.Fatalf("mount exit = %+v, want permanent startup", exitErr)
			}
			if remoteCalls.Load() != 0 {
				t.Fatalf("remote calls = %d, want 0", remoteCalls.Load())
			}
			if _, err := fs.localOverlay.Lstat("/project/dist/.site-next/assets/app.js"); err != nil {
				t.Fatalf("invalid journal changed local source: %v", err)
			}
		})
	}
}

func TestSynchronousPromotionRejectsSymlinkedQuarantineBeforeAnySideEffect(t *testing.T) {
	for _, phase := range []string{promotionPhaseReleased, promotionPhaseCommitted} {
		t.Run(phase, func(t *testing.T) {
			var remoteCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				remoteCalls.Add(1)
				http.Error(w, "unexpected", http.StatusInternalServerError)
			}))
			defer server.Close()

			fs := newPromotionTestFS(t, server.URL)
			createPromotionTestTree(t, fs)
			const operationID = "01234567-89ab-cdef-0123-456789abcdef"
			record := localPromotionRecord{
				OperationID: operationID, RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(),
				Source: "/project/dist/.site-next", TargetLocal: "/project/site", TargetRemote: "/project/site/",
				ManifestSHA256: strings.Repeat("a", 64), Phase: phase,
			}
			if err := fs.writePromotionRecord(record); err != nil {
				t.Fatal(err)
			}

			victimRoot := t.TempDir()
			victim := filepath.Join(victimRoot, operationID)
			if err := os.Mkdir(victim, 0o755); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(victim, "sentinel")
			if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(victimRoot, filepath.Join(fs.promotionStateDir(), "quarantine")); err != nil {
				t.Fatal(err)
			}

			if err := fs.recoverSynchronousPromotion(context.Background()); err == nil {
				t.Fatal("recovery accepted symlinked quarantine root")
			}
			if remoteCalls.Load() != 0 {
				t.Fatalf("remote calls = %d, want 0", remoteCalls.Load())
			}
			if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
				t.Fatalf("external victim changed: data=%q err=%v", data, err)
			}
			if _, err := fs.localOverlay.Lstat(record.Source); err != nil {
				t.Fatalf("local source changed: %v", err)
			}
		})
	}
}

func TestSynchronousPromotionRecoveryRejectsSymlinkedOverlayBeforeAnySideEffect(t *testing.T) {
	var remoteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		remoteCalls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	record := localPromotionRecord{
		OperationID: "01234567-89ab-cdef-0123-456789abcdef", RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(),
		Source: "/project/dist/.site-next", TargetLocal: "/project/site", TargetRemote: "/project/site/",
		ManifestSHA256: strings.Repeat("a", 64), Phase: promotionPhaseCommitted,
	}
	if err := fs.writePromotionRecord(record); err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(fs.opts.LocalRoot, "overlay")
	if err := os.Rename(overlay, overlay+".saved"); err != nil {
		t.Fatal(err)
	}
	victimRoot := t.TempDir()
	sentinel := filepath.Join(victimRoot, "project", "dist", ".site-next", "sentinel")
	if err := os.MkdirAll(filepath.Dir(sentinel), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victimRoot, overlay); err != nil {
		t.Fatal(err)
	}

	if err := fs.recoverSynchronousPromotion(context.Background()); err == nil {
		t.Fatal("recovery accepted symlinked overlay")
	}
	if remoteCalls.Load() != 0 {
		t.Fatalf("remote calls = %d, want 0", remoteCalls.Load())
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
		t.Fatalf("external victim changed: data=%q err=%v", data, err)
	}
}

func TestPromotionHTTPUsesClientActorAndRedirectCredentialPolicy(t *testing.T) {
	const actor = "promotion-mount-a"
	var sourceActor string
	var destinationAuth, destinationActor string
	result := promotion.Result{
		OperationID: "01234567-89ab-cdef-0123-456789abcdef", Target: "/project/site/",
		ManifestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Committed: true,
	}
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destinationAuth = r.Header.Get("Authorization")
		destinationActor = r.Header.Get("X-Dat9-Actor")
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceActor = r.Header.Get("X-Dat9-Actor")
		http.Redirect(w, r, destination.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	c := client.New(source.URL, "owner-secret")
	c.SetActor(actor)
	opts := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: t.TempDir(), EnableSynchronousPromotion: true, actorID: actor}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)
	got, err := fs.getPromotionResult(context.Background(), result.OperationID, result.Target)
	if err != nil {
		t.Fatal(err)
	}
	if got.OperationID != result.OperationID {
		t.Fatalf("operation ID = %q, want %q", got.OperationID, result.OperationID)
	}
	if sourceActor != actor {
		t.Fatalf("source actor = %q, want %q", sourceActor, actor)
	}
	if destinationAuth != "" || destinationActor != "" {
		t.Fatalf("redirect leaked credentials: authorization=%q actor=%q", destinationAuth, destinationActor)
	}
}

func TestPromotionHTTPKeepsRequestContextAliveWhileReadingBody(t *testing.T) {
	result := promotion.Result{
		OperationID: "01234567-89ab-cdef-0123-456789abcdef", Target: "/project/site/",
		ManifestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Committed: true,
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	got, err := fs.getPromotionResult(context.Background(), result.OperationID, result.Target)
	if err != nil {
		t.Fatalf("get delayed promotion result: %v", err)
	}
	if *got != result {
		t.Fatalf("result = %+v, want %+v", got, result)
	}
}

func TestSynchronousPromotionRejectsSpecialPermissionBitsBeforePublish(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		publishCalls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	special, err := fs.localOverlay.abs("/project/dist/.site-next/assets")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(special, 0o755|os.ModeSticky); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(special)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSticky == 0 {
		t.Skip("filesystem does not preserve sticky bit")
	}

	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("promotion status = %v, want EXDEV", status)
	}
	if publishCalls.Load() != 0 {
		t.Fatalf("publish calls = %d, want 0", publishCalls.Load())
	}
}

func TestSynchronousPromotionRejectsUnsupportedSourceShapesBeforePublish(t *testing.T) {
	type sourceMutation func(t *testing.T, fs *Dat9FS, assetsAbs, appAbs string)
	tests := []struct {
		name   string
		mutate sourceMutation
	}{
		{
			name: "hard_link",
			mutate: func(t *testing.T, _ *Dat9FS, assetsAbs, appAbs string) {
				t.Helper()
				if err := os.Link(appAbs, filepath.Join(assetsAbs, "app-copy.js")); err != nil {
					t.Fatalf("create hard link: %v", err)
				}
			},
		},
		{
			name: "sparse_file",
			mutate: func(t *testing.T, _ *Dat9FS, assetsAbs, _ string) {
				t.Helper()
				file, err := os.OpenFile(filepath.Join(assetsAbs, "sparse.bin"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
				if err != nil {
					t.Fatal(err)
				}
				if err := file.Truncate(64 << 20); err != nil {
					_ = file.Close()
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(filepath.Join(assetsAbs, "sparse.bin"))
				if err != nil {
					t.Fatal(err)
				}
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok || stat.Blocks*512 >= info.Size() {
					t.Skip("filesystem did not create a sparse fixture")
				}
			},
		},
		{
			name: "os_xattr",
			mutate: func(t *testing.T, _ *Dat9FS, _ string, appAbs string) {
				t.Helper()
				name := "user.drive9-promotion-test"
				if runtime.GOOS == "darwin" {
					name = "com.drive9.promotion-test"
				}
				if err := unix.Setxattr(appAbs, name, []byte("present"), 0); err != nil {
					t.Skipf("cannot create xattr fixture: %v", err)
				}
			},
		},
		{
			name: "symlink",
			mutate: func(t *testing.T, _ *Dat9FS, assetsAbs, _ string) {
				t.Helper()
				if err := os.Symlink("app.js", filepath.Join(assetsAbs, "app-link.js")); err != nil {
					t.Fatalf("create symlink: %v", err)
				}
			},
		},
		{
			name: "special_file",
			mutate: func(t *testing.T, _ *Dat9FS, assetsAbs, _ string) {
				t.Helper()
				if err := unix.Mkfifo(filepath.Join(assetsAbs, "events.fifo"), 0o600); err != nil {
					t.Skipf("cannot create fifo fixture: %v", err)
				}
			},
		},
		{
			name: "backing_foreign_owner",
			mutate: func(t *testing.T, fs *Dat9FS, _ string, appAbs string) {
				t.Helper()
				uid, gid := int(fs.uid)+1, int(fs.gid)+1
				if err := os.Chown(appAbs, uid, gid); err != nil {
					t.Skipf("cannot create foreign-owner fixture: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var publishCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:promotion" {
					publishCalls.Add(1)
				}
				http.Error(w, "unexpected", http.StatusInternalServerError)
			}))
			defer server.Close()

			fs := newPromotionTestFS(t, server.URL)
			createPromotionTestTree(t, fs)
			assetsAbs, err := fs.localOverlay.abs("/project/dist/.site-next/assets")
			if err != nil {
				t.Fatal(err)
			}
			appAbs := filepath.Join(assetsAbs, "app.js")
			test.mutate(t, fs, assetsAbs, appAbs)

			assertPromotionRejectedWithoutRemoteEffect(t, fs, &publishCalls)
		})
	}
}

func TestSynchronousPromotionRejectsFUSEVisibleForeignOwnerBeforePublish(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:promotion" {
			publishCalls.Add(1)
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	localPath := "/project/dist/.site-next/assets/app.js"
	info, err := fs.localOverlay.Lstat(localPath)
	if err != nil {
		t.Fatal(err)
	}
	ino := fs.inodes.Lookup(localPath, false, info.Size(), info.ModTime())
	wantedUID, wantedGID := fs.uid+1, fs.gid+1
	var out gofuse.AttrOut
	status := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 0, Gid: 0}}},
		Valid:    gofuse.FATTR_UID | gofuse.FATTR_GID,
		Owner:    gofuse.Owner{Uid: wantedUID, Gid: wantedGID},
	}}, &out)
	if status != gofuse.OK {
		t.Skipf("cannot create FUSE-visible foreign-owner fixture: %v", status)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || !entry.HasUID || !entry.HasGID || entry.Uid != wantedUID || entry.Gid != wantedGID {
		t.Fatalf("visible owner = %+v, want %d:%d", entry, wantedUID, wantedGID)
	}
	backingInfo, err := fs.localOverlay.Lstat(localPath)
	if err != nil {
		t.Fatal(err)
	}
	backingStat, ok := backingInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("missing backing stat metadata")
	}
	if backingStat.Uid != fs.uid || backingStat.Gid != fs.gid {
		t.Fatalf("backing owner unexpectedly changed to %d:%d", backingStat.Uid, backingStat.Gid)
	}

	assertPromotionRejectedWithoutRemoteEffect(t, fs, &publishCalls)
}

func TestSynchronousPromotionRejectsSymlinkedOverlayBeforePublish(t *testing.T) {
	var publishCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:promotion" {
			publishCalls.Add(1)
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer server.Close()

	localRoot := t.TempDir()
	externalOverlay := t.TempDir()
	if err := os.Symlink(externalOverlay, filepath.Join(localRoot, "overlay")); err != nil {
		t.Fatalf("create overlay symlink: %v", err)
	}
	fs := newPromotionTestFSWithRoot(t, server.URL, localRoot)
	createPromotionTestTree(t, fs)
	assertPromotionRejectedWithoutRemoteEffect(t, fs, &publishCalls)
	if _, err := os.Stat(filepath.Join(externalOverlay, "project", "dist", ".site-next", "assets", "app.js")); err != nil {
		t.Fatalf("external source changed after rejection: %v", err)
	}
}

func assertPromotionRejectedWithoutRemoteEffect(t *testing.T, fs *Dat9FS, publishCalls *atomic.Int32) {
	t.Helper()
	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("promotion status = %v, want EXDEV", status)
	}
	if publishCalls.Load() != 0 {
		t.Fatalf("publish calls = %d, want 0", publishCalls.Load())
	}
	if _, err := fs.localOverlay.Lstat("/project/dist/.site-next/assets/app.js"); err != nil {
		t.Fatalf("source changed after rejection: %v", err)
	}
	if _, err := fs.localOverlay.Lstat("/project/site"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target changed after rejection: %v", err)
	}
}

func TestSynchronousPromotionRejectsNonCanonicalSourceNamesBeforePublish(t *testing.T) {
	for _, name := range []string{`a\b.js`, "cafe\u0301.js"} {
		t.Run(name, func(t *testing.T) {
			var publishCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if r.Method == http.MethodPost && r.URL.Path == "/v1/fs:promotion" {
					publishCalls.Add(1)
				}
				http.Error(w, "unexpected", http.StatusInternalServerError)
			}))
			defer server.Close()

			fs := newPromotionTestFS(t, server.URL)
			createPromotionTestTree(t, fs)
			assets, err := fs.localOverlay.abs("/project/dist/.site-next/assets")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(assets, name), []byte("invalid path"), 0o644); err != nil {
				t.Fatal(err)
			}

			status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
			if status != gofuse.Status(syscall.EXDEV) {
				t.Fatalf("promotion status = %v, want EXDEV", status)
			}
			if publishCalls.Load() != 0 {
				t.Fatalf("publish calls = %d, want 0", publishCalls.Load())
			}
			if _, err := os.Lstat(filepath.Join(assets, name)); err != nil {
				t.Fatalf("source changed after rejection: %v", err)
			}
		})
	}
}

func TestSynchronousPromotionQuotaRejectionReturnsENOSPCWithoutBlockingMount(t *testing.T) {
	var publishCalls, getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path != "/v1/fs:promotion" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodPost:
			publishCalls.Add(1)
			http.Error(w, "quota exceeded", http.StatusInsufficientStorage)
		case http.MethodGet:
			getCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	status := fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	if status != gofuse.Status(syscall.ENOSPC) {
		t.Fatalf("promotion status = %v, want ENOSPC", status)
	}
	if publishCalls.Load() != 1 || getCalls.Load() != 1 {
		t.Fatalf("calls publish=%d get=%d, want 1/1", publishCalls.Load(), getCalls.Load())
	}
	if _, err := fs.localOverlay.Lstat("/project/dist/.site-next/assets/app.js"); err != nil {
		t.Fatalf("source changed after quota rejection: %v", err)
	}
	if _, err := os.Stat(fs.promotionRecordPath()); !os.IsNotExist(err) {
		t.Fatalf("journal after quota rejection: %v", err)
	}
	if fs.promotionBlocked.Load() {
		t.Fatal("quota rejection blocked subsequent mount mutation")
	}
	unlock, ok := fs.lockPromotionMutation()
	if !ok {
		t.Fatal("ordinary mutation lock refused after quota rejection")
	}
	unlock()
}

func TestSynchronousPromotionReturnsBeforeAckAndRestartsReleasedCleanup(t *testing.T) {
	var acknowledgeCalls atomic.Int32
	acknowledgeStarted := make(chan struct{})
	releaseFirstAcknowledge := make(chan struct{})
	firstAcknowledgeFinished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path != "/v1/fs:promotion" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodPost:
			var request promotion.PublishRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(promotion.Result{
				OperationID: request.OperationID, Target: request.Target,
				ManifestSHA256: request.ManifestSHA256, Committed: true,
			})
		case http.MethodDelete:
			if acknowledgeCalls.Add(1) == 1 {
				close(acknowledgeStarted)
				<-releaseFirstAcknowledge
				http.Error(w, "injected cleanup outage", http.StatusServiceUnavailable)
				close(firstAcknowledgeFinished)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		select {
		case <-releaseFirstAcknowledge:
		default:
			close(releaseFirstAcknowledge)
		}
	})

	fs := newPromotionTestFS(t, server.URL)
	fs.client.SetSmallFileThresholdForTests(50_000)
	createPromotionTestTree(t, fs)
	statusResult := make(chan gofuse.Status, 1)
	go func() {
		statusResult <- fs.promoteLocalTree(context.Background(), &gofuse.RenameIn{}, "/project/dist/.site-next", "/project/site")
	}()
	var status gofuse.Status
	select {
	case status = <-statusResult:
	case <-time.After(5 * time.Second):
		t.Fatal("rename waited for background acknowledgement")
	}
	if status != gofuse.OK {
		t.Fatalf("promotion status = %v, want OK", status)
	}
	select {
	case <-acknowledgeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("ack request did not start")
	}
	unlock, ok := fs.lockPromotionMutation()
	if !ok {
		t.Fatal("background acknowledgement blocked ordinary mount mutation")
	}
	unlock()
	record, err := fs.readPromotionRecord()
	if err != nil || record == nil || record.Phase != promotionPhaseReleased {
		t.Fatalf("record after ack stall = %+v, %v; want released", record, err)
	}
	qPath, err := fs.promotionQuarantinePath(record.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(qPath); err != nil {
		t.Fatalf("released quarantine after ack stall: %v", err)
	}
	if _, err := fs.localOverlay.Lstat(record.Source); !os.IsNotExist(err) {
		t.Fatalf("source after ack stall: %v", err)
	}
	// The returned rename lets the user reuse the old source name immediately.
	// Cleanup must only touch the operation-ID quarantine, never this new tree.
	if err := fs.localOverlay.Mkdir(record.Source, 0o755); err != nil {
		t.Fatal(err)
	}
	newFile, err := fs.localOverlay.OpenFile(record.Source+"/new.txt", uint32(os.O_CREATE|os.O_WRONLY), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newFile.WriteString("new user data"); err != nil {
		t.Fatal(err)
	}
	if err := newFile.Close(); err != nil {
		t.Fatal(err)
	}
	close(releaseFirstAcknowledge)
	select {
	case <-firstAcknowledgeFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("first background acknowledgement did not finish")
	}

	restarted := newPromotionTestFSWithRoot(t, server.URL, fs.opts.LocalRoot)
	if err := restarted.recoverSynchronousPromotion(context.Background()); err != nil {
		t.Fatalf("restart cleanup: %v", err)
	}
	waitForPromotionCleanup(t, restarted)
	if acknowledgeCalls.Load() != 2 {
		t.Fatalf("ack calls = %d, want 2", acknowledgeCalls.Load())
	}
	data, err := os.ReadFile(filepath.Join(restarted.opts.LocalRoot, "overlay", "project", "dist", ".site-next", "new.txt"))
	if err != nil || string(data) != "new user data" {
		t.Fatalf("recreated source changed by cleanup: data=%q err=%v", data, err)
	}
}

func TestSynchronousPromotionStartupRecoveryFinishesCommittedOperation(t *testing.T) {
	const operationID = "01234567-89ab-cdef-0123-456789abcdef"
	var acknowledgeCalls atomic.Int32
	result := promotion.Result{
		OperationID: operationID, Target: "/project/site/", Committed: true,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs:promotion" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(result)
		case http.MethodDelete:
			acknowledgeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	opts := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: t.TempDir(), EnableSynchronousPromotion: true}
	opts.setDefaults()
	fs := NewDat9FS(client.New(server.URL, ""), opts)
	if err := fs.localOverlay.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	if err := fs.localOverlay.Mkdir("/project/dist/.site-next", 0o755); err != nil {
		t.Fatal(err)
	}
	request, err := fs.scanPromotionTree(context.Background(), "/project/dist/.site-next", "/project/site", result.Target, operationID)
	if err != nil {
		t.Fatal(err)
	}
	result.ManifestSHA256 = request.ManifestSHA256
	record := localPromotionRecord{
		OperationID: operationID, Source: "/project/dist/.site-next", TargetLocal: "/project/site",
		RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(), TargetRemote: result.Target,
		ManifestSHA256: result.ManifestSHA256, Phase: promotionPhasePrepared,
	}
	if err := fs.writePromotionRecord(record); err != nil {
		t.Fatal(err)
	}

	if err := fs.recoverSynchronousPromotion(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	waitForPromotionCleanup(t, fs)
	if acknowledgeCalls.Load() != 1 {
		t.Fatalf("acknowledge calls = %d, want 1", acknowledgeCalls.Load())
	}
	if _, err := fs.localOverlay.Lstat(record.Source); !os.IsNotExist(err) {
		t.Fatalf("source after recovery: %v", err)
	}
	if _, err := os.Stat(fs.promotionRecordPath()); !os.IsNotExist(err) {
		t.Fatalf("journal after recovery: %v", err)
	}
}

func TestSynchronousPromotionRecoveryRejectsDifferentRemoteIdentityBeforeRequest(t *testing.T) {
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer serverA.Close()
	var serverBCalls atomic.Int32
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverBCalls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer serverB.Close()

	root := t.TempDir()
	optsA := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: root, EnableSynchronousPromotion: true}
	optsA.setDefaults()
	fsA := NewDat9FS(client.New(serverA.URL, "tenant-a"), optsA)
	if err := fsA.localOverlay.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	createPromotionTestTree(t, fsA)
	record := localPromotionRecord{
		OperationID:          "01234567-89ab-cdef-0123-456789abcdef",
		RemoteIdentitySHA256: fsA.promotionRemoteIdentitySHA256(),
		Source:               "/project/dist/.site-next",
		TargetLocal:          "/project/site",
		TargetRemote:         "/project/site/",
		ManifestSHA256:       strings.Repeat("a", 64),
		Phase:                promotionPhasePrepared,
	}
	if err := fsA.writePromotionRecord(record); err != nil {
		t.Fatal(err)
	}

	optsB := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: root}
	optsB.setDefaults()
	fsB := NewDat9FS(client.New(serverB.URL, "tenant-b"), optsB)
	if err := fsB.recoverSynchronousPromotion(context.Background()); err == nil {
		t.Fatal("recovery accepted a journal from another remote identity")
	}
	if serverBCalls.Load() != 0 {
		t.Fatalf("new remote calls = %d, want 0", serverBCalls.Load())
	}
	if _, err := fsB.localOverlay.Lstat("/project/dist/.site-next/assets/app.js"); err != nil {
		t.Fatalf("identity mismatch changed local source: %v", err)
	}
}

func TestSynchronousPromotionGateOffStillRecoversPendingJournal(t *testing.T) {
	const operationID = "01234567-89ab-cdef-0123-456789abcdef"
	var getCalls, acknowledgeCalls atomic.Int32
	result := promotion.Result{OperationID: operationID, Target: "/project/site/", Committed: true}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls.Add(1)
			_ = json.NewEncoder(w).Encode(result)
		case http.MethodDelete:
			acknowledgeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	root := t.TempDir()
	enabled := newPromotionTestFSWithRoot(t, server.URL, root)
	if err := enabled.localOverlay.Mkdir("/project/dist/.site-next", 0o755); err != nil {
		t.Fatal(err)
	}
	request, err := enabled.scanPromotionTree(context.Background(), "/project/dist/.site-next", "/project/site", result.Target, operationID)
	if err != nil {
		t.Fatal(err)
	}
	result.ManifestSHA256 = request.ManifestSHA256
	record := localPromotionRecord{
		OperationID: operationID, RemoteIdentitySHA256: enabled.promotionRemoteIdentitySHA256(),
		Source: "/project/dist/.site-next", TargetLocal: "/project/site", TargetRemote: "/project/site/",
		ManifestSHA256: result.ManifestSHA256, Phase: promotionPhasePrepared,
	}
	if err := enabled.writePromotionRecord(record); err != nil {
		t.Fatal(err)
	}

	disabledOpts := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: root}
	disabledOpts.setDefaults()
	disabled := NewDat9FS(client.New(server.URL, ""), disabledOpts)
	if err := disabled.recoverSynchronousPromotion(context.Background()); err != nil {
		t.Fatalf("gate-off recovery: %v", err)
	}
	waitForPromotionCleanup(t, disabled)
	if getCalls.Load() != 1 || acknowledgeCalls.Load() != 1 {
		t.Fatalf("calls get=%d ack=%d, want 1/1", getCalls.Load(), acknowledgeCalls.Load())
	}
	if _, err := disabled.localOverlay.Lstat(record.Source); !os.IsNotExist(err) {
		t.Fatalf("source after gate-off recovery: %v", err)
	}
}

func TestSynchronousPromotionRecoveryConvergesDurableDualName(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		name := "identical"
		if mismatch {
			name = "mismatch"
		}
		t.Run(name, func(t *testing.T) {
			var result atomic.Pointer[promotion.Result]
			var acknowledgeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					stored := result.Load()
					if stored == nil {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_ = json.NewEncoder(w).Encode(stored)
				case http.MethodDelete:
					acknowledgeCalls.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, "unexpected", http.StatusInternalServerError)
				}
			}))
			defer server.Close()

			fs := newPromotionTestFS(t, server.URL)
			createPromotionTestTree(t, fs)
			const operationID = "01234567-89ab-cdef-0123-456789abcdef"
			request, err := fs.scanPromotionTree(context.Background(), "/project/dist/.site-next", "/project/site", "/project/site/", operationID)
			if err != nil {
				t.Fatal(err)
			}
			result.Store(&promotion.Result{
				OperationID: operationID, Target: request.Target,
				ManifestSHA256: request.ManifestSHA256, Committed: true,
			})
			sourceAbs, err := fs.localOverlay.abs("/project/dist/.site-next")
			if err != nil {
				t.Fatal(err)
			}
			qPath, err := fs.promotionQuarantinePath(operationID)
			if err != nil {
				t.Fatal(err)
			}
			if err := ensurePromotionDirDurable(filepath.Dir(qPath), 0o700); err != nil {
				t.Fatal(err)
			}
			copyPromotionTestTree(t, sourceAbs, qPath)
			if mismatch {
				if err := os.WriteFile(filepath.Join(qPath, "assets", "app.js"), []byte("different"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			record := localPromotionRecord{
				OperationID: operationID, RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(),
				Source: "/project/dist/.site-next", TargetLocal: "/project/site", TargetRemote: request.Target,
				ManifestSHA256: request.ManifestSHA256, Phase: promotionPhaseCommitted,
			}
			if err := fs.writePromotionRecord(record); err != nil {
				t.Fatal(err)
			}

			err = fs.recoverSynchronousPromotion(context.Background())
			if mismatch {
				if err == nil {
					t.Fatal("recovery removed mismatched dual-name state")
				}
				if _, statErr := os.Stat(sourceAbs); statErr != nil {
					t.Fatalf("source not preserved: %v", statErr)
				}
				if _, statErr := os.Stat(qPath); statErr != nil {
					t.Fatalf("quarantine not preserved: %v", statErr)
				}
				if acknowledgeCalls.Load() != 0 {
					t.Fatalf("ack calls = %d, want 0", acknowledgeCalls.Load())
				}
				return
			}
			if err != nil {
				t.Fatalf("recover identical dual-name state: %v", err)
			}
			waitForPromotionCleanup(t, fs)
			if _, statErr := os.Stat(sourceAbs); !os.IsNotExist(statErr) {
				t.Fatalf("source after recovery: %v", statErr)
			}
			if _, statErr := os.Stat(qPath); !os.IsNotExist(statErr) {
				t.Fatalf("quarantine after recovery: %v", statErr)
			}
			if acknowledgeCalls.Load() != 1 {
				t.Fatalf("ack calls = %d, want 1", acknowledgeCalls.Load())
			}
		})
	}
}

func TestSynchronousPromotionRecoveryRejectsChangedSourceBeforeQuarantine(t *testing.T) {
	var result atomic.Pointer[promotion.Result]
	var acknowledgeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(result.Load())
		case http.MethodDelete:
			acknowledgeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	createPromotionTestTree(t, fs)
	const operationID = "abcdef12-3456-7890-abcd-ef1234567890"
	request, err := fs.scanPromotionTree(context.Background(), "/project/dist/.site-next", "/project/site", "/project/site/", operationID)
	if err != nil {
		t.Fatal(err)
	}
	result.Store(&promotion.Result{
		OperationID: operationID, Target: request.Target,
		ManifestSHA256: request.ManifestSHA256, Committed: true,
	})
	record := localPromotionRecord{
		OperationID: operationID, RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(),
		Source: "/project/dist/.site-next", TargetLocal: "/project/site", TargetRemote: request.Target,
		ManifestSHA256: request.ManifestSHA256, Phase: promotionPhaseCommitted,
	}
	if err := fs.writePromotionRecord(record); err != nil {
		t.Fatal(err)
	}
	sourceAbs, err := fs.localOverlay.abs(record.Source)
	if err != nil {
		t.Fatal(err)
	}
	changedPath := filepath.Join(sourceAbs, "assets", "app.js")
	if err := os.WriteFile(changedPath, []byte("changed after commit"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := fs.recoverSynchronousPromotion(context.Background()); err == nil {
		t.Fatal("recovery quarantined source changed after commit")
	}
	data, err := os.ReadFile(changedPath)
	if err != nil || string(data) != "changed after commit" {
		t.Fatalf("changed source was not preserved: data=%q err=%v", data, err)
	}
	qPath, err := fs.promotionQuarantinePath(operationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(qPath); !os.IsNotExist(err) {
		t.Fatalf("quarantine created for changed source: %v", err)
	}
	if acknowledgeCalls.Load() != 0 {
		t.Fatalf("acknowledge calls = %d, want 0", acknowledgeCalls.Load())
	}
}

func TestPromotionRequestDefinitelyRejectedTreatsProxyTimeoutsAsUnknown(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests} {
		err := &client.StatusError{StatusCode: status, Message: "proxy response"}
		if promotionRequestDefinitelyRejected(err) {
			t.Fatalf("status %d classified as definitive rejection", status)
		}
	}
	if !promotionRequestDefinitelyRejected(&client.StatusError{StatusCode: http.StatusConflict, Message: "target exists"}) {
		t.Fatal("conflict should be a definitive rejection after receipt lookup returned not found")
	}
}

func TestPromotionOutcomeUnknownIsStickyAcrossRetries(t *testing.T) {
	var postCalls, getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			call := postCalls.Add(1)
			if call == 1 {
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
				return
			}
			http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		case http.MethodGet:
			getCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	request := testPromotionRequest()
	_, definitive, err := fs.publishPromotionWithRecovery(context.Background(), request)
	if err == nil || definitive {
		t.Fatalf("publish result definitive=%v err=%v, want sticky outcome-unknown", definitive, err)
	}
	if postCalls.Load() != 3 || getCalls.Load() < 3 {
		t.Fatalf("calls post=%d get=%d, want 3/>=3", postCalls.Load(), getCalls.Load())
	}
}

func TestPromotionDefiniteRejectionSurvivesReceiptLookupFailure(t *testing.T) {
	var postCalls, getCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postCalls.Add(1)
			http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		case http.MethodGet:
			getCalls.Add(1)
			panic(http.ErrAbortHandler)
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	fs := newPromotionTestFS(t, server.URL)
	_, definitive, err := fs.publishPromotionWithRecovery(context.Background(), testPromotionRequest())
	if err == nil || !definitive {
		t.Fatalf("publish result definitive=%v err=%v, want definite POST rejection", definitive, err)
	}
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("publish error = %T %v, want original 413", err, err)
	}
	if postCalls.Load() != 1 || getCalls.Load() < 1 {
		t.Fatalf("calls post=%d get=%d, want 1/>=1", postCalls.Load(), getCalls.Load())
	}
}

func testPromotionRequest() promotion.PublishRequest {
	entries := []promotion.Entry{{RelativePath: ".", Type: promotion.EntryDirectory, Mode: 0o755}}
	return promotion.PublishRequest{
		OperationID:    "01234567-89ab-cdef-0123-456789abcdef",
		Target:         "/project/site/",
		ManifestSHA256: promotion.ManifestSHA256(entries),
		Entries:        entries,
	}
}

func newPromotionTestFS(t *testing.T, serverURL string) *Dat9FS {
	t.Helper()
	return newPromotionTestFSWithRoot(t, serverURL, t.TempDir())
}

func newPromotionTestFSWithRoot(t *testing.T, serverURL, localRoot string) *Dat9FS {
	t.Helper()
	opts := &MountOptions{CacheSize: 1 << 20, Profile: MountProfileCodingAgent, LocalRoot: localRoot, EnableSynchronousPromotion: true}
	opts.setDefaults()
	fs := NewDat9FS(client.New(serverURL, ""), opts)
	if err := fs.localOverlay.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	return fs
}

func createPromotionTestTree(t *testing.T, fs *Dat9FS) {
	t.Helper()
	if err := fs.localOverlay.Mkdir("/project/dist/.site-next", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := fs.localOverlay.Mkdir("/project/dist/.site-next/assets", 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := fs.localOverlay.OpenFile("/project/dist/.site-next/assets/app.js", uint32(os.O_CREATE|os.O_WRONLY), 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("console.log('ok')"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitForPromotionCleanup(t *testing.T, fs *Dat9FS) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Lstat(fs.promotionRecordPath())
		if os.IsNotExist(err) {
			// The cleanup removes the journal before its final directory fsync.
			// Wait for its lifecycle critical section to finish so tests may
			// safely restore process-wide durability failpoints.
			fs.promotionLifecycleMu.Lock()
			_, lockedErr := os.Lstat(fs.promotionRecordPath())
			fs.promotionLifecycleMu.Unlock()
			if os.IsNotExist(lockedErr) {
				return
			}
			if lockedErr != nil {
				t.Fatalf("stat promotion journal after cleanup synchronization: %v", lockedErr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("stat promotion journal: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("promotion cleanup did not remove journal %s", fs.promotionRecordPath())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func copyPromotionTestTree(t *testing.T, sourceRoot, destinationRoot string) {
	t.Helper()
	type directoryMetadata struct {
		path  string
		mode  os.FileMode
		mtime time.Time
	}
	var directories []directoryMetadata
	err := filepath.WalkDir(sourceRoot, func(sourcePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(sourceRoot, sourcePath)
		if err != nil {
			return err
		}
		destinationPath := filepath.Join(destinationRoot, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if err := os.MkdirAll(destinationPath, info.Mode().Perm()); err != nil {
				return err
			}
			directories = append(directories, directoryMetadata{path: destinationPath, mode: info.Mode().Perm(), mtime: info.ModTime()})
			return nil
		}
		data, err := os.ReadFile(sourcePath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(destinationPath, data, info.Mode().Perm()); err != nil {
			return err
		}
		return os.Chtimes(destinationPath, info.ModTime(), info.ModTime())
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := os.Chmod(directories[i].path, directories[i].mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(directories[i].path, directories[i].mtime, directories[i].mtime); err != nil {
			t.Fatal(err)
		}
	}
}
