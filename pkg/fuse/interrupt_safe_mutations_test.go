package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newInterruptMutationTestFS(t *testing.T, legacyInterruptible bool) *Dat9FS {
	t.Helper()
	opts := &MountOptions{LegacyInterruptibleMutations: legacyInterruptible}
	opts.setDefaults()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Success for every mutation; probe/read routes are irrelevant for
		// these tests because the commit must succeed on the first attempt.
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)
	return NewDat9FS(newTestClient(ts.URL), opts)
}

func canceledCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, cancel
}

func TestInterruptSafeCommitContextPolicy(t *testing.T) {
	canceledCtx, cancel := canceledCtx()
	defer cancel()

	fs := newInterruptMutationTestFS(t, false)
	commitCtx, commitCancel := fs.namespaceMutationCommitContext(canceledCtx)
	defer commitCancel()
	if err := commitCtx.Err(); err != nil {
		t.Fatalf("interrupt-safe commit ctx err = %v, want detached from parent cancel", err)
	}
	deadline, ok := commitCtx.Deadline()
	if !ok {
		t.Fatal("interrupt-safe commit ctx has no deadline")
	}
	// Detachment must not shorten the first attempt's deadline: the budget
	// stays at fuseTimeout so slow-but-successful commits do not regress for
	// operations without retry recovery (chmod, delete, mknod, symlink).
	// The lower bound catches a regression back to the detached-retry budget.
	budget := time.Until(deadline)
	if budget > fuseTimeout {
		t.Fatalf("interrupt-safe commit budget = %v, want <= fuseTimeout (%v)", budget, fuseTimeout)
	}
	if budget <= namespaceMutationRetryTimeout {
		t.Fatalf("interrupt-safe commit budget = %v, want > the detached-retry budget %v (first attempt must keep the full request deadline)", budget, namespaceMutationRetryTimeout)
	}

	// A parent with less remaining deadline than fuseTimeout clamps the
	// budget: the detached commit never outlives its request.
	parent, parentCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer parentCancel()
	clampedCtx, clampedCancel := fs.namespaceMutationCommitContext(parent)
	defer clampedCancel()
	clampedDeadline, ok := clampedCtx.Deadline()
	if !ok || time.Until(clampedDeadline) > 500*time.Millisecond {
		t.Fatalf("clamped commit deadline = %v/%v, want <= the parent's 500ms remaining", clampedDeadline, ok)
	}

	legacyFS := newInterruptMutationTestFS(t, true)
	legacyCtx, legacyCancel := legacyFS.namespaceMutationCommitContext(canceledCtx)
	defer legacyCancel()
	if legacyCtx != canceledCtx {
		t.Fatal("legacy mount commit ctx should be the parent ctx unchanged")
	}

	gvisorFS := newInterruptMutationTestFS(t, false)
	gvisorFS.opts.GVisorCompat = true
	gvisorCtx, gvisorCancel := gvisorFS.namespaceMutationCommitContext(canceledCtx)
	defer gvisorCancel()
	if err := gvisorCtx.Err(); err != nil {
		t.Fatalf("gVisor commit ctx err = %v, want detached from parent cancel", err)
	}
	gvisorDeadline, ok := gvisorCtx.Deadline()
	if !ok {
		t.Fatal("gVisor commit ctx has no deadline")
	}
	gvisorBudget := time.Until(gvisorDeadline)
	if gvisorBudget > fuseTimeout {
		t.Fatalf("gVisor commit budget = %v, want <= fuseTimeout (%v)", gvisorBudget, fuseTimeout)
	}
	if gvisorBudget <= namespaceMutationRetryTimeout {
		t.Fatalf("gVisor commit budget = %v, want > the detached-retry budget %v (gVisor keeps its historical fuseTimeout budget)", gvisorBudget, namespaceMutationRetryTimeout)
	}
}

// TestInterruptSafeMutationsCommitDetachedFromFuseInterrupt verifies that each
// idempotent namespace mutation commits even when the FUSE request context is
// already canceled: with a pre-canceled parent, the commit must still reach
// the server exactly once and succeed.
func TestInterruptSafeMutationsCommitDetachedFromFuseInterrupt(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, fs *Dat9FS, ctx context.Context)
	}{
		{
			name: "hardlink",
			run: func(t *testing.T, fs *Dat9FS, ctx context.Context) {
				if err := fs.hardlinkRemoteWithTransientRecovery(ctx, "/src.txt", "/dst.txt"); err != nil {
					t.Fatalf("hardlink commit err = %v, want nil", err)
				}
			},
		},
		{
			name: "rename",
			run: func(t *testing.T, fs *Dat9FS, ctx context.Context) {
				if err := fs.renameRemoteWithTransientRetry(ctx, "/old.txt", "/new.txt"); err != nil {
					t.Fatalf("rename commit err = %v, want nil", err)
				}
			},
		},
		{
			name: "delete-file",
			run: func(t *testing.T, fs *Dat9FS, ctx context.Context) {
				if err := fs.deleteRemoteFileWithInterruptRecovery(ctx, "/a.txt"); err != nil {
					t.Fatalf("delete commit err = %v, want nil", err)
				}
			},
		},
		{
			name: "delete-dir",
			run: func(t *testing.T, fs *Dat9FS, ctx context.Context) {
				if err := fs.deleteRemoteDirWithInterruptRecovery(ctx, "/a/"); err != nil {
					t.Fatalf("rmdir commit err = %v, want nil", err)
				}
			},
		},
		{
			name: "chmod",
			run: func(t *testing.T, fs *Dat9FS, ctx context.Context) {
				if err := fs.applyRemoteMode(ctx, "/a.txt", 0o644); err != nil {
					t.Fatalf("chmod commit err = %v, want nil", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mutationCalls atomic.Int32
			opts := &MountOptions{}
			opts.setDefaults()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost, http.MethodDelete:
					mutationCalls.Add(1)
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), opts)

			ctx, cancel := canceledCtx()
			defer cancel()
			tc.run(t, fs, ctx)

			if got := mutationCalls.Load(); got != 1 {
				t.Fatalf("mutation calls = %d, want exactly 1 (commit must run detached)", got)
			}
		})
	}
}

// TestLegacyMutationsKeepCancellableFirstCommit pins the rollback path: with
// LegacyInterruptibleMutations the first commit attempt still honors the
// canceled request context (no request reaches the server) and recovery
// resolves the outcome via the detached committed-probe instead.
func TestLegacyMutationsKeepCancellableFirstCommit(t *testing.T) {
	var mutationCalls, statCalls atomic.Int32
	opts := &MountOptions{LegacyInterruptibleMutations: true}
	opts.setDefaults()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			mutationCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			statCalls.Add(1)
			w.Header().Set("Content-Length", "6")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Resource-ID", "file-1")
			w.Header().Set("X-Dat9-Nlink", "2")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	ctx, cancel := canceledCtx()
	defer cancel()
	if err := fs.hardlinkRemoteWithTransientRecovery(ctx, "/src.txt", "/dst.txt"); err != nil {
		t.Fatalf("hardlink recovery err = %v, want nil", err)
	}
	if got := mutationCalls.Load(); got != 0 {
		t.Fatalf("mutation calls = %d, want 0 (legacy first commit must honor the canceled ctx)", got)
	}
	if got := statCalls.Load(); got < 1 {
		t.Fatalf("probe stat calls = %d, want >= 1 committed-probe", got)
	}
}

// TestInterruptSafeLinkUsesDetachedPostCommitStat runs the full Link entry
// point with a FUSE interrupt (cancel channel closed) landing exactly when
// the server commits the hardlink. The commit and the post-commit
// confirmation stat both run detached, so Link must succeed with the
// server-refreshed attributes rather than falling back.
func TestInterruptSafeLinkUsesDetachedPostCommitStat(t *testing.T) {
	cancel := make(chan struct{})
	var closeCancel sync.Once
	var postCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postCalls.Add(1)
			closeCancel.Do(func() { close(cancel) })
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case http.MethodHead:
			w.Header().Set("Content-Length", "6")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Resource-ID", "file-1")
			w.Header().Set("X-Dat9-Nlink", "5")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 1, false, 6, time.Now())

	var out gofuse.EntryOut
	st := fs.Link(cancel, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK", st)
	}
	if got := postCalls.Load(); got != 1 {
		t.Fatalf("POST calls = %d, want 1", got)
	}
	// fillEntryOut reports nlink as the alias count for regular files, so
	// assert on the inode: the detached confirm read must have applied the
	// server-reported nlink (5), not the local fallback (2).
	entry, ok := fs.inodes.GetEntry(srcIno)
	if !ok {
		t.Fatal("source entry missing after link")
	}
	if entry.Nlink != 5 {
		t.Fatalf("entry nlink = %d, want 5 from the detached post-commit stat", entry.Nlink)
	}
}

// TestLegacyInterruptibleLinkFallsBackOnInterruptedStat is the counterpart
// with the legacy option: the post-commit stat is bound to the FUSE cancel
// channel, so the interrupt costs the confirmation read. Link must still
// succeed (the tolerant guard from the post-commit-stat fix) using fallback
// attributes.
func TestLegacyInterruptibleLinkFallsBackOnInterruptedStat(t *testing.T) {
	cancel := make(chan struct{})
	var closeCancel sync.Once
	var postCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postCalls.Add(1)
			closeCancel.Do(func() { close(cancel) })
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case http.MethodHead:
			w.WriteHeader(statusClientClosedRequest)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{LegacyInterruptibleMutations: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	srcIno := fs.inodes.LookupWithIdentity("/src.txt", "file-1", 1, false, 6, time.Now())

	var out gofuse.EntryOut
	st := fs.Link(cancel, &gofuse.LinkIn{
		InHeader:  gofuse.InHeader{NodeId: 1},
		Oldnodeid: srcIno,
	}, "dst.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Link status = %v, want OK via the tolerant post-commit-stat guard", st)
	}
	if got := postCalls.Load(); got < 1 {
		t.Fatalf("POST calls = %d, want >= 1", got)
	}
	// The confirm read is bound to the canceled request in legacy mode, so the
	// inode keeps the local fallback nlink (source nlink + 1 = 2) instead of
	// the server's value.
	entry, ok := fs.inodes.GetEntry(srcIno)
	if !ok {
		t.Fatal("source entry missing after link")
	}
	if entry.Nlink != 2 {
		t.Fatalf("entry nlink = %d, want fallback 2 (source nlink + 1)", entry.Nlink)
	}
}

// TestInterruptSafeSymlinkCommitDetachedFromFuseInterrupt runs the Symlink
// entry point with the FUSE cancel channel already closed (the interrupt
// arrived before the commit). The symlink commit runs detached, so Symlink
// must succeed with exactly one mutation request.
func TestInterruptSafeSymlinkCommitDetachedFromFuseInterrupt(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)
	var posts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			posts.Add(1)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			// The post-create attribute refresh runs on the request context
			// and is already non-fatal; a transient failure is fine here.
			w.WriteHeader(statusClientClosedRequest)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Symlink(cancel, &gofuse.InHeader{NodeId: 1}, "/target", "link.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Symlink status = %v, want OK", st)
	}
	if got := posts.Load(); got != 1 {
		t.Fatalf("symlink POST calls = %d, want 1", got)
	}
}

// TestInterruptSafeMknodCommitDetachedFromFuseInterrupt runs Mknod for a
// regular file (which also exercises the conditional chmod because the mode
// differs from the default 0644) with the FUSE cancel channel already
// closed. Both commit calls run detached, so Mknod must succeed with exactly
// one create and one chmod.
func TestInterruptSafeMknodCommitDetachedFromFuseInterrupt(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)
	var puts, chmods atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			puts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"revision":1}`))
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmods.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			w.WriteHeader(statusClientClosedRequest)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	var out gofuse.EntryOut
	st := fs.Mknod(cancel, &gofuse.MknodIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Mode:     uint32(syscall.S_IFREG) | 0o600,
	}, "node.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Mknod status = %v, want OK", st)
	}
	if got := puts.Load(); got != 1 {
		t.Fatalf("create PUT calls = %d, want 1", got)
	}
	if got := chmods.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
}

// TestInterruptSafeMkdirCommitDetachedFromFuseInterrupt runs the Mkdir wrapper
// with the FUSE cancel channel already closed. The directory create runs
// detached, so it must commit exactly once.
func TestInterruptSafeMkdirCommitDetachedFromFuseInterrupt(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)
	var posts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			posts.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	if err := fs.mkdirRemoteWithTransientRetry(cancel, "/d", 0o755); err != nil {
		t.Fatalf("mkdir commit err = %v, want nil", err)
	}
	if got := posts.Load(); got != 1 {
		t.Fatalf("mkdir POST calls = %d, want 1", got)
	}
}
