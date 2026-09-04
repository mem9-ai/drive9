package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestAppendLogFullRewriteUsesOneConditionalPUT(t *testing.T) {
	var putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		putCalls++
		if r.Method != http.MethodPut || r.URL.RawQuery != "" {
			t.Fatalf("request = %s %s", r.Method, r.URL.String())
		}
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
			t.Fatalf("expected revision = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "rewrite" {
			t.Fatalf("full rewrite body = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)

	fh.Lock()
	result := fs.tryAppendLogFullRewriteLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != 0 {
		t.Fatalf("result = %+v, want committed", result)
	}
	if putCalls != 1 || fh.BaseRev != 6 || fh.OrigSize != int64(len("rewrite")) || fh.DirtySeq != 0 {
		t.Fatalf("calls/handle = %d/%+v", putCalls, fh)
	}
	if got := fh.appendLogLayoutAt(6, int64(len("rewrite"))); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout = %q, want append_log", got)
	}
}

func TestAppendLogFullRewriteResolvesUnknownLayoutOnce(t *testing.T) {
	for _, test := range []struct {
		name      string
		layout    client.ContentLayout
		wantRoute appendLogRoute
		wantPut   int
	}{
		{name: "append log", layout: client.ContentLayoutAppendLog, wantRoute: appendLogRouteCommitted, wantPut: 1},
		{name: "single", layout: client.ContentLayoutSingle, wantRoute: appendLogRouteNotApplicable},
	} {
		t.Run(test.name, func(t *testing.T) {
			var headCalls, putCalls int
			fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					headCalls++
					w.Header().Set("Content-Length", "3")
					w.Header().Set("X-Dat9-Revision", "5")
					w.Header().Set("X-Dat9-Content-Layout", string(test.layout))
					w.WriteHeader(http.StatusOK)
				case http.MethodPut:
					putCalls++
					_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
				default:
					t.Fatalf("unexpected method %s", r.Method)
				}
			})
			defer closeServer()
			setAppendLogRewriteDirty(t, fh, "rewrite")

			fh.Lock()
			result := fs.tryAppendLogFullRewriteLocked(context.Background(), fh)
			fh.Unlock()
			if result.route != test.wantRoute || result.status != 0 {
				t.Fatalf("result = %+v, want route %v", result, test.wantRoute)
			}
			if headCalls != 1 || putCalls != test.wantPut {
				t.Fatalf("head/put calls = %d/%d, want 1/%d", headCalls, putCalls, test.wantPut)
			}
		})
	}
}

func TestAppendLogFullRewriteRejectsMissingLayoutWithoutWrite(t *testing.T) {
	var headCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	beforeSeq := fh.DirtySeq

	fh.Lock()
	result := fs.tryAppendLogFullRewriteLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteFailed || result.status != gofuse.EIO {
		t.Fatalf("result = %+v, want EIO failure", result)
	}
	if headCalls != 1 || putCalls != 0 {
		t.Fatalf("head/put calls = %d/%d, want 1/0", headCalls, putCalls)
	}
	if fh.DirtySeq != beforeSeq || !fh.Dirty.HasDirtyParts() {
		t.Fatal("missing layout must preserve dirty state")
	}
}

func TestAppendLogFtruncateZeroRetainsCommittedBaselineForFullRewrite(t *testing.T) {
	var headCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
				t.Fatalf("expected revision = %q, want 5", got)
			}
			body, _ := io.ReadAll(r.Body)
			if len(body) != 0 {
				t.Fatalf("full rewrite body = %q, want empty", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()
	if err := fh.Dirty.Truncate(0); err != nil {
		t.Fatal(err)
	}
	fh.appendLogRecordTruncate()
	fh.OrigSize = 0 // mirrors truncateWritableHandleLocked's generic-routing state.

	fh.Lock()
	result := fs.tryAppendLogFullRewriteLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if headCalls != 1 || putCalls != 1 {
		t.Fatalf("head/put calls = %d/%d, want 1/1", headCalls, putCalls)
	}
}

func TestAppendLogCheckpointReuseFsyncUsesConditionalFullPUT(t *testing.T) {
	var appendCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			w.WriteHeader(http.StatusInternalServerError)
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
				t.Errorf("expected revision = %q, want 5", got)
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "checkpoint" {
				t.Errorf("checkpoint rewrite body = %q, want checkpoint", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, fh.BaseRev, fh.OrigSize)
	if err := fh.Dirty.Truncate(0); err != nil {
		t.Fatal(err)
	}
	fh.appendLogRecordTruncate()
	fh.OrigSize = 0
	if _, err := fh.Dirty.Write(0, []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	fh.appendLogRecordUserWrite(0, 0, int64(len("checkpoint")))
	handleID := fs.fileHandles.Allocate(fh)

	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	if appendCalls != 0 || putCalls != 1 {
		t.Fatalf("append/put calls = %d/%d, want 0/1", appendCalls, putCalls)
	}
}

func TestAppendLogPathTruncateClearsAdoptedHandleGeneration(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()
	ino := fs.inodes.Lookup(fh.Path, false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 5)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}
	fh.Ino = ino
	fh.OpenPID = 77
	fh.DirtySeq = fs.markDirtySize(ino, fh.Dirty.Size())
	fs.openHandles.Add(fh)
	defer fs.openHandles.Remove(fh)

	handled, status := fs.tryAppendLogPathTruncate(context.Background(), entry, ino, 77, 0, nil)
	if !handled || status != gofuse.OK {
		t.Fatalf("handled/status = %t/%d, want true/OK", handled, status)
	}
	if fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() || fh.BaseRev != 6 || fh.OrigSize != 0 || fh.ZeroBase {
		t.Fatalf("adopted handle = %+v", fh)
	}
}

func TestAppendLogFullRewriteTooLargePreservesLayoutAndDirtyState(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeTooLarge, "code": client.AppendLogCodeTooLarge})
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)

	fh.Lock()
	result := fs.tryAppendLogFullRewriteLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteFailed || result.status != gofuseStatus(syscall.EFBIG) {
		t.Fatalf("result = %+v, want EFBIG failure", result)
	}
	if fh.DirtySeq != 1 || !fh.Dirty.HasDirtyParts() {
		t.Fatal("failed full rewrite must preserve dirty state")
	}
	if got := fh.appendLogLayoutAt(5, 3); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout after failure = %q, want append_log", got)
	}
}

func TestAppendLogFullRewriteFailuresPreserveLayoutAndDirtyState(t *testing.T) {
	tests := []struct {
		name   string
		ctx    func() context.Context
		handle http.HandlerFunc
	}{
		{
			name: "conflict",
			ctx:  context.Background,
			handle: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "conflict", "code": client.AppendLogCodeConflict})
			},
		},
		{
			name: "server error",
			ctx:  context.Background,
			handle: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		},
		{
			name: "malformed success",
			ctx:  context.Background,
			handle: func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]int64{})
			},
		},
		{
			name: "canceled context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			handle: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs, fh, closeServer := newAppendLogEngineFixture(t, false, test.handle)
			defer closeServer()
			setAppendLogRewriteDirty(t, fh, "rewrite")
			fh.appendLogObserveLayout(client.ContentLayoutAppendLog, fh.BaseRev, fh.OrigSize)

			fh.Lock()
			result := fs.tryAppendLogFullRewriteLocked(test.ctx(), fh)
			fh.Unlock()
			if result.route != appendLogRouteFailed || result.status == gofuse.OK {
				t.Fatalf("result = %+v, want terminal failure", result)
			}
			if fh.DirtySeq != 1 || !fh.Dirty.HasDirtyParts() {
				t.Fatal("failed full rewrite must preserve dirty state")
			}
			if got := fh.appendLogLayoutAt(5, 3); got != client.ContentLayoutAppendLog {
				t.Fatalf("layout after failure = %q, want append_log", got)
			}
		})
	}
}

func TestAppendLogPathTruncateUsesConditionalFullPUT(t *testing.T) {
	var headCalls, putCalls int
	fs, _, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
				t.Fatalf("expected revision = %q, want 5", got)
			}
			body, _ := io.ReadAll(r.Body)
			if len(body) != 0 {
				t.Fatalf("truncate body = %q, want empty", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
	})
	defer closeServer()
	ino := fs.inodes.Lookup("/db-wal", false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 5)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}

	if status := fs.applyRemoteTruncate(context.Background(), entry, ino, 0, 0); status != gofuse.OK {
		t.Fatalf("applyRemoteTruncate status = %d, want OK", status)
	}
	if headCalls != 1 || putCalls != 1 {
		t.Fatalf("head/put calls = %d/%d, want 1/1", headCalls, putCalls)
	}
}

func TestAppendLogPathTruncateGrowthFreezesPrefixAndZeroFill(t *testing.T) {
	var getCalls, headCalls, putCalls int
	fs, _, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls++
			_, _ = w.Write([]byte("abc"))
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
				t.Fatalf("expected revision = %q, want 5", got)
			}
			body, _ := io.ReadAll(r.Body)
			if want := []byte{'a', 'b', 'c', 0, 0}; !bytes.Equal(body, want) {
				t.Fatalf("truncate growth body = %v, want %v", body, want)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
	})
	defer closeServer()
	ino := fs.inodes.Lookup("/db-wal", false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 5)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("inode entry missing")
	}

	if status := fs.applyRemoteTruncate(context.Background(), entry, ino, 0, 5); status != gofuse.OK {
		t.Fatalf("applyRemoteTruncate status = %d, want OK", status)
	}
	if getCalls != 1 || headCalls != 1 || putCalls != 1 {
		t.Fatalf("get/head/put calls = %d/%d/%d, want 1/1/1", getCalls, headCalls, putCalls)
	}
}

func TestAppendLogGenericUnsupportedReroutesOnceToFullPUT(t *testing.T) {
	var putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		putCalls++
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "pretail" {
			t.Fatalf("full rewrite body = %q, want pretail", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()

	fh.Lock()
	handled, status := fs.routeAppendLogGenericUnsupportedLocked(
		context.Background(),
		fh,
		fh.Path,
		fh.DirtySeq,
		&client.StatusError{StatusCode: http.StatusBadRequest, Code: client.AppendLogCodeUnsupported},
	)
	fh.Unlock()
	if !handled || status != gofuse.OK || putCalls != 1 {
		t.Fatalf("handled/status/puts = %t/%d/%d, want true/OK/1", handled, status, putCalls)
	}
	if got := fh.appendLogLayoutAt(6, 7); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout = %q, want append_log", got)
	}
}

func TestAppendLogReleaseShadowSpillRoutesBeforeCommitQueue(t *testing.T) {
	var appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Fatalf("request = %s %s", r.Method, r.URL.String())
		}
		appendCalls++
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(fh.Path, []byte("pretail"), fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	queue := NewCommitQueue(fs.client, shadow, pending, nil, 1, 8)
	defer queue.DrainAll()
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = queue
	fh.ShadowSpill = true
	fh.ShadowReady = true
	fh.ShadowCommitReady = true
	fh.ShadowCommitSeq = fh.DirtySeq
	handleID := fs.fileHandles.Allocate(fh)

	fs.Release(nil, &gofuse.ReleaseIn{Fh: handleID})
	if appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", appendCalls)
	}
}

func TestAppendLogReleaseNonTailUsesConditionalFullPUT(t *testing.T) {
	var appendCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			w.WriteHeader(http.StatusInternalServerError)
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "rewrite" {
				t.Errorf("release full rewrite body = %q, want rewrite", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, fh.BaseRev, fh.OrigSize)
	handleID := fs.fileHandles.Allocate(fh)

	fs.Release(nil, &gofuse.ReleaseIn{Fh: handleID})
	if appendCalls != 0 || putCalls != 1 {
		t.Fatalf("append/put calls = %d/%d, want 0/1", appendCalls, putCalls)
	}
}

func TestAppendLogEntryPointFlushUsesAppendRoutesBeforeGenericUpload(t *testing.T) {
	var appendCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
		case http.MethodPut:
			putCalls++
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != 0 || appendCalls != 1 || putCalls != 0 {
		t.Fatalf("status/append/put = %d/%d/%d, want OK/1/0", status, appendCalls, putCalls)
	}
}

func TestAppendLogEntryPointFlushUsesFullPUTForNonTailWrite(t *testing.T) {
	var appendCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "rewrite" {
				t.Fatalf("full rewrite body = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodHead:
			t.Fatalf("known append_log layout must not restat")
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != 0 || appendCalls != 0 || putCalls != 1 {
		t.Fatalf("status/append/put = %d/%d/%d, want OK/0/1", status, appendCalls, putCalls)
	}
}

func TestAppendLogWriteSyncWriteUsesAppendRoute(t *testing.T) {
	var appendCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "tailmore" {
				t.Errorf("append body = %q, want tailmore", got)
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 11})
		case http.MethodPut:
			putCalls++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	fh.WritePolicy = WritePolicyWriteSync
	handleID := fs.fileHandles.Allocate(fh)

	written, status := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       handleID,
		Offset:   uint64(fh.Dirty.Size()),
	}, []byte("more"))
	if status != gofuse.OK || written != 4 {
		t.Fatalf("Write status/written = %d/%d, want OK/4", status, written)
	}
	if appendCalls != 1 || putCalls != 0 {
		t.Fatalf("append/put calls = %d/%d, want 1/0", appendCalls, putCalls)
	}
}

func TestAppendLogEntryPointFsyncForcesRemoteAppendInInteractiveMode(t *testing.T) {
	var appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Fatalf("request = %s %s", r.Method, r.URL.String())
		}
		appendCalls++
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()
	fs.syncMode = SyncInteractive
	fs.perf = newFusePerfCounters(true)
	handleID := fs.fileHandles.Allocate(fh)

	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != 0 {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	if appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", appendCalls)
	}
	if got := fs.perf.snapshot().Counters["append_log_fsync_append_count"]; got != 1 {
		t.Fatalf("append_log_fsync_append_count = %d, want 1", got)
	}
}

func TestAppendLogEntryPointFsyncRecordsFullRewrite(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)
	fs.perf = newFusePerfCounters(true)
	handleID := fs.fileHandles.Allocate(fh)

	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	if got := fs.perf.snapshot().Counters["append_log_fsync_full_rewrite_count"]; got != 1 {
		t.Fatalf("append_log_fsync_full_rewrite_count = %d, want 1", got)
	}
}

func TestAppendLogUnsupportedFallbackUsesOneFullPUT(t *testing.T) {
	var appendCalls, statCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case http.MethodHead:
			statCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "pretail" {
				t.Fatalf("fallback full body = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != 0 || appendCalls != 1 || statCalls != 1 || putCalls != 1 {
		t.Fatalf("status/append/stat/put = %d/%d/%d/%d, want OK/1/1/1", status, appendCalls, statCalls, putCalls)
	}
	if !fh.appendLog.unsupported {
		t.Fatal("unsupported append must suppress later tail attempts for this handle")
	}
}

func TestAppendLogUnsupportedConcurrentWriteDoesNotFallbackFromNewerGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var appendCalls, headCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			close(started)
			<-release
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()

	resultCh := make(chan gofuse.Status, 1)
	go func() {
		fh.Lock()
		resultCh <- fs.flushHandle(context.Background(), fh)
		fh.Unlock()
	}()
	<-started

	fh.Lock()
	preWriteSize := fh.Dirty.Size()
	if _, err := fh.Dirty.Write(preWriteSize, []byte("next")); err != nil {
		fh.Unlock()
		t.Fatalf("concurrent Write: %v", err)
	}
	fh.appendLogRecordUserWrite(preWriteSize, preWriteSize, 4)
	fh.DirtySeq = 2
	fh.Unlock()
	close(release)

	if status := <-resultCh; status != gofuse.EAGAIN {
		t.Fatalf("flush status = %d, want EAGAIN", status)
	}
	if appendCalls != 1 || headCalls != 0 || putCalls != 0 {
		t.Fatalf("append/head/put calls = %d/%d/%d, want 1/0/0", appendCalls, headCalls, putCalls)
	}
	if !fh.appendLog.unsupported || fh.DirtySeq != 2 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("handle must retain newer dirty generation: %+v", fh)
	}
}

func TestAppendLogFsyncUnsupportedFallbackRecordsFullRewrite(t *testing.T) {
	var appendCalls, headCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	fs.perf = newFusePerfCounters(true)
	handleID := fs.fileHandles.Allocate(fh)

	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	if appendCalls != 1 || headCalls != 1 || putCalls != 1 {
		t.Fatalf("append/head/put calls = %d/%d/%d, want 1/1/1", appendCalls, headCalls, putCalls)
	}
	snapshot := fs.perf.snapshot()
	if got := snapshot.Counters["append_log_fsync_full_rewrite_count"]; got != 1 {
		t.Fatalf("append_log_fsync_full_rewrite_count = %d, want 1", got)
	}
	if got := snapshot.Counters["append_log_fsync_append_count"]; got != 0 {
		t.Fatalf("append_log_fsync_append_count = %d, want 0", got)
	}
}

func TestAppendLogShadowSpillUnsupportedV2ReroutesOnceToFullPUT(t *testing.T) {
	var initiateCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			initiateCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/db-wal":
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
				t.Errorf("expected revision = %q, want 5", got)
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "pretail" {
				t.Errorf("full rewrite body = %q, want pretail", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(fh.Path, []byte("pretail"), fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fh.ShadowSpill = true
	fh.ShadowReady = true
	fh.appendLogObserveLayout(client.ContentLayoutSingle, fh.BaseRev, fh.OrigSize)

	fh.Lock()
	status := fs.syncHandleToRemoteLocked(context.Background(), fh)
	fh.Unlock()
	if status != gofuse.OK {
		t.Fatalf("sync status = %d, want OK", status)
	}
	if initiateCalls != 1 || putCalls != 1 {
		t.Fatalf("initiate/put calls = %d/%d, want 1/1", initiateCalls, putCalls)
	}
}

func TestAppendLogCreateAndOpenDoNotAttachGenericStreamer(t *testing.T) {
	t.Run("capability enabled create", func(t *testing.T) {
		fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		})
		defer closeServer()

		var out gofuse.CreateOut
		if status := fs.Create(nil, &gofuse.CreateIn{
			InHeader: gofuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_RDWR),
		}, "db-wal", &out); status != gofuse.OK {
			t.Fatalf("Create status = %d, want OK", status)
		}
		fh, ok := fs.fileHandles.Get(out.Fh)
		if !ok {
			t.Fatal("created handle is missing")
		}
		if fh.Streamer != nil {
			t.Fatal("configured append-log create must not attach a generic streamer")
		}
	})

	for _, capability := range []bool{false, true} {
		t.Run("existing open", func(t *testing.T) {
			fs, fh, closeServer := newAppendLogEngineFixture(t, capability, func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			})
			defer closeServer()
			ino := fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
			fs.inodes.UpdateRevision(ino, fh.BaseRev)
			fs.readCache.Put(fh.Path, []byte("pre"), fh.BaseRev)

			var out gofuse.OpenOut
			if status := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out); status != gofuse.OK {
				t.Fatalf("Open status = %d, want OK", status)
			}
			opened, ok := fs.fileHandles.Get(out.Fh)
			if !ok {
				t.Fatal("opened handle is missing")
			}
			if opened.Streamer != nil {
				t.Fatal("configured append-log open must not attach a generic streamer")
			}
		})
	}
}

func TestAppendLogUnsupportedSingleFallsBackToGenericRewrite(t *testing.T) {
	var appendCalls, statCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case http.MethodHead:
			statCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutSingle))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "pretail" {
				t.Fatalf("generic rewrite body = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()
	fs.client.SetSmallFileThresholdForTests(100)
	fs.perf = newFusePerfCounters(true)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != gofuse.OK || appendCalls != 1 || statCalls != 1 || putCalls != 1 {
		t.Fatalf("status/append/stat/put = %d/%d/%d/%d, want OK/1/1/1", status, appendCalls, statCalls, putCalls)
	}
	if !fh.appendLog.unsupported {
		t.Fatal("single fallback must suppress further append attempts on this handle")
	}
	if got := fs.perf.snapshot().Counters["append_log_full_rewrite_count"]; got != 1 {
		t.Fatalf("append_log_full_rewrite_count = %d, want 1", got)
	}
}

func TestAppendLogSingleLayoutSelectionDoesNotPrecountGenericTransport(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutSingle, fh.BaseRev, fh.OrigSize)
	fs.perf = newFusePerfCounters(true)

	fh.Lock()
	handled, status, _ := fs.routeAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if handled || status != gofuse.OK {
		t.Fatalf("handled/status = %t/%d, want false/OK", handled, status)
	}
	if got := fs.perf.snapshot().Counters["append_log_full_rewrite_count"]; got != 0 {
		t.Fatalf("append_log_full_rewrite_count = %d before generic transport, want 0", got)
	}
}

func TestAppendLogSingleGenericPatchDoesNotCountFullRewrite(t *testing.T) {
	var patchCalls, uploadCalls, completeCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/db-wal":
			patchCalls++
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.PatchPlan{
				UploadID: "patch-1",
				PartSize: 1024,
				UploadParts: []*client.PatchPartURL{{
					Number: 1,
					URL:    "http://" + r.Host + "/upload/1",
					Size:   3,
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/upload/1":
			uploadCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "new" {
				t.Errorf("patch body = %q, want new", got)
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/patch-1/complete":
			completeCalls++
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "new")
	fh.appendLogObserveLayout(client.ContentLayoutSingle, fh.BaseRev, fh.OrigSize)
	fs.client.SetSmallFileThresholdForTests(1)
	fs.perf = newFusePerfCounters(true)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != gofuse.OK {
		t.Fatalf("flush status = %d, want OK", status)
	}
	if patchCalls != 1 || uploadCalls != 1 || completeCalls != 1 {
		t.Fatalf("patch/upload/complete calls = %d/%d/%d, want 1/1/1", patchCalls, uploadCalls, completeCalls)
	}
	if got := fs.perf.snapshot().Counters["append_log_full_rewrite_count"]; got != 0 {
		t.Fatalf("append_log_full_rewrite_count = %d after PATCH, want 0", got)
	}
}

func TestAppendLogGenericPatchUnsupportedReroutesOnceToFullPUT(t *testing.T) {
	var patchCalls, fullPutCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/fs/db-wal":
			patchCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/db-wal":
			fullPutCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "new" {
				t.Errorf("full rewrite body = %q, want new", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "new")
	fh.appendLogObserveLayout(client.ContentLayoutSingle, fh.BaseRev, fh.OrigSize)
	fs.client.SetSmallFileThresholdForTests(1)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != gofuse.OK {
		t.Fatalf("flush status = %d, want OK", status)
	}
	if patchCalls != 1 || fullPutCalls != 1 {
		t.Fatalf("patch/full PUT calls = %d/%d, want 1/1", patchCalls, fullPutCalls)
	}
}

func TestAppendLogGenericDirectPUTUnsupportedReroutesOnceToFullPUT(t *testing.T) {
	var putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/fs/db-wal" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		putCalls++
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "pretail" {
			t.Errorf("PUT %d body = %q, want pretail", putCalls, got)
		}
		if putCalls == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	fh.appendLogObserveLayout(client.ContentLayoutSingle, fh.BaseRev, fh.OrigSize)
	fs.client.SetSmallFileThresholdForTests(100)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != gofuse.OK {
		t.Fatalf("flush status = %d, want OK", status)
	}
	if putCalls != 2 {
		t.Fatalf("direct/full PUT calls = %d, want 2", putCalls)
	}
}

func TestAppendLogLegacyUploadPlanUnsupportedReroutesOnceToFullPUT(t *testing.T) {
	var v2Calls, v1Calls, legacyPlanCalls, fullPutCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			v2Calls++
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/uploads/initiate":
			v1Calls++
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/db-wal" && r.ContentLength == 0:
			legacyPlanCalls++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": client.AppendLogCodeUnsupported, "code": client.AppendLogCodeUnsupported})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/db-wal":
			fullPutCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "rewrite" {
				t.Errorf("full rewrite body = %q, want rewrite", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutSingle, fh.BaseRev, fh.OrigSize)
	fs.client.SetSmallFileThresholdForTests(1)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != gofuse.OK {
		t.Fatalf("flush status = %d, want OK", status)
	}
	if v2Calls != 1 || v1Calls != 1 || legacyPlanCalls != 1 || fullPutCalls != 1 {
		t.Fatalf("v2/v1/legacy/full calls = %d/%d/%d/%d, want 1/1/1/1", v2Calls, v1Calls, legacyPlanCalls, fullPutCalls)
	}
}

func TestAppendLogCapabilityChangeStillFullRewritesKnownAppendLayout(t *testing.T) {
	var putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		putCalls++
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)

	fh.Lock()
	status := fs.flushHandle(context.Background(), fh)
	fh.Unlock()
	if status != 0 || putCalls != 1 {
		t.Fatalf("status/put = %d/%d, want OK/1", status, putCalls)
	}
}

func TestAppendLogCapabilityDisabledPureTailFsyncUsesConditionalFullPUT(t *testing.T) {
	var putCalls, unexpectedCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.RawQuery != "" {
			unexpectedCalls++
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		putCalls++
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
			t.Errorf("expected revision = %q, want 5", got)
		}
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "pretail" {
			t.Errorf("full rewrite body = %q, want pretail", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	fs.client.SetSmallFileThresholdForTests(1)
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, fh.BaseRev, fh.OrigSize)
	fs.perf = newFusePerfCounters(true)
	handleID := fs.fileHandles.Allocate(fh)

	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	if putCalls != 1 || unexpectedCalls != 0 {
		t.Fatalf("conditional PUT/unexpected calls = %d/%d, want 1/0", putCalls, unexpectedCalls)
	}
	snapshot := fs.perf.snapshot()
	if got := snapshot.Counters["append_log_fsync_full_rewrite_count"]; got != 1 {
		t.Fatalf("append_log_fsync_full_rewrite_count = %d, want 1", got)
	}
	if got := snapshot.Counters["append_log_fsync_append_count"]; got != 0 {
		t.Fatalf("append_log_fsync_append_count = %d, want 0", got)
	}
}

func TestAppendLogFullRewriteConcurrentMutationPreservesNewerDirtyGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		close(started)
		<-release
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "rewrite" {
			t.Fatalf("full rewrite body = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	setAppendLogRewriteDirty(t, fh, "rewrite")
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)

	resultCh := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		result := fs.tryAppendLogFullRewriteLocked(context.Background(), fh)
		fh.Unlock()
		resultCh <- result
	}()
	<-started

	fh.Lock()
	preWriteSize := fh.Dirty.Size()
	if _, err := fh.Dirty.Write(preWriteSize, []byte("next")); err != nil {
		fh.Unlock()
		t.Fatalf("concurrent Write: %v", err)
	}
	fh.appendLogRecordUserWrite(preWriteSize, preWriteSize, 4)
	fh.DirtySeq = 2
	fh.Unlock()
	close(release)

	result := <-resultCh
	if result.route != appendLogRouteCommitted || result.status != 0 {
		t.Fatalf("result = %+v, want committed", result)
	}
	if fh.BaseRev != 6 || fh.OrigSize != int64(len("rewrite")) || fh.DirtySeq != 2 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("handle after concurrent full rewrite = %+v", fh)
	}
	if got := fh.appendLogLayoutAt(6, int64(len("rewrite"))); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout = %q, want append_log", got)
	}
}

func setAppendLogRewriteDirty(t *testing.T, fh *FileHandle, body string) {
	t.Helper()
	dirty := NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := dirty.Write(0, []byte(body)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	fh.Dirty = dirty
	fh.DirtySeq = 1
	fh.IsNew = false
	fh.OrigSize = 3
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{}
	fh.appendLogRecordUserWrite(3, 0, 1)
}
