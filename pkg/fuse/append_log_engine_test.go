package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestAppendLogTailCommit(t *testing.T) {
	var appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		appendCalls++
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
			t.Errorf("expected revision = %q", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Dat9-Expected-Size"); got != "3" {
			t.Errorf("expected size = %q", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "tail" {
			t.Errorf("append body = %q, want tail", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != 0 {
		t.Fatalf("result = %+v, want committed OK", result)
	}
	if appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", appendCalls)
	}
	if fh.BaseRev != 6 || fh.OrigSize != 7 || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
		t.Fatalf("committed handle = %+v", fh)
	}
	if got := fh.appendLogLayoutAt(6, 7); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout = %q, want append_log", got)
	}
}

func TestAppendLogTailCommitFinalizesBeforePendingModeFailure(t *testing.T) {
	var appendCalls, chmodCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			appendCalls++
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls++
			if chmodCalls == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()

	fh.Lock()
	fs.setPendingModeLocked(fh, 0o600, 1)
	first := fs.tryAppendLogLocked(context.Background(), fh)
	if first.route != appendLogRouteFailed || first.status == gofuse.OK {
		fh.Unlock()
		t.Fatalf("first result = %+v, want chmod failure", first)
	}
	if fh.BaseRev != 6 || fh.OrigSize != 7 || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() || !fh.HasPendingMode {
		fh.Unlock()
		t.Fatalf("content finalization after chmod failure = %+v", fh)
	}
	secondHandled, secondStatus, _ := fs.routeAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if !secondHandled || secondStatus != gofuse.OK {
		t.Fatalf("mode retry = handled=%t status=%d, want true/OK", secondHandled, secondStatus)
	}
	if appendCalls != 1 || chmodCalls != 2 || fh.HasPendingMode {
		t.Fatalf("append/chmod/pending = %d/%d/%t, want 1/2/false", appendCalls, chmodCalls, fh.HasPendingMode)
	}
}

func TestAppendLogTailCommitAppliesPendingModeAndCachesDirEntry(t *testing.T) {
	var appendCalls, chmodCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			appendCalls++
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls++
			var request struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode chmod request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if request.Mode != 0o600 {
				t.Errorf("chmod mode = %o, want 600", request.Mode)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	defer closeServer()
	fh.Ino = fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "db-wal", Size: 0, Revision: 5}})

	fh.Lock()
	fs.setPendingModeLocked(fh, 0o600, 1)
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if appendCalls != 1 || chmodCalls != 1 || fh.HasPendingMode {
		t.Fatalf("append/chmod/pending = %d/%d/%t, want 1/1/false", appendCalls, chmodCalls, fh.HasPendingMode)
	}
	if entry, ok := fs.inodes.GetEntry(fh.Ino); !ok || !entry.HasMode || entry.Mode != 0o600 {
		t.Fatalf("inode mode = %+v/%t, want explicit 600", entry, ok)
	}
	cached := fs.dirCache.Lookup("/", "db-wal")
	if cached.kind != namespaceLookupPositive || cached.item.Size != 7 || cached.item.Revision != 6 {
		t.Fatalf("dir cache = %+v, want size/revision 7/6", cached)
	}
}

func TestAppendLogTailCommitRemovesUnownedStaleShadow(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	if err := shadow.WriteFull(fh.Path, []byte("stale"), fh.BaseRev); err != nil {
		t.Fatal(err)
	}

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if shadow.Has(fh.Path) {
		t.Fatal("append success retained an unowned stale shadow")
	}
}

func TestAppendLogNewZeroByteCreate(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "0" {
			t.Errorf("expected revision = %q", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Dat9-Expected-Size"); got != "0" {
			t.Errorf("expected size = %q", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if len(body) != 0 {
			t.Errorf("zero-byte create body = %q", body)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 1, Size: 0})
	})
	defer closeServer()

	fh.IsNew = true
	fh.OrigSize = 0
	fh.BaseRev = 0
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	fh.DirtySeq = 1
	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != 0 {
		t.Fatalf("result = %+v, want committed OK", result)
	}
	if fh.IsNew || fh.BaseRev != 1 || fh.OrigSize != 0 || fh.DirtySeq != 0 {
		t.Fatalf("created handle = %+v", fh)
	}
}

func TestAppendLogNewRandomAssemblyCreatesCompleteImage(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "0" {
			t.Errorf("expected revision = %q", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Dat9-Expected-Size"); got != "0" {
			t.Errorf("expected size = %q", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		want := []byte{'h', 'i', 0, 0, 't', 'a', 'i', 'l'}
		if !bytes.Equal(body, want) {
			t.Errorf("create body = %v, want %v", body, want)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 1, Size: int64(len(want))})
	})
	defer closeServer()

	fh.IsNew = true
	fh.OrigSize = 0
	fh.BaseRev = 0
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(4, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if _, err := fh.Dirty.Write(0, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = 1

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
}

func TestAppendLogRebaseRetriesOnceWithFrozenTail(t *testing.T) {
	var appendCalls, statCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "tail" {
				t.Errorf("append %d body = %q", appendCalls, got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if appendCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "rebase required", "code": client.AppendLogCodeRebased})
				return
			}
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
				t.Errorf("retry expected revision = %q", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 7})
		case http.MethodHead:
			statCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "6")
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	defer closeServer()

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != 0 {
		t.Fatalf("result = %+v, want committed OK", result)
	}
	if appendCalls != 2 || statCalls != 1 {
		t.Fatalf("append/stat calls = %d/%d, want 2/1", appendCalls, statCalls)
	}
}

func TestAppendLogRebaseRejectsChangedSizeWithoutRetry(t *testing.T) {
	var appendCalls, statCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "rebase required", "code": client.AppendLogCodeRebased})
		case http.MethodHead:
			statCalls++
			w.Header().Set("Content-Length", "4")
			w.Header().Set("X-Dat9-Revision", "6")
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	defer closeServer()

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteFailed || result.status == 0 {
		t.Fatalf("result = %+v, want terminal failure", result)
	}
	if appendCalls != 1 || statCalls != 1 {
		t.Fatalf("append/stat calls = %d/%d, want 1/1", appendCalls, statCalls)
	}
	if fh.DirtySeq != 1 || !fh.Dirty.HasDirtyParts() {
		t.Fatal("failed rebase must preserve dirty state")
	}
}

func TestAppendLogEnginePerfRecordsRebase(t *testing.T) {
	var appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			if appendCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "rebase required", "code": client.AppendLogCodeRebased})
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 7})
		case http.MethodHead:
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "6")
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
	defer closeServer()
	fs.perf = newFusePerfCounters(true)

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != 0 {
		t.Fatalf("result = %+v, want committed", result)
	}
	snap := fs.perf.snapshot()
	if got := snap.RemoteOps["append_log"]; got.count != 2 || got.bytes != 8 || got.errors != 1 {
		t.Fatalf("append-log stats = %+v, want count=2 bytes=8 errors=1", got)
	}
	if snap.Counters["append_log_outcome_rebased"] != 1 || snap.Counters["append_log_outcome_success"] != 1 || snap.Counters["append_log_rebase_retry_count"] != 1 {
		t.Fatalf("append-log counters = %v", snap.Counters)
	}
}

func TestAppendLogConcurrentTailPreservesNewerDirtyGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "tail" {
			t.Errorf("append body = %q, want tail", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()

	resultCh := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		result := fs.tryAppendLogLocked(context.Background(), fh)
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
	if fh.BaseRev != 6 || fh.OrigSize != 7 || fh.DirtySeq != 2 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("handle after concurrent append = %+v", fh)
	}
	if !fh.appendLogCanUseTail() {
		t.Fatal("contiguous newer suffix should remain append-safe from the committed size")
	}
}

func TestAppendLogTailRetargetDuringRequestPreservesDirtyGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs/db-wal" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		close(started)
		<-release
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()
	fs.openHandles.Add(fh)
	defer fs.openHandles.Remove(fh)

	resultCh := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		resultCh <- fs.tryAppendLogLocked(context.Background(), fh)
		fh.Unlock()
	}()
	<-started
	fs.retargetOpenHandlesForRename("/db-wal", "/renamed.db-wal")
	close(release)

	result := <-resultCh
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed OK", result)
	}
	if fh.Path != "/renamed.db-wal" || fh.DirtySeq != 1 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("retargeted handle must retain dirty generation: %+v", fh)
	}
}

func TestAppendLogTailUnlinkDuringRequestPreservesDirtyGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		close(started)
		<-release
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer closeServer()

	resultCh := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		resultCh <- fs.tryAppendLogLocked(context.Background(), fh)
		fh.Unlock()
	}()
	<-started
	fh.Lock()
	fh.Unlinked = true
	fh.Unlock()
	close(release)

	result := <-resultCh
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed OK", result)
	}
	if !fh.Unlinked || fh.DirtySeq != 1 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("unlinked handle must retain dirty generation: %+v", fh)
	}
}

func TestAppendLogUnsupportedAndTooLargePreserveDirtyState(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		code       string
		wantRoute  appendLogRoute
		wantStatus syscall.Errno
	}{
		{name: "unsupported", status: http.StatusBadRequest, code: client.AppendLogCodeUnsupported, wantRoute: appendLogRouteNeedsRewrite},
		{name: "too large", status: http.StatusRequestEntityTooLarge, code: client.AppendLogCodeTooLarge, wantRoute: appendLogRouteFailed, wantStatus: syscall.EFBIG},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": test.code, "code": test.code})
			})
			defer closeServer()
			fh.appendLogObserveLayout(client.ContentLayoutAppendLog, fh.BaseRev, fh.OrigSize)
			beforeSeq := fh.DirtySeq

			fh.Lock()
			result := fs.tryAppendLogLocked(context.Background(), fh)
			fh.Unlock()
			if result.route != test.wantRoute || result.status != gofuseStatus(test.wantStatus) {
				t.Fatalf("result = %+v, want route=%v status=%v", result, test.wantRoute, test.wantStatus)
			}
			if fh.DirtySeq != beforeSeq || !fh.Dirty.HasDirtyParts() {
				t.Fatal("failed append must preserve dirty state")
			}
			if test.code == client.AppendLogCodeUnsupported && !fh.appendLog.unsupported {
				t.Fatal("unsupported response must suppress this handle")
			}
			if got := fh.appendLogLayoutAt(5, 3); got != client.ContentLayoutAppendLog {
				t.Fatalf("layout after failure = %q, want append_log", got)
			}
		})
	}
}

func TestAppendLogConflictAndTimeoutPreserveDirtyState(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "conflict", "code": client.AppendLogCodeConflict})
		})
		defer closeServer()

		beforeSeq := fh.DirtySeq
		fh.Lock()
		result := fs.tryAppendLogLocked(context.Background(), fh)
		fh.Unlock()
		if result.route != appendLogRouteFailed || result.status == gofuse.OK {
			t.Fatalf("result = %+v, want failure", result)
		}
		if fh.DirtySeq != beforeSeq || !fh.Dirty.HasDirtyParts() {
			t.Fatal("conflict must preserve dirty state")
		}
	})

	t.Run("timeout", func(t *testing.T) {
		fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {})
		defer closeServer()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		beforeSeq := fh.DirtySeq
		fh.Lock()
		result := fs.tryAppendLogLocked(ctx, fh)
		fh.Unlock()
		if result.route != appendLogRouteFailed || result.status != gofuse.Status(syscall.EAGAIN) {
			t.Fatalf("result = %+v, want EAGAIN failure", result)
		}
		if fh.DirtySeq != beforeSeq || !fh.Dirty.HasDirtyParts() {
			t.Fatal("timeout must preserve dirty state")
		}
	})
}

func TestAppendLogCapabilityDisabledDoesNotProbeEndpoint(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("append endpoint must not be called when capability is disabled")
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteNotApplicable || result.status != 0 {
		t.Fatalf("result = %+v, want not-applicable OK", result)
	}
}

func newAppendLogEngineFixture(t *testing.T, capability bool, appendHandler http.HandlerFunc) (*Dat9FS, *FileHandle, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(map[string]any{"storage_capabilities": map[string]bool{"append_log_v1": capability}})
			return
		}
		appendHandler(w, r)
	}))
	c := client.New(server.URL, "")
	c.Warm(context.Background())
	opts := &MountOptions{CacheDir: t.TempDir(), AppendLogPatterns: []string{"**/db-wal"}}
	opts.setDefaults()
	fs := NewDat9FS(c, opts)

	dirty := NewWriteBuffer("/db-wal", 1024, 0)
	if _, err := dirty.Write(0, []byte("pretail")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	fh := &FileHandle{Ino: 1, Path: "/db-wal", Dirty: dirty, DirtySeq: 1, OrigSize: 3, BaseRev: 5}
	fh.appendLogRecordUserWrite(3, 3, 4)
	return fs, fh, server.Close
}

func gofuseStatus(errno syscall.Errno) gofuse.Status {
	return gofuse.Status(errno)
}
