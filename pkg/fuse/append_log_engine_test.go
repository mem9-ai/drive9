package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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

func TestAppendLogSmallTailCommitDoesNotRequireSnapshotCache(t *testing.T) {
	var appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		appendCalls++
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
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
	fs.appendLogSnapshotRoot = ""
	fs.opts.CacheDir = ""
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, fh.BaseRev, fh.OrigSize)

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", appendCalls)
	}
}

func TestAppendLogReadOnlyOpenDiscardsDiskOnlyShadow(t *testing.T) {
	const path = "/configured-log"
	opts := &MountOptions{AppendLogPatterns: []string{"**/configured-log"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	if !fs.appendLogPathConfigured(path) {
		t.Fatal("fixture path is not append-log configured")
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex = pending

	shadowDir := t.TempDir()
	beforeRestart, err := NewShadowStoreWithQuota(shadowDir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := beforeRestart.WriteFull(path, []byte("uncommitted-tail"), 5); err != nil {
		beforeRestart.Close()
		t.Fatal(err)
	}
	beforeRestart.Close()
	afterRestart, err := NewShadowStoreWithQuota(shadowDir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer afterRestart.Close()
	afterRestart.RecoverPendingBytes()
	if got := afterRestart.PendingBytes(); got != int64(len("uncommitted-tail")) {
		t.Fatalf("recovered pending bytes = %d, want %d", got, len("uncommitted-tail"))
	}
	fs.shadowStore = afterRestart

	ino := fs.inodes.Lookup(path, false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 5)
	var out gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out); status != gofuse.OK {
		t.Fatalf("Open status = %d, want OK", status)
	}
	defer fs.fileHandles.Delete(out.Fh)
	reader, ok := fs.fileHandles.Get(out.Fh)
	if !ok {
		t.Fatal("read-only handle is missing")
	}
	if reader.ShadowPinned {
		t.Fatal("read-only append-log handle pinned disk-only shadow after restart")
	}
	if afterRestart.Has(path) {
		t.Fatal("disk-only append-log shadow survived read-only reopen")
	}
	if got := afterRestart.PendingBytes(); got != 0 {
		t.Fatalf("pending bytes after disk-only discard = %d, want 0", got)
	}
}

func TestAppendLogReadOnlyOpenPinsResidentShadow(t *testing.T) {
	const path = "/configured-log"
	opts := &MountOptions{AppendLogPatterns: []string{"**/configured-log"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(path, []byte("committed-cache"), 5); err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ino := fs.inodes.Lookup(path, false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 5)
	var out gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out); status != gofuse.OK {
		t.Fatalf("Open status = %d, want OK", status)
	}
	defer fs.fileHandles.Delete(out.Fh)
	reader, ok := fs.fileHandles.Get(out.Fh)
	if !ok || !reader.ShadowPinned {
		t.Fatal("read-only append-log handle did not pin resident shadow")
	}
}

func TestAppendLogReadOnlyOpenRetainsPendingDiskOnlyShadow(t *testing.T) {
	const path = "/configured-log"
	opts := &MountOptions{AppendLogPatterns: []string{"**/configured-log"}}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Put(path, int64(len("recoverable-tail")), PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex = pending

	shadowDir := t.TempDir()
	beforeRestart, err := NewShadowStoreWithQuota(shadowDir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := beforeRestart.WriteFull(path, []byte("recoverable-tail"), 5); err != nil {
		beforeRestart.Close()
		t.Fatal(err)
	}
	beforeRestart.Close()
	afterRestart, err := NewShadowStoreWithQuota(shadowDir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer afterRestart.Close()
	fs.shadowStore = afterRestart

	ino := fs.inodes.Lookup(path, false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 5)
	var out gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Flags:    uint32(syscall.O_RDONLY),
	}, &out); status != gofuse.OK {
		t.Fatalf("Open status = %d, want OK", status)
	}
	defer fs.fileHandles.Delete(out.Fh)
	reader, ok := fs.fileHandles.Get(out.Fh)
	if !ok || !reader.ShadowPinned || !afterRestart.Has(path) {
		t.Fatal("read-only append-log handle did not retain pending disk-only shadow")
	}
}

func TestAppendLogAdoptsCommittedRevisionAndSizeTogether(t *testing.T) {
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("pretail"))
			return
		}
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
			t.Errorf("expected revision = %q, want 6", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("X-Dat9-Expected-Size"); got != "7" {
			t.Errorf("expected size = %q, want 7", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "next" {
			t.Errorf("append body = %q, want next", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 11})
	})
	defer closeServer()
	// Only a clean handle can adopt a sibling commit. Its stale inode size
	// must not override the size atomically published with the new revision.
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, []byte("pre")); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.DirtySeq = 0
	fh.Ino = fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
	fs.recordCommittedRevisionWithSize(fh.Path, 6, 7)

	fh.Lock()
	fs.adoptCommittedRevisionLocked(fh)
	if fh.BaseRev != 6 || fh.OrigSize != 7 {
		fh.Unlock()
		t.Fatalf("adopted baseline = %d/%d, want 6/7", fh.BaseRev, fh.OrigSize)
	}
	preWriteSize := fh.Dirty.Size()
	if _, err := fh.Dirty.Write(preWriteSize, []byte("next")); err != nil {
		fh.Unlock()
		t.Fatal(err)
	}
	fh.appendLogRecordUserWrite(preWriteSize, preWriteSize, 4)
	fh.DirtySeq = 2
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("append result = %+v, want committed", result)
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

func TestAppendLogWriteSyncModeFailureDoesNotRestoreCommittedTail(t *testing.T) {
	var appendCalls, chmodCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			appendCalls++
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "tail" {
				t.Errorf("append body = %q, want tail", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls++
			if chmodCalls == 1 {
				w.WriteHeader(http.StatusInternalServerError)
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, []byte("pre")); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.DirtySeq = 0
	fh.BaseRev = 5
	fh.OrigSize = 3
	fh.WritePolicy = WritePolicyWriteSync
	fh.appendLog = appendLogHandleState{initialized: true, appendSafe: true, layout: client.ContentLayoutAppendLog, revision: 5, size: 3}
	handleID := fs.fileHandles.Allocate(fh)
	defer fs.fileHandles.Delete(handleID)
	fh.Lock()
	fs.setPendingModeLocked(fh, 0o600, 1)
	fh.Unlock()

	written, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: 3}, []byte("tail"))
	if status == gofuse.OK || written != 0 {
		t.Fatalf("write result = %d/%d, want 0/chmod error", written, status)
	}
	if fh.BaseRev != 6 || fh.OrigSize != 7 || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() || string(fh.Dirty.Bytes()) != "pretail" || !fh.HasPendingMode {
		t.Fatalf("committed tail was restored after chmod failure: %+v body=%q", fh, fh.Dirty.Bytes())
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("mode retry Fsync = %d, want OK", status)
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

func TestAppendLogRebasedUnsupportedFallsBackWithRebasedRevision(t *testing.T) {
	var appendCalls, headCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			if appendCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "rebase required", "code": client.AppendLogCodeRebased})
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "append unsupported", "code": client.AppendLogCodeUnsupported})
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "6")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
				t.Errorf("full rewrite expected revision = %q, want 6", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "pretail" {
				t.Errorf("full rewrite body = %q, want pretail", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 7})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()

	fh.Lock()
	handled, status, fullRewrite := fs.routeAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if !handled || status != gofuse.OK || !fullRewrite {
		t.Fatalf("handled/status/full rewrite = %t/%d/%t, want true/OK/true", handled, status, fullRewrite)
	}
	if appendCalls != 2 || headCalls != 1 || putCalls != 1 {
		t.Fatalf("append/head/put calls = %d/%d/%d, want 2/1/1", appendCalls, headCalls, putCalls)
	}
}

func TestAppendLogRebaseDoesNotPublishStateAcrossConcurrentWrite(t *testing.T) {
	statStarted := make(chan struct{})
	releaseStat := make(chan struct{})
	writerHolding := make(chan struct{})
	releaseWriter := make(chan struct{})
	retryStarted := make(chan struct{}, 1)
	var (
		appendCalls       int
		releaseStatOnce   sync.Once
		releaseWriterOnce sync.Once
	)
	finishStat := func() { releaseStatOnce.Do(func() { close(releaseStat) }) }
	finishWriter := func() { releaseWriterOnce.Do(func() { close(releaseWriter) }) }
	closeAll := func() {
		finishStat()
		finishWriter()
	}
	t.Cleanup(closeAll)

	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			appendCalls++
			if appendCalls == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "rebase required", "code": client.AppendLogCodeRebased})
				return
			}
			retryStarted <- struct{}{}
			w.WriteHeader(http.StatusInternalServerError)
		case http.MethodHead:
			close(statStarted)
			<-releaseStat
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-Revision", "6")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	fs.opts.RemoteCommitWaitTimeout = 20 * time.Millisecond
	// The fixture seeds fh.Dirty directly. Register that initial generation as
	// production Open/Write paths do, so the concurrent Write must advance
	// DirtySeq rather than reusing the fixture's hand-written value.
	fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
	handleID := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(handleID, fh)

	resultCh := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		result := fs.tryAppendLogLocked(context.Background(), fh)
		fh.Unlock()
		resultCh <- result
	}()
	select {
	case <-statStarted:
	case <-time.After(time.Second):
		t.Fatal("rebase stat did not start")
	}

	writeCh := make(chan gofuse.Status, 1)
	go func() {
		_, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: 7}, []byte("next"))
		fh.Lock()
		close(writerHolding)
		<-releaseWriter
		fh.Unlock()
		writeCh <- status
	}()
	select {
	case <-writerHolding:
	case <-time.After(time.Second):
		t.Fatal("concurrent Write did not finish its timed lock path")
	}

	finishStat()
	select {
	case <-retryStarted:
		finishWriter()
		t.Fatal("rebase retry started while concurrent writer held fh.mu")
	case <-time.After(100 * time.Millisecond):
	}
	finishWriter()

	select {
	case status := <-writeCh:
		if status != gofuse.OK {
			t.Fatalf("concurrent Write status = %d, want OK", status)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent Write did not return")
	}
	select {
	case result := <-resultCh:
		if result.route != appendLogRouteFailed || result.status != gofuse.EAGAIN {
			t.Fatalf("rebase result = %+v, want failed/EAGAIN", result)
		}
	case <-time.After(time.Second):
		t.Fatal("rebase append did not return")
	}
	if appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", appendCalls)
	}
	if fh.appendLog.hasRewriteBase {
		t.Fatal("concurrent writer observed a rebase rewrite baseline")
	}
}

func TestAppendLogSameHandleConcurrentTailSyncUsesCommittedGeneration(t *testing.T) {
	firstAppendStarted := make(chan struct{})
	allowFirstAppend := make(chan struct{})
	var appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("append-log") {
			t.Errorf("request = %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		appendCalls++
		if appendCalls == 1 {
			close(firstAppendStarted)
			<-allowFirstAppend
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "duplicate tail must not upload", "code": client.AppendLogCodeConflict})
	})
	defer closeServer()

	firstDone := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		result := fs.tryAppendLogLocked(context.Background(), fh)
		fh.Unlock()
		firstDone <- result
	}()
	select {
	case <-firstAppendStarted:
	case <-time.After(time.Second):
		t.Fatal("first append did not reach the server")
	}

	secondDone := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		result := fs.tryAppendLogLocked(context.Background(), fh)
		fh.Unlock()
		secondDone <- result
	}()
	deadline := time.Now().Add(time.Second)
	for fh.TryLock() {
		fh.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("second append did not reach the path-lock wait")
		}
		time.Sleep(time.Millisecond)
	}
	close(allowFirstAppend)

	select {
	case result := <-firstDone:
		if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
			t.Fatalf("first result = %+v, want committed/OK", result)
		}
	case <-time.After(time.Second):
		t.Fatal("first append did not finish")
	}
	select {
	case result := <-secondDone:
		if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
			t.Fatalf("second result = %+v, want committed/OK", result)
		}
	case <-time.After(time.Second):
		t.Fatal("second append did not finish")
	}
	if appendCalls != 1 {
		t.Fatalf("append calls = %d, want 1", appendCalls)
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
	fh.Ino = fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "db-wal", Size: fh.OrigSize, Revision: fh.BaseRev}})

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
	fs.inodes.UpdateSize(fh.Ino, fh.Dirty.Size())
	fs.cacheFileForPath(fh.Path, fh.Dirty.Size(), time.Now(), fh.BaseRev)
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
	if entry, ok := fs.inodes.GetEntry(fh.Ino); !ok || entry.Size != int64(len("pretailnext")) {
		t.Fatalf("inode size after concurrent append = %+v/%t, want %d", entry, ok, len("pretailnext"))
	}
	if cached := fs.dirCache.Lookup("/", "db-wal"); cached.kind != namespaceLookupPositive || cached.item.Size != int64(len("pretailnext")) {
		t.Fatalf("dir cache after concurrent append = %+v, want live size %d", cached, len("pretailnext"))
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
