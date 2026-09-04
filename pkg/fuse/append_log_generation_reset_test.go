package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestAppendLogGenerationResetPublishesHeaderThenAppendsFirstFrame(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeaderBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	newHeader, ok := parseSQLiteWALHeader(newHeaderBytes)
	if !ok {
		t.Fatal("new header did not parse")
	}
	if !oldHeader.saltsDiffer(newHeader) {
		t.Fatal("fixture salts must differ")
	}

	var putCalls, appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "5" {
				t.Fatalf("reset expected revision = %q, want 5", got)
			}
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, newHeaderBytes) {
				t.Fatalf("reset body = %x, want header %x", body, newHeaderBytes)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
				t.Fatalf("append expected revision = %q, want 6", got)
			}
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Fatalf("append expected size = %q, want 32", got)
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "frame" {
				t.Fatalf("append body = %q, want frame", got)
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 37})
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
	})
	defer closeServer()

	image := make([]byte, 128)
	copy(image, newHeaderBytes)
	copy(image[32:], []byte("old-generation-tail-must-not-be-uploaded"))
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, image); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = 11
	fh.OrigSize = int64(len(image))
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{
		initialized:                  true,
		layout:                       client.ContentLayoutAppendLog,
		revision:                     5,
		size:                         int64(len(image)),
		appendSafe:                   false,
		sqliteWALConfirmed:           true,
		sqliteWALCommittedHeader:     oldHeader,
		sqliteWALWriteAtZero:         true,
		sqliteWALHeaderDirtyByteMask: ^uint32(0),
	}
	fs.readCache.Put(fh.Path, []byte("stale-generation"), 5)

	fh.Lock()
	if got, parsed := fs.appendLogReadSQLiteWALHeaderLocked(fh); !parsed || got != newHeader {
		fh.Unlock()
		t.Fatalf("dirty H1 = %+v parsed=%t, want %+v", got, parsed, newHeader)
	}
	handled, status, fullRewrite := fs.routeAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if !handled || status != gofuse.OK || !fullRewrite {
		t.Fatalf("reset route = handled=%t status=%d fullRewrite=%t, want true/OK/true", handled, status, fullRewrite)
	}
	if putCalls != 1 || appendCalls != 0 {
		t.Fatalf("reset put/append calls = %d/%d, want 1/0", putCalls, appendCalls)
	}
	if fh.BaseRev != 6 || fh.OrigSize != sqliteWALHeaderSize || fh.Dirty.Size() != sqliteWALHeaderSize || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
		t.Fatalf("reset handle = %+v size=%d dirty=%t", fh, fh.Dirty.Size(), fh.Dirty.HasDirtyParts())
	}
	if !fh.appendLog.sqliteWALConfirmed || fh.appendLog.sqliteWALCommittedHeader != newHeader {
		t.Fatal("reset did not adopt the new committed WAL header")
	}
	if cached, ok := fs.readCache.Get(fh.Path, 6); !ok || !bytes.Equal(cached, newHeaderBytes) {
		t.Fatalf("read cache = %x/%t, want new header", cached, ok)
	}

	if _, err := fh.Dirty.Write(sqliteWALHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	fh.appendLogRecordUserWrite(sqliteWALHeaderSize, sqliteWALHeaderSize, int64(len("frame")))
	fh.DirtySeq = 12
	fh.Lock()
	appendResult := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if appendResult.route != appendLogRouteCommitted || appendResult.status != gofuse.OK {
		t.Fatalf("first-frame append = %+v, want committed", appendResult)
	}
	if putCalls != 1 || appendCalls != 1 {
		t.Fatalf("put/append calls = %d/%d, want 1/1", putCalls, appendCalls)
	}
}

func TestAppendLogGenerationResetRejectsUnchangedSalt(t *testing.T) {
	header, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("header did not parse")
	}
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request %s", r.Method)
	})
	defer closeServer()

	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, append(make([]byte, 0, 64), header.raw[:]...)); err != nil {
		t.Fatal(err)
	}
	if err := fh.Dirty.Truncate(64); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = 11
	fh.OrigSize = 64
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{
		initialized:                  true,
		layout:                       client.ContentLayoutAppendLog,
		revision:                     5,
		size:                         64,
		sqliteWALConfirmed:           true,
		sqliteWALCommittedHeader:     header,
		sqliteWALWriteAtZero:         true,
		sqliteWALHeaderDirtyByteMask: ^uint32(0),
	}

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteNotApplicable || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want not applicable", result)
	}
}

func TestAppendLogGenerationResetRequiresCompleteHeader(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("incomplete header must not issue %s", r.Method)
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	fh.appendLog.sqliteWALHeaderDirtyByteMask = (uint32(1) << 16) - 1

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteNotApplicable || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want not applicable", result)
	}
}

func TestAppendLogGenerationResetResolvesUnknownLayout(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var headCalls, putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			w.Header().Set("Content-Length", "64")
			w.Header().Set("X-Dat9-Revision", "5")
			w.Header().Set("X-Dat9-Content-Layout", string(client.ContentLayoutAppendLog))
			w.WriteHeader(http.StatusOK)
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, newHeader) {
				t.Fatalf("reset body = %x, want %x", body, newHeader)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	fh.appendLog.layout = ""
	fh.appendLog.revision = 0
	fh.appendLog.size = 0

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if headCalls != 1 || putCalls != 1 {
		t.Fatalf("head/put calls = %d/%d, want 1/1", headCalls, putCalls)
	}
}

func TestAppendLogGenerationResetFailurePreservesOldGeneration(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"transient"}`))
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteFailed || result.status == gofuse.OK {
		t.Fatalf("result = %+v, want failed", result)
	}
	if fh.BaseRev != 5 || fh.OrigSize != 64 || fh.Dirty.Size() != 64 || fh.DirtySeq != 11 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("failed reset mutated handle = %+v size=%d", fh, fh.Dirty.Size())
	}
	if fh.appendLog.sqliteWALCommittedHeader != oldHeader {
		t.Fatal("failed reset must preserve H0")
	}
}

func TestAppendLogGenerationResetTerminalResponsesPreserveOldGeneration(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "conflict", status: http.StatusConflict, body: `{"error":"conflict","code":"append_log_conflict"}`},
		{name: "unsupported", status: http.StatusBadRequest, body: `{"error":"unsupported","code":"append_log_unsupported"}`},
		{name: "malformed success", status: http.StatusOK, body: `{"revision":0}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls int
			fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			})
			defer closeServer()
			setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)

			fh.Lock()
			result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
			fh.Unlock()
			if result.route != appendLogRouteFailed || result.status == gofuse.OK {
				t.Fatalf("result = %+v, want failed", result)
			}
			if calls != 1 || fh.BaseRev != 5 || fh.OrigSize != 64 || fh.Dirty.Size() != 64 || fh.DirtySeq != 11 || !fh.Dirty.HasDirtyParts() || fh.appendLog.sqliteWALCommittedHeader != oldHeader {
				t.Fatalf("terminal result mutated state: calls=%d handle=%+v size=%d", calls, fh, fh.Dirty.Size())
			}
		})
	}
}

func TestAppendLogGenerationResetCanceledRequestPreservesOldGeneration(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("canceled request must not reach server")
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(ctx, fh)
	fh.Unlock()
	if result.route != appendLogRouteFailed || result.status == gofuse.OK {
		t.Fatalf("result = %+v, want failed", result)
	}
	if fh.BaseRev != 5 || fh.OrigSize != 64 || fh.Dirty.Size() != 64 || fh.DirtySeq != 11 || fh.appendLog.sqliteWALCommittedHeader != oldHeader {
		t.Fatalf("canceled reset mutated handle = %+v size=%d", fh, fh.Dirty.Size())
	}
}

func TestAppendLogGenerationResetRetargetOrUnlinkPreservesDirtyGeneration(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	for _, test := range []struct {
		name   string
		mutate func(*FileHandle)
	}{
		{name: "retarget", mutate: func(fh *FileHandle) { fh.Path = "/renamed.db-wal" }},
		{name: "unlink", mutate: func(fh *FileHandle) { fh.Unlinked = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var fh *FileHandle
			fs, fixtureHandle, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				fh.Lock()
				test.mutate(fh)
				fh.Unlock()
				_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
			})
			defer closeServer()
			fh = fixtureHandle
			setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)

			fh.Lock()
			result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
			fh.Unlock()
			if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
				t.Fatalf("result = %+v, want committed", result)
			}
			if fh.DirtySeq != 11 || fh.Dirty.Size() != 64 || !fh.Dirty.HasDirtyParts() || fh.appendLog.sqliteWALCommittedHeader != oldHeader {
				t.Fatalf("retarget/unlink reset mutated live generation: %+v size=%d", fh, fh.Dirty.Size())
			}
		})
	}
}

func TestAppendLogGenerationResetConcurrentWriteRetainsDirtyGeneration(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var fh *FileHandle
	fs, fixtureHandle, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		fh.Lock()
		if _, err := fh.Dirty.Write(sqliteWALHeaderSize, []byte("frame")); err != nil {
			fh.Unlock()
			t.Fatal(err)
		}
		fh.appendLogRecordUserWrite(sqliteWALHeaderSize, sqliteWALHeaderSize, int64(len("frame")))
		fh.DirtySeq = 12
		fh.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	fh = fixtureHandle
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if fh.BaseRev != 6 || fh.OrigSize != sqliteWALHeaderSize || fh.DirtySeq != 12 || fh.Dirty.Size() != 64 || !fh.Dirty.HasDirtyParts() {
		t.Fatalf("concurrent reset state = %+v size=%d", fh, fh.Dirty.Size())
	}
	if fh.appendLog.appendSafe {
		t.Fatal("concurrent reset must force the next flush through full rewrite")
	}
}

func TestAppendLogGenerationResetFsyncRecordsConditionalPUT(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, newHeader) {
			t.Fatalf("reset body = %x, want %x", body, newHeader)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	fs.perf = newFusePerfCounters(true)
	handleID := fs.fileHandles.Allocate(fh)

	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	snapshot := fs.perf.snapshot()
	if got := snapshot.Counters["append_log_fsync_full_rewrite_count"]; got != 1 {
		t.Fatalf("full-rewrite fsync count = %d, want 1", got)
	}
	if got := snapshot.Counters["append_log_generation_reset_count"]; got != 1 {
		t.Fatalf("generation-reset count = %d, want 1", got)
	}
	if got := snapshot.Counters["append_log_generation_reset_bytes"]; got != sqliteWALHeaderSize {
		t.Fatalf("generation-reset bytes = %d, want %d", got, sqliteWALHeaderSize)
	}
}

func TestAppendLogGenerationResetRotatesPinnedShadow(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	fs.perf = newFusePerfCounters(true)
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	shadowImage := make([]byte, 64)
	copy(shadowImage, newHeader)
	if err := shadow.WriteFull(fh.Path, shadowImage, fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.Pin(fh.Path)
	fh.ShadowReady = true
	fh.ShadowPinned = true
	fh.ShadowGen = shadowGen

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if !fh.ShadowReady || !fh.ShadowSpill || fh.ShadowPinned || fh.ShadowGen != 0 {
		t.Fatalf("shadow state = ready=%t spill=%t pinned=%t gen=%d", fh.ShadowReady, fh.ShadowSpill, fh.ShadowPinned, fh.ShadowGen)
	}
	if !shadow.Has(fh.Path) || shadow.Size(fh.Path) != sqliteWALHeaderSize {
		t.Fatalf("rotated shadow = has=%t size=%d, want true/%d", shadow.Has(fh.Path), shadow.Size(fh.Path), sqliteWALHeaderSize)
	}
	data, err := shadow.ReadAll(fh.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, newHeader) {
		t.Fatalf("rotated shadow = %x, want H1 %x", data, newHeader)
	}
	if shadow.SizeGen(shadowGen) >= 0 {
		t.Fatal("generation reset retained the old pinned shadow generation")
	}
	if got := fs.perf.snapshot().Counters["append_log_generation_reset_shadow_ready"]; got != 1 {
		t.Fatalf("shadow ready count = %d, want 1", got)
	}
}

func TestAppendLogGenerationResetRotatesUnownedPathShadow(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	if err := shadow.WriteFull(fh.Path, bytes.Repeat([]byte{'o'}, 64), fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	// The previous shadow owner may already have closed. A newly opened handle
	// has no per-handle shadow flags, but the path-level shadow remains active.
	if fh.ShadowReady || fh.ShadowSpill || fh.ShadowPinned {
		t.Fatal("fixture unexpectedly has handle-owned shadow state")
	}

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	data, err := shadow.ReadAll(fh.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, newHeader) {
		t.Fatalf("rotated shadow = %x, want H1 %x", data, newHeader)
	}
	if !fh.ShadowReady || !fh.ShadowSpill {
		t.Fatalf("new generation shadow state = ready=%t spill=%t, want true/true", fh.ShadowReady, fh.ShadowSpill)
	}
}

func TestAppendLogGenerationResetAppliesPendingModeAndCachesDirEntry(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var putCalls, chmodCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls++
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			chmodCalls++
			var request struct {
				Mode uint32 `json:"mode"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			if request.Mode != 0o600 {
				t.Fatalf("chmod mode = %o, want 600", request.Mode)
			}
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	fh.Ino = fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "db-wal", Size: 0, Revision: 5}})

	fh.Lock()
	fs.setPendingModeLocked(fh, 0o600, 1)
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if putCalls != 1 || chmodCalls != 1 || fh.HasPendingMode {
		t.Fatalf("put/chmod/pending = %d/%d/%t, want 1/1/false", putCalls, chmodCalls, fh.HasPendingMode)
	}
	cached := fs.dirCache.Lookup("/", "db-wal")
	if cached.kind != namespaceLookupPositive || cached.item.Size != sqliteWALHeaderSize || cached.item.Revision != 6 {
		t.Fatalf("dir cache = %+v, want size/revision %d/6", cached, sqliteWALHeaderSize)
	}
}

func TestAppendLogGenerationResetShadowFailureKeepsRemoteReset(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var remoteReadCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: sqliteWALHeaderSize + int64(len("frame"))})
		case http.MethodGet:
			remoteReadCalls++
			_, _ = w.Write(append(append([]byte(nil), newHeader...), []byte("frame")...))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	defer closeServer()

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	fs.perf = newFusePerfCounters(true)
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	shadowImage := make([]byte, 64)
	copy(shadowImage, newHeader)
	if _, err := shadow.WriteAt(fh.Path, 0, shadowImage, fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	fh.ShadowReady = true
	fh.ShadowSpill = true

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if fh.BaseRev != 6 || fh.OrigSize != sqliteWALHeaderSize || fh.Dirty.Size() != sqliteWALHeaderSize || fh.DirtySeq != 0 {
		t.Fatalf("remote reset state = base=%d size=%d dirty=%d dirtySeq=%d", fh.BaseRev, fh.OrigSize, fh.Dirty.Size(), fh.DirtySeq)
	}
	if fh.ShadowReady || fh.ShadowSpill || shadow.Has(fh.Path) {
		t.Fatalf("shadow failure state = ready=%t spill=%t active=%t", fh.ShadowReady, fh.ShadowSpill, shadow.Has(fh.Path))
	}
	if got := fs.perf.snapshot().Counters["append_log_generation_reset_shadow_degraded"]; got != 1 {
		t.Fatalf("shadow degraded count = %d, want 1", got)
	}
	handleID := fs.fileHandles.Allocate(fh)
	defer fs.fileHandles.Delete(handleID)
	if written, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: sqliteWALHeaderSize}, []byte("frame")); status != gofuse.OK || written != uint32(len("frame")) {
		t.Fatalf("first-frame write = %d/%d, want %d/OK", written, status, len("frame"))
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("first-frame fsync = %d, want OK", status)
	}
	got, status, err := readDat9FSTestRange(fs, fh.Ino, handleID, sqliteWALHeaderSize, len("frame"))
	if err != nil {
		t.Fatal(err)
	}
	if status != gofuse.OK || string(got) != "frame" {
		t.Fatalf("degraded read = %q/%d remoteCalls=%d base=%d orig=%d dirty=%d, want frame/OK", got, status, remoteReadCalls, fh.BaseRev, fh.OrigSize, fh.Dirty.Size())
	}
	if remoteReadCalls != 1 {
		t.Fatalf("remote WAL reads = %d, want 1", remoteReadCalls)
	}
}

func TestAppendLogGenerationResetShadowServesFirstFrameRead(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	frame := bytes.Repeat([]byte("f"), 4<<10)
	var putCalls, appendCalls, remoteReadCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, newHeader) {
				t.Fatalf("reset body = %x, want H1 %x", body, newHeader)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Fatalf("append expected size = %q, want 32", got)
			}
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, frame) {
				t.Fatalf("append body length = %d, want frame length %d", len(body), len(frame))
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: sqliteWALHeaderSize + int64(len(frame))})
		case http.MethodGet:
			remoteReadCalls++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
	})
	defer closeServer()

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	shadowImage := make([]byte, 64)
	copy(shadowImage, newHeader)
	copy(shadowImage[sqliteWALHeaderSize:], bytes.Repeat([]byte("o"), len(shadowImage)-sqliteWALHeaderSize))
	if err := shadow.WriteFull(fh.Path, shadowImage, fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	fh.ShadowReady = true
	fh.ShadowSpill = true

	fh.Lock()
	reset := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if reset.route != appendLogRouteCommitted || reset.status != gofuse.OK {
		t.Fatalf("reset = %+v, want committed", reset)
	}
	fh.Dirty.maxSize = sqliteWALHeaderSize + int64(len(frame))

	handleID := fs.fileHandles.Allocate(fh)
	defer fs.fileHandles.Delete(handleID)
	if written, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: sqliteWALHeaderSize}, frame); status != gofuse.OK || int(written) != len(frame) {
		t.Fatalf("first-frame write = %d/%d, want %d/OK", written, status, len(frame))
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("first-frame fsync = %d, want OK", status)
	}
	if putCalls != 1 || appendCalls != 1 {
		t.Fatalf("put/append calls = %d/%d, want 1/1", putCalls, appendCalls)
	}
	got, status, err := readDat9FSTestRange(fs, fh.Ino, handleID, sqliteWALHeaderSize, len(frame))
	if err != nil {
		t.Fatal(err)
	}
	if status != gofuse.OK || !bytes.Equal(got, frame) {
		t.Fatalf("checkpoint-like read = status=%d bytes=%d, want OK/frame", status, len(got))
	}
	if remoteReadCalls != 0 {
		t.Fatalf("remote WAL reads = %d, want 0", remoteReadCalls)
	}
}

func TestAppendLogGenerationResetDoesNotInvertHandleAndRemoteCommitLocks(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	responseStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			close(responseStarted)
			<-releaseResponse
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			return
		default:
			t.Errorf("method = %s, want PUT", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()

	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	fh.Lock()
	fs.setPendingModeLocked(fh, 0o600, 1)
	fh.Unlock()
	resetDone := make(chan appendLogAttemptResult, 1)
	go func() {
		fh.Lock()
		result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
		fh.Unlock()
		resetDone <- result
	}()
	<-responseStarted

	writerHasHandle := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		fh.Lock()
		close(writerHasHandle)
		unlockRemoteCommit := fs.lockHandleRemoteCommitPathLocked(fh)
		unlockRemoteCommit()
		fh.Unlock()
		close(writerDone)
	}()
	<-writerHasHandle
	close(releaseResponse)
	select {
	case result := <-resetDone:
		if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
			t.Fatalf("reset = %+v, want committed", result)
		}
	case <-time.After(time.Second):
		t.Fatal("generation reset deadlocked behind a writer holding fh.Lock")
	}
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("writer did not complete after generation reset")
	}
}

func TestAppendLogCaptureSQLiteWALHeaderPreservesTruncateFence(t *testing.T) {
	oldHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, append(oldHeader, bytes.Repeat([]byte("x"), 32)...)); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.appendLog = appendLogHandleState{}
	fh.appendLogRecordTruncate()
	if !fh.appendLog.sqliteWALTruncated {
		t.Fatal("truncate fence was not set")
	}
	fs.appendLogCaptureSQLiteWALPreWriteLocked(fh, 0, sqliteWALHeaderSize)
	if !fh.appendLog.sqliteWALConfirmed {
		t.Fatal("header was not confirmed")
	}
	if !fh.appendLog.sqliteWALTruncated {
		t.Fatal("header confirmation cleared the truncate fence")
	}
}

func TestAppendLogGenerationResetWriteThenFsyncUsesFirstFrameAppend(t *testing.T) {
	oldHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var putCalls, appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls++
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, newHeader) {
				t.Fatalf("reset body = %x, want %x", body, newHeader)
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Fatalf("append expected size = %q, want 32", got)
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "frame" {
				t.Fatalf("append body = %q, want frame", got)
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 37})
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
	})
	defer closeServer()

	oldImage := make([]byte, 64)
	copy(oldImage, oldHeader)
	copy(oldImage[sqliteWALHeaderSize:], []byte("old-generation-tail"))
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, oldImage); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.DirtySeq = 0
	fh.OrigSize = int64(len(oldImage))
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{
		initialized: true,
		appendSafe:  true,
		layout:      client.ContentLayoutAppendLog,
		revision:    5,
		size:        int64(len(oldImage)),
	}
	handleID := fs.fileHandles.Allocate(fh)

	if written, status := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       handleID,
		Offset:   0,
	}, newHeader); status != gofuse.OK || written != uint32(len(newHeader)) {
		t.Fatalf("header Write status/written = %d/%d", status, written)
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("header Fsync status = %d, want OK", status)
	}
	if putCalls != 1 || appendCalls != 0 {
		t.Fatalf("after reset put/append = %d/%d, want 1/0", putCalls, appendCalls)
	}
	if fh.OrigSize != sqliteWALHeaderSize || fh.Dirty.Size() != sqliteWALHeaderSize {
		t.Fatalf("reset size = handle/buffer %d/%d, want 32/32", fh.OrigSize, fh.Dirty.Size())
	}

	if written, status := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fh.Ino},
		Fh:       handleID,
		Offset:   sqliteWALHeaderSize,
	}, []byte("frame")); status != gofuse.OK || written != uint32(len("frame")) {
		t.Fatalf("frame Write status/written = %d/%d", status, written)
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("frame Fsync status = %d, want OK", status)
	}
	if putCalls != 1 || appendCalls != 1 {
		t.Fatalf("final put/append = %d/%d, want 1/1", putCalls, appendCalls)
	}
}

func TestAppendLogGenerationResetWithFrameBeforeFsyncUsesFullRewrite(t *testing.T) {
	oldHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var putCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want complete-image PUT", r.Method)
		}
		putCalls++
		body, _ := io.ReadAll(r.Body)
		if len(body) != 64 || !bytes.Equal(body[:sqliteWALHeaderSize], newHeader) || string(body[sqliteWALHeaderSize:sqliteWALHeaderSize+len("frame")]) != "frame" {
			t.Fatalf("full rewrite body = %x, want H1 plus frame in 64-byte image", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()

	oldImage := make([]byte, 64)
	copy(oldImage, oldHeader)
	copy(oldImage[sqliteWALHeaderSize:], []byte("old-generation-tail"))
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, oldImage); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.DirtySeq = 0
	fh.OrigSize = int64(len(oldImage))
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{
		initialized: true,
		appendSafe:  true,
		layout:      client.ContentLayoutAppendLog,
		revision:    5,
		size:        int64(len(oldImage)),
	}
	fh.WritePolicy = WritePolicyWriteBack
	fs.perf = newFusePerfCounters(true)
	handleID := fs.fileHandles.Allocate(fh)

	for _, write := range []struct {
		offset uint64
		data   []byte
	}{
		{offset: 0, data: newHeader},
		{offset: sqliteWALHeaderSize, data: []byte("frame")},
	} {
		written, status := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: fh.Ino},
			Fh:       handleID,
			Offset:   write.offset,
		}, write.data)
		if status != gofuse.OK || written != uint32(len(write.data)) {
			t.Fatalf("Write(%d) status/written = %d/%d", write.offset, status, written)
		}
	}
	if !fh.appendLog.sqliteWALWriteBeyondHeader {
		t.Fatal("frame write must fence a header-only generation reset")
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("Fsync status = %d, want OK", status)
	}
	if putCalls != 1 {
		t.Fatalf("complete-image PUT calls = %d, want 1", putCalls)
	}
	if got := fs.perf.snapshot().Counters["append_log_generation_reset_count"]; got != 0 {
		t.Fatalf("generation-reset count = %d, want 0", got)
	}
}

func TestAppendLogCapturesSQLiteWALHeaderBeforeOffsetZeroWrite(t *testing.T) {
	oldHeaderBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	oldHeader, ok := parseSQLiteWALHeader(oldHeaderBytes)
	if !ok {
		t.Fatal("old header did not parse")
	}
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request %s", r.Method)
	})
	defer closeServer()

	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, append(oldHeaderBytes, []byte("old-tail")...)); err != nil {
		t.Fatal(err)
	}
	fh.OrigSize = fh.Dirty.Size()
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{initialized: true}

	fh.Lock()
	fs.appendLogCaptureSQLiteWALPreWriteLocked(fh, 0, sqliteWALHeaderSize)
	fh.Unlock()
	if !fh.appendLog.sqliteWALConfirmed || fh.appendLog.sqliteWALCommittedHeader != oldHeader {
		t.Fatalf("captured header = %+v, want %+v", fh.appendLog.sqliteWALCommittedHeader, oldHeader)
	}
}

func TestAppendLogNewSQLiteWALCreateAdoptsCommittedHeader(t *testing.T) {
	headerBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	header, ok := parseSQLiteWALHeader(headerBytes)
	if !ok {
		t.Fatal("header did not parse")
	}
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, headerBytes) {
			t.Fatalf("create body = %x, want %x", body, headerBytes)
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 1, Size: sqliteWALHeaderSize})
	})
	defer closeServer()
	fh.IsNew = true
	fh.BaseRev = 0
	fh.OrigSize = 0
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, headerBytes); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = 1

	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if !fh.appendLog.sqliteWALConfirmed || fh.appendLog.sqliteWALCommittedHeader != header {
		t.Fatalf("committed header = %+v, want %+v", fh.appendLog.sqliteWALCommittedHeader, header)
	}
}

func TestAppendLogTailFsyncLazilyConfirmsSQLiteWALHeader(t *testing.T) {
	headerBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	header, ok := parseSQLiteWALHeader(headerBytes)
	if !ok {
		t.Fatal("header did not parse")
	}
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "tail" {
			t.Fatalf("tail body = %q, want tail", got)
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: sqliteWALHeaderSize + int64(len("tail"))})
	})
	defer closeServer()

	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, append(headerBytes, []byte("tail")...)); err != nil {
		t.Fatal(err)
	}
	fh.OrigSize = sqliteWALHeaderSize
	fh.BaseRev = 5
	fh.DirtySeq = 1
	fh.appendLog = appendLogHandleState{initialized: true, appendSafe: true}

	fh.Lock()
	handled, status, fullRewrite := fs.routeAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if !handled || status != gofuse.OK || fullRewrite {
		t.Fatalf("route = handled=%t status=%d fullRewrite=%t, want true/OK/false", handled, status, fullRewrite)
	}
	if !fh.appendLog.sqliteWALConfirmed || fh.appendLog.sqliteWALCommittedHeader != header {
		t.Fatalf("confirmed header = %+v, want %+v", fh.appendLog.sqliteWALCommittedHeader, header)
	}
}

func setGenerationResetDirty(t *testing.T, fh *FileHandle, oldHeader sqliteWALHeader, newHeader []byte, size int) {
	t.Helper()
	image := make([]byte, size)
	copy(image, newHeader)
	copy(image[sqliteWALHeaderSize:], []byte("old-generation-tail"))
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, image); err != nil {
		t.Fatal(err)
	}
	fh.DirtySeq = 11
	fh.OrigSize = int64(size)
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{
		initialized:                  true,
		layout:                       client.ContentLayoutAppendLog,
		revision:                     5,
		size:                         int64(size),
		sqliteWALConfirmed:           true,
		sqliteWALCommittedHeader:     oldHeader,
		sqliteWALWriteAtZero:         true,
		sqliteWALHeaderDirtyByteMask: ^uint32(0),
	}
}
