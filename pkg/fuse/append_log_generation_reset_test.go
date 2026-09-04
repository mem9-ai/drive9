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
				t.Errorf("reset expected revision = %q, want 5", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, newHeaderBytes) {
				t.Errorf("reset body = %x, want header %x", body, newHeaderBytes)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
				t.Errorf("append expected revision = %q, want 6", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Errorf("append expected size = %q, want 32", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "frame" {
				t.Errorf("append body = %q, want frame", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 37})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
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

func TestAppendLogGenerationResetRestartsShadowStreamingState(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	fs.perf = newFusePerfCounters(true)

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	if err := shadow.WriteFull(fh.Path, bytes.Repeat([]byte("o"), 64), fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	fh.ShadowReady = true
	fh.ShadowSpill = true
	fh.Dirty.sequential = false
	fh.Dirty.uploadedParts = map[int]bool{0: true}
	oldCallbacks := 0
	fh.Dirty.OnPartFull = func(int, []byte) { oldCallbacks++ }

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if !fh.Dirty.IsSequential() {
		t.Fatal("reset left the next WAL generation non-sequential")
	}
	if got := len(fh.Dirty.uploadedParts); got != 0 {
		t.Fatalf("uploaded parts = %d, want no old-generation entries", got)
	}
	if fh.Dirty.OnPartFull == nil {
		t.Fatal("shadow-ready reset did not reinstall the spill callback")
	}
	fh.Dirty.OnPartFull(0, []byte("new-generation-part"))
	if oldCallbacks != 0 {
		t.Fatalf("old spill callback invoked %d times", oldCallbacks)
	}
	if !fh.Dirty.uploadedParts[0] {
		t.Fatal("new spill callback did not evict the new-generation part")
	}
	if got := fs.perf.snapshot().Counters["append_log_outcome_success"]; got != 0 {
		t.Fatalf("append-log outcome success = %d, want 0 for conditional reset PUT", got)
	}
}

func TestAppendLogGenerationResetClearsReadOnlySiblingTarget(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer closeServer()
	setGenerationResetDirty(t, fh, oldHeader, newHeader, 64)
	reader := &FileHandle{Path: fh.Path, ReadTarget: &client.ReadTarget{ObjectURL: "https://old-object.example"}}
	fs.openHandles.Add(reader)
	defer fs.openHandles.Remove(reader)

	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("result = %+v, want committed", result)
	}
	if reader.ReadTarget != nil {
		t.Fatalf("read-only sibling retained stale target: %+v", reader.ReadTarget)
	}
}

func TestAppendLogGenerationResetRejectsUnchangedSalt(t *testing.T) {
	header, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("header did not parse")
	}
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
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
		t.Errorf("incomplete header must not issue %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
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
				t.Errorf("reset body = %x, want %x", body, newHeader)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
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
		t.Error("canceled request must not reach server")
		w.WriteHeader(http.StatusInternalServerError)
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

func TestAppendLogGenerationResetSerializesSameHandleWrite(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	putStarted := make(chan struct{})
	releasePut := make(chan struct{})
	var putCalls, appendCalls int
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls++
			close(putStarted)
			<-releasePut
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
				t.Errorf("append expected revision = %q, want 6", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Errorf("append expected size = %q, want 32", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "frame" {
				t.Errorf("append body = %q, want frame", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: sqliteWALHeaderSize + int64(len("frame"))})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	oldImage := make([]byte, 64)
	copy(oldImage, oldHeader.raw[:])
	copy(oldImage[sqliteWALHeaderSize:], []byte("old-generation-tail"))
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, oldImage); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.DirtySeq = 0
	fh.OrigSize = int64(len(oldImage))
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{initialized: true, appendSafe: true, layout: client.ContentLayoutAppendLog, revision: 5, size: int64(len(oldImage))}
	handleID := fs.fileHandles.Allocate(fh)
	defer fs.fileHandles.Delete(handleID)

	if written, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: 0}, newHeader); status != gofuse.OK || written != uint32(len(newHeader)) {
		t.Fatalf("header write = %d/%d, want %d/OK", written, status, len(newHeader))
	}
	resetDone := make(chan gofuse.Status, 1)
	go func() { resetDone <- fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}) }()
	<-putStarted

	writeStarted := make(chan struct{})
	writeDone := make(chan struct{})
	var written uint32
	var writeStatus gofuse.Status
	go func() {
		close(writeStarted)
		written, writeStatus = fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: sqliteWALHeaderSize}, []byte("frame"))
		close(writeDone)
	}()
	<-writeStarted
	select {
	case <-writeDone:
		t.Fatal("same-handle write completed before reset finalization")
	default:
	}

	close(releasePut)
	if status := <-resetDone; status != gofuse.OK {
		t.Fatalf("reset Fsync = %d, want OK", status)
	}
	if <-writeDone; writeStatus != gofuse.OK || written != uint32(len("frame")) {
		t.Fatalf("frame write = %d/%d, want %d/OK", written, writeStatus, len("frame"))
	}
	if fh.OrigSize != sqliteWALHeaderSize || fh.Dirty.Size() != sqliteWALHeaderSize+int64(len("frame")) || !fh.appendLog.appendSafe {
		t.Fatalf("post-reset write state = %+v size=%d", fh, fh.Dirty.Size())
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{Fh: handleID}); status != gofuse.OK {
		t.Fatalf("first-frame Fsync = %d, want OK", status)
	}
	if putCalls != 1 || appendCalls != 1 {
		t.Fatalf("put/append calls = %d/%d, want 1/1", putCalls, appendCalls)
	}
}

func TestAppendLogH0ProbePreservesEarlierFrameFence(t *testing.T) {
	oldHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()
	image := make([]byte, 64)
	copy(image, oldHeader)
	fh.Dirty = NewWriteBuffer(fh.Path, 1024, 0)
	if _, err := fh.Dirty.Write(0, image); err != nil {
		t.Fatal(err)
	}
	fh.Dirty.ClearDirty()
	fh.DirtySeq = 0
	fh.OrigSize = int64(len(image))
	fh.BaseRev = 5
	fh.appendLog = appendLogHandleState{initialized: true, appendSafe: true, layout: client.ContentLayoutAppendLog, revision: 5, size: int64(len(image))}
	handleID := fs.fileHandles.Allocate(fh)
	defer fs.fileHandles.Delete(handleID)

	if written, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: sqliteWALHeaderSize}, []byte("frame")); status != gofuse.OK || written != uint32(len("frame")) {
		t.Fatalf("frame write = %d/%d, want %d/OK", written, status, len("frame"))
	}
	if !fh.appendLog.sqliteWALWriteBeyondHeader {
		t.Fatal("frame write did not establish the reset fence")
	}
	if written, status := fs.Write(nil, &gofuse.WriteIn{Fh: handleID, Offset: 0}, newHeader); status != gofuse.OK || written != uint32(len(newHeader)) {
		t.Fatalf("header write = %d/%d, want %d/OK", written, status, len(newHeader))
	}
	if !fh.appendLog.sqliteWALWriteBeyondHeader {
		t.Fatal("H0 probe cleared the earlier frame reset fence")
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
			t.Errorf("method = %s, want PUT", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, newHeader) {
			t.Errorf("reset body = %x, want %x", body, newHeader)
			w.WriteHeader(http.StatusInternalServerError)
			return
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

func TestAppendLogReleaseDoesNotDoubleUnpinRotatedShadow(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeaderBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	newHeader, ok := parseSQLiteWALHeader(newHeaderBytes)
	if !ok {
		t.Fatal("new header did not parse")
	}
	fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	oldImage := append(append([]byte(nil), oldHeader.raw[:]...), []byte("old-tail")...)
	if err := shadow.WriteFull(fh.Path, oldImage, fh.BaseRev); err != nil {
		t.Fatal(err)
	}
	ownerGen := shadow.Pin(fh.Path)
	readerGen := shadow.Pin(fh.Path)
	fh.ShadowReady = true
	fh.ShadowSpill = true
	fh.ShadowPinned = true
	fh.ShadowGen = ownerGen

	fh.Lock()
	fs.rotateAppendLogGenerationShadowLocked(fh, fh.Path, newHeader, 6)
	fh.Unlock()
	fs.releaseHandleShadowPin(fh)

	read := make([]byte, len(oldImage))
	if n, err := shadow.ReadAtGen(readerGen, 0, read); err != nil || n != len(read) || !bytes.Equal(read, oldImage) {
		t.Fatalf("reader generation after owner release = %d/%v/%x, want old shadow", n, err, read)
	}
	shadow.Unpin(readerGen)
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

func TestAppendLogUnownedAppendClearsLiveSiblingShadow(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	var resetCalls, appendCalls int
	fs, owner, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			resetCalls++
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			appendCalls++
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: sqliteWALHeaderSize + int64(len("tail"))})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	setGenerationResetDirty(t, owner, oldHeader, newHeader, 64)
	oldImage := make([]byte, 64)
	copy(oldImage, newHeader)
	if err := shadow.WriteFull(owner.Path, oldImage, owner.BaseRev); err != nil {
		t.Fatal(err)
	}
	owner.ShadowReady = true
	owner.ShadowSpill = true
	fs.openHandles.Add(owner)
	defer fs.openHandles.Remove(owner)

	owner.Lock()
	reset := fs.tryAppendLogGenerationResetLocked(context.Background(), owner)
	owner.Unlock()
	if reset.route != appendLogRouteCommitted || reset.status != gofuse.OK {
		t.Fatalf("reset = %+v, want committed", reset)
	}
	if !owner.ShadowReady || !owner.ShadowSpill || !shadow.Has(owner.Path) {
		t.Fatal("reset did not establish the owner shadow fixture")
	}

	sibling := &FileHandle{
		Ino:      2,
		Path:     owner.Path,
		Dirty:    NewWriteBuffer(owner.Path, 1024, 0),
		DirtySeq: 1,
		OrigSize: sqliteWALHeaderSize,
		BaseRev:  6,
		appendLog: appendLogHandleState{
			initialized: true,
			appendSafe:  true,
			layout:      client.ContentLayoutAppendLog,
			revision:    6,
			size:        sqliteWALHeaderSize,
		},
	}
	if _, err := sibling.Dirty.Write(0, append(append([]byte(nil), newHeader...), []byte("tail")...)); err != nil {
		t.Fatal(err)
	}
	fs.openHandles.Add(sibling)
	defer fs.openHandles.Remove(sibling)

	sibling.Lock()
	appendResult := fs.tryAppendLogLocked(context.Background(), sibling)
	sibling.Unlock()
	if appendResult.route != appendLogRouteCommitted || appendResult.status != gofuse.OK {
		t.Fatalf("sibling append = %+v, want committed", appendResult)
	}
	if owner.ShadowReady || owner.ShadowSpill {
		t.Fatalf("owner retained removed shadow flags: ready=%t spill=%t", owner.ShadowReady, owner.ShadowSpill)
	}
	if owner.BaseRev != 7 || owner.Dirty.Size() != sqliteWALHeaderSize+int64(len("tail")) {
		t.Fatalf("owner rebind = base=%d size=%d, want 7/%d", owner.BaseRev, owner.Dirty.Size(), sqliteWALHeaderSize+len("tail"))
	}
	if resetCalls != 1 || appendCalls != 1 {
		t.Fatalf("reset/append calls = %d/%d, want 1/1", resetCalls, appendCalls)
	}
}

func TestAppendLogRefreshClearsRemovedCleanSiblingShadow(t *testing.T) {
	fs, owner, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	owner.Dirty = NewWriteBuffer(owner.Path, 1024, 0)
	if _, err := owner.Dirty.Write(0, []byte("clean")); err != nil {
		t.Fatal(err)
	}
	owner.Dirty.ClearDirty()
	owner.DirtySeq = 0
	owner.BaseRev = 6
	owner.OrigSize = 5
	owner.ShadowReady = true
	owner.ShadowSpill = true
	fs.openHandles.Add(owner)
	defer fs.openHandles.Remove(owner)

	// This models an owner skipped by the first TryLock cleanup loop after a
	// sibling removed the active path shadow. The later revision refresh must
	// clear the now-invalid local source before publishing the new revision.
	fs.refreshCommittedRevisionForOpenHandlesWithSize(owner.Path, 7, nil, 9)
	if owner.ShadowReady || owner.ShadowSpill {
		t.Fatalf("refresh retained removed shadow flags: ready=%t spill=%t", owner.ShadowReady, owner.ShadowSpill)
	}
	if owner.BaseRev != 7 || owner.Dirty.Size() != 9 {
		t.Fatalf("refresh rebind = base=%d size=%d, want 7/9", owner.BaseRev, owner.Dirty.Size())
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
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
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
	fh.Dirty.OnPartFull = func(int, []byte) {}

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
	if fh.Dirty.OnPartFull != nil {
		t.Fatal("shadow degradation retained the part-eviction callback")
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
				t.Errorf("reset body = %x, want H1 %x", body, newHeader)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Errorf("append expected size = %q, want 32", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, frame) {
				t.Errorf("append body length = %d, want frame length %d", len(body), len(frame))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: sqliteWALHeaderSize + int64(len(frame))})
		case http.MethodGet:
			remoteReadCalls++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
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

	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		fh.Lock()
		unlockRemoteCommit := fs.lockHandleRemoteCommitPathLocked(fh)
		unlockRemoteCommit()
		fh.Unlock()
		close(writerDone)
	}()
	<-writerStarted
	select {
	case <-writerDone:
		t.Fatal("writer completed before reset finalization")
	default:
	}
	close(releaseResponse)
	select {
	case result := <-resetDone:
		if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
			t.Fatalf("reset = %+v, want committed", result)
		}
	case <-time.After(time.Second):
		t.Fatal("generation reset did not complete")
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
				t.Errorf("reset body = %x, want %x", body, newHeader)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			appendCalls++
			if got := r.Header.Get("X-Dat9-Expected-Size"); got != "32" {
				t.Errorf("append expected size = %q, want 32", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if got := string(body); got != "frame" {
				t.Errorf("append body = %q, want frame", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 37})
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
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
			t.Errorf("method = %s, want complete-image PUT", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		putCalls++
		body, _ := io.ReadAll(r.Body)
		if len(body) != 64 || !bytes.Equal(body[:sqliteWALHeaderSize], newHeader) || string(body[sqliteWALHeaderSize:sqliteWALHeaderSize+len("frame")]) != "frame" {
			t.Errorf("full rewrite body = %x, want H1 plus frame in 64-byte image", body)
			w.WriteHeader(http.StatusInternalServerError)
			return
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
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
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
			t.Errorf("create body = %x, want %x", body, headerBytes)
			w.WriteHeader(http.StatusInternalServerError)
			return
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
			t.Errorf("method = %s, want POST", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if got := string(body); got != "tail" {
			t.Errorf("tail body = %q, want tail", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
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
