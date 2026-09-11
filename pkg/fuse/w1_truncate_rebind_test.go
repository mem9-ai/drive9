package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

// TestW1TruncateCommitRebindsSiblingAppendLogBaseline covers the W1 rebind:
// a clean pre-opened WAL sibling that applied a zero-truncate adopts the
// committed revision together with the append-log baseline, and a dirty
// sibling never does.
func TestW1TruncateCommitRebindsSiblingAppendLogBaseline(t *testing.T) {
	fs, owner, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer closeServer()

	sibling := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  5,
		OrigSize: 4096,
	}
	sibling.appendLog = appendLogHandleState{
		initialized: true,
		layout:      client.ContentLayoutAppendLog,
		revision:    5,
		size:        4096,
	}
	sibling.appendLogRecordTruncate()
	if !sibling.appendLog.hasRewriteBase || sibling.appendLog.rewriteBaseRevision != 5 ||
		sibling.appendLog.rewriteBaseSize != 4096 || !sibling.appendLog.sqliteWALTruncated || sibling.appendLog.appendSafe {
		t.Fatalf("precondition appendLog = %+v", sibling.appendLog)
	}
	fs.openHandles.Add(sibling)
	defer fs.openHandles.Remove(sibling)

	// A dirty sibling must not silently adopt the newer base.
	dirty := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  5,
		OrigSize: 4096,
	}
	if _, err := dirty.Dirty.Write(0, []byte("dirty")); err != nil {
		t.Fatal(err)
	}
	dirty.DirtySeq = 2
	dirty.appendLog = sibling.appendLog
	fs.openHandles.Add(dirty)
	defer fs.openHandles.Remove(dirty)

	fs.refreshCommittedRevisionForOpenHandlesWithSize(sibling.Path, 7, owner, 0)

	if sibling.BaseRev != 7 || sibling.OrigSize != 0 {
		t.Fatalf("sibling base/size = %d/%d, want 7/0", sibling.BaseRev, sibling.OrigSize)
	}
	if sibling.appendLog.hasRewriteBase || sibling.appendLog.rewriteBaseRevision != 0 ||
		sibling.appendLog.rewriteBaseSize != 0 || sibling.appendLog.sqliteWALTruncated ||
		!sibling.appendLog.appendSafe || sibling.appendLog.revision != 7 || sibling.appendLog.size != 0 {
		t.Fatalf("sibling appendLog after rebind = %+v", sibling.appendLog)
	}
	if dirty.BaseRev != 5 || !dirty.appendLog.sqliteWALTruncated || dirty.appendLog.appendSafe ||
		dirty.appendLog.rewriteBaseRevision != 5 {
		t.Fatalf("dirty sibling was adopted: base=%d appendLog=%+v", dirty.BaseRev, dirty.appendLog)
	}
}

// TestW1TruncateCommitRebindsShadowSpilledSibling proves the rebind runs even
// when the shadow-removed early-continue fires, so a shadow-spilled sibling
// does not keep the stale baseline.
func TestW1TruncateCommitRebindsShadowSpilledSibling(t *testing.T) {
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

	sibling := &FileHandle{
		Ino:         1,
		Path:        "/db-wal",
		Dirty:       NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:     5,
		OrigSize:    4096,
		ShadowReady: true,
		ShadowSpill: true,
	}
	sibling.appendLog = appendLogHandleState{
		initialized: true,
		layout:      client.ContentLayoutAppendLog,
		revision:    5,
		size:        4096,
	}
	sibling.appendLogRecordTruncate()
	fs.openHandles.Add(sibling)
	defer fs.openHandles.Remove(sibling)

	fs.refreshCommittedRevisionForOpenHandlesWithSize(sibling.Path, 7, owner, 0)

	if sibling.ShadowReady || sibling.ShadowSpill {
		t.Fatalf("removed shadow flags retained: ready=%t spill=%t", sibling.ShadowReady, sibling.ShadowSpill)
	}
	if sibling.BaseRev != 7 || sibling.OrigSize != 0 {
		t.Fatalf("sibling base/size = %d/%d, want 7/0", sibling.BaseRev, sibling.OrigSize)
	}
	if sibling.appendLog.hasRewriteBase || sibling.appendLog.sqliteWALTruncated || !sibling.appendLog.appendSafe {
		t.Fatalf("shadow-spilled sibling appendLog not rebound: %+v", sibling.appendLog)
	}
}

// TestW1GenerationResetFiresOnZeroByteBaseline proves that after the rebind
// the writer's 32-byte header write takes the generation-reset route against
// the new revision even though the committed size is zero.
func TestW1GenerationResetFiresOnZeroByteBaseline(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeaderBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	newHeader, ok := parseSQLiteWALHeader(newHeaderBytes)
	if !ok {
		t.Fatal("new header did not parse")
	}

	var putCalls int
	fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		putCalls++
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "7" {
			t.Errorf("reset expected revision = %q, want 7", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, newHeaderBytes) {
			t.Errorf("reset body = %x, want header %x", body, newHeaderBytes)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 8})
	})
	defer closeServer()

	sibling := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  7,
		OrigSize: 0,
	}
	sibling.appendLog = appendLogHandleState{
		initialized:              true,
		appendSafe:               true,
		layout:                   client.ContentLayoutAppendLog,
		revision:                 7,
		size:                     0,
		sqliteWALConfirmed:       true,
		sqliteWALCommittedHeader: oldHeader,
	}
	if _, err := sibling.Dirty.Write(0, newHeaderBytes); err != nil {
		t.Fatal(err)
	}
	sibling.appendLogRecordUserWrite(0, 0, sqliteWALHeaderSize)

	sibling.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), sibling)
	sibling.Unlock()
	if result.route != appendLogRouteCommitted || result.status != gofuse.OK {
		t.Fatalf("reset route = %+v, want committed/OK", result)
	}
	if putCalls != 1 {
		t.Fatalf("reset put calls = %d, want 1", putCalls)
	}
	if sibling.BaseRev != 8 || sibling.OrigSize != sqliteWALHeaderSize {
		t.Fatalf("reset base/size = %d/%d, want 8/%d", sibling.BaseRev, sibling.OrigSize, sqliteWALHeaderSize)
	}
	if sibling.appendLog.sqliteWALTruncated || !sibling.appendLog.appendSafe ||
		sibling.appendLog.sqliteWALCommittedHeader != newHeader {
		t.Fatalf("reset appendLog = %+v", sibling.appendLog)
	}
}

// TestW1SaltsUnchangedHeaderFallsBackWithoutShadowRotation pins the salts-equal
// fallback: no generation reset, a layout-aware full rewrite publishes the
// 32-byte object, and the shadow is not rotated.
func TestW1SaltsUnchangedHeaderFallsBackWithoutShadowRotation(t *testing.T) {
	headerBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	header, ok := parseSQLiteWALHeader(headerBytes)
	if !ok {
		t.Fatal("header did not parse")
	}

	var putCalls int
	fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		putCalls++
		if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "7" {
			t.Errorf("rewrite expected revision = %q, want 7", got)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, headerBytes) {
			t.Errorf("rewrite body = %x, want header %x", body, headerBytes)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 8})
	})
	defer closeServer()

	sibling := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  7,
		OrigSize: 0,
	}
	sibling.appendLog = appendLogHandleState{
		initialized:              true,
		appendSafe:               true,
		layout:                   client.ContentLayoutAppendLog,
		revision:                 7,
		size:                     0,
		sqliteWALConfirmed:       true,
		sqliteWALCommittedHeader: header,
	}
	if _, err := sibling.Dirty.Write(0, headerBytes); err != nil {
		t.Fatal(err)
	}
	sibling.appendLogRecordUserWrite(0, 0, sqliteWALHeaderSize)

	sibling.Lock()
	reset := fs.tryAppendLogGenerationResetLocked(context.Background(), sibling)
	sibling.Unlock()
	if reset.route != appendLogRouteNotApplicable {
		t.Fatalf("reset route = %+v, want not applicable for unchanged salts", reset)
	}

	sibling.Lock()
	rewrite := fs.tryAppendLogFullRewriteLocked(context.Background(), sibling)
	sibling.Unlock()
	if rewrite.route != appendLogRouteCommitted || rewrite.status != gofuse.OK {
		t.Fatalf("rewrite route = %+v, want committed/OK", rewrite)
	}
	if putCalls != 1 {
		t.Fatalf("rewrite put calls = %d, want 1", putCalls)
	}
	if sibling.ShadowReady || sibling.ShadowSpill {
		t.Fatalf("shadow rotated on salts-equal fallback: ready=%t spill=%t", sibling.ShadowReady, sibling.ShadowSpill)
	}
	if sibling.BaseRev != 8 || sibling.OrigSize != sqliteWALHeaderSize {
		t.Fatalf("rewrite base/size = %d/%d, want 8/%d", sibling.BaseRev, sibling.OrigSize, sqliteWALHeaderSize)
	}
}

// TestW1TruncateCommitThenWriterHeaderTakesGenerationReset covers the full
// chain the EC2 harness did not exercise: a writer that opened the WAL at
// creation (H0 proven) survives a sibling TRUNCATE checkpoint, adopts the new
// revision through the W1 rebind, and its next 32-byte header write is routed
// through the generation reset (conditional PUT) rather than tail append.
func TestW1TruncateCommitThenWriterHeaderTakesGenerationReset(t *testing.T) {
	oldHeader, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
	if !ok {
		t.Fatal("old header did not parse")
	}
	newHeaderBytes := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	newHeader, ok := parseSQLiteWALHeader(newHeaderBytes)
	if !ok {
		t.Fatal("new header did not parse")
	}

	var putCalls, appendCalls int
	fs, _, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putCalls++
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "6" {
				t.Errorf("reset expected revision = %q, want 6", got)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, newHeaderBytes) {
				t.Errorf("reset body = %x, want header %x", body, newHeaderBytes)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 7})
		case http.MethodPost:
			appendCalls++
			t.Errorf("unexpected tail append request")
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer closeServer()

	checkpointer := &FileHandle{Ino: 1, Path: "/db-wal", Dirty: NewWriteBuffer("/db-wal", 1024, 0), BaseRev: 5}
	fs.openHandles.Add(checkpointer)
	defer fs.openHandles.Remove(checkpointer)

	// Writer that opened the WAL at creation: H0 proven, append-log active.
	writer := &FileHandle{
		Ino:      1,
		Path:     "/db-wal",
		Dirty:    NewWriteBuffer("/db-wal", 1024, 0),
		BaseRev:  5,
		OrigSize: 4096,
	}
	writer.appendLog = appendLogHandleState{
		initialized:              true,
		appendSafe:               true,
		layout:                   client.ContentLayoutAppendLog,
		revision:                 5,
		size:                     4096,
		sqliteWALConfirmed:       true,
		sqliteWALCommittedHeader: oldHeader,
	}
	// The sibling zero-truncate event is applied on the writer.
	writer.appendLogRecordTruncate()
	fs.openHandles.Add(writer)
	defer fs.openHandles.Remove(writer)

	// The checkpointer's truncate commit publishes revision 6, size 0.
	fs.refreshCommittedRevisionForOpenHandlesWithSize(writer.Path, 6, checkpointer, 0)

	if writer.BaseRev != 6 || writer.OrigSize != 0 {
		t.Fatalf("writer base/size after rebind = %d/%d, want 6/0", writer.BaseRev, writer.OrigSize)
	}
	if writer.appendLog.sqliteWALTruncated || !writer.appendLog.appendSafe ||
		writer.appendLog.hasRewriteBase || writer.appendLog.rewriteBaseRevision != 0 {
		t.Fatalf("writer appendLog after rebind = %+v", writer.appendLog)
	}
	if writer.appendLog.sqliteWALCommittedHeader != oldHeader {
		t.Fatal("H0 was not preserved across the rebind")
	}

	// The writer writes the new generation header.
	if _, err := writer.Dirty.Write(0, newHeaderBytes); err != nil {
		t.Fatal(err)
	}
	writer.appendLogRecordUserWrite(0, 0, sqliteWALHeaderSize)

	writer.Lock()
	handled, status, fullRewrite := fs.routeAppendLogLocked(context.Background(), writer)
	writer.Unlock()
	if !handled || status != gofuse.OK || !fullRewrite {
		t.Fatalf("route = handled=%t status=%d fullRewrite=%t, want true/OK/true", handled, status, fullRewrite)
	}
	if putCalls != 1 {
		t.Fatalf("reset put calls = %d, want 1", putCalls)
	}
	if appendCalls != 0 {
		t.Fatalf("tail append calls = %d, want 0 (must take the generation-reset route)", appendCalls)
	}
	if writer.BaseRev != 7 || writer.OrigSize != sqliteWALHeaderSize {
		t.Fatalf("writer base/size = %d/%d, want 7/%d", writer.BaseRev, writer.OrigSize, sqliteWALHeaderSize)
	}
	if writer.appendLog.sqliteWALCommittedHeader != newHeader {
		t.Fatal("reset did not adopt the new committed header")
	}
}
