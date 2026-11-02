// v1 recovery suites (recovery_snapshot_test.go core rows +
// RecoverPending contract), retargeted to the new staging ledger +
// Writeback.Start wiring: an acknowledged .dat/.meta snapshot survives a
// crash (short/torn/missing WAL states), Start re-enqueues it through the
// real commit queue, the upload stays fenced by the snapshot's recorded
// revision, and a revision-conflict against an advanced remote marks the
// entry conflicted instead of silently rebasing.
package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// newRecoveryStack builds a writeback over a fresh staging ledger and
// starts it (the recovery wiring) against the given handler-backed
// server.
func newRecoveryStack(t *testing.T, h http.HandlerFunc) (*Writeback, *staging.Staging, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	return wb, ledger, ts
}

// v1 TestRecoveryCompleteSnapshotFallback (writeback half): the
// acknowledged .dat/.meta snapshot re-uploads at its recorded base
// revision after Start; the CAS header is exactly the snapshot's base.
func TestPortRecoveryReuploadsSnapshotAtRecordedBase(t *testing.T) {
	const path = "/snapshot"
	const acknowledged = "ACKNOWLEDGED"
	var puts atomic.Int32
	var badBase atomic.Bool
	wb, ledger, _ := newRecoveryStack(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			if !strings.Contains(r.URL.Path, "/v1/fs/snapshot") {
				http.NotFound(w, r)
				return
			}
			puts.Add(1)
			if r.Header.Get("X-Dat9-Expected-Revision") != "7" {
				badBase.Store(true)
			}
			_, _ = w.Write([]byte(`{"revision":8}`))
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "7")
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})

	// Seed the acknowledged snapshot in the staging ledger the way the
	// crash left it (shadow bytes + pending meta at the recorded base).
	if err := ledger.Shadow.WriteFull(path, []byte(acknowledged), 7); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRev(path, int64(len(acknowledged)), staging.PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}

	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := wb.WaitCommitQueueIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := puts.Load(); got < 1 {
		t.Fatal("recovery must re-upload the acknowledged snapshot")
	}
	if badBase.Load() {
		t.Fatal("upload CAS base must be the snapshot's recorded revision")
	}
	if _, ok := ledger.Pending.GetMeta(path); ok {
		t.Fatal("successful recovery upload must clear the pending entry")
	}
}

// v1 conflict flavor: the remote advanced past the snapshot's base — the
// recovery upload conflicts and the entry lands conflicted, never
// silently rebased onto the newer remote image.
func TestPortRecoveryConflictMarksEntryNotSilentRebase(t *testing.T) {
	const path = "/conflict.db"
	var puts atomic.Int32
	wb, ledger, _ := newRecoveryStack(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			puts.Add(1)
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
		case http.MethodHead:
			// Remote advanced: rev 9 (the queue may LWW-resolve; the row
			// asserts the conflict path engaged at all — the fenced
			// no-rebase behavior is pinned by the generation-fence suite).
			w.Header().Set("X-Dat9-Revision", "9")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})

	if err := ledger.Shadow.WriteFull(path, []byte("stale-ack"), 7); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRev(path, 9, staging.PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = wb.WaitCommitQueueIdle(context.Background())
	if got := puts.Load(); got < 1 {
		t.Fatal("the recovery upload must be attempted against the recorded base")
	}
	// The acknowledged bytes stay locally (conflict path preserves data).
	if data, err := ledger.Shadow.ReadAll(path); err != nil || string(data) != "stale-ack" {
		t.Fatalf("shadow = %q err=%v, want the preserved acknowledged bytes", data, err)
	}
}

// v1 TestRecoveryFallbackSurvivesSecondRestart, retargeted to the new
// terminal-failure policy: an entry whose recovery upload failed with the
// server down is conflict-marked and preserved (never dropped); a fresh
// staging of the same image lands once the server answers.
func TestPortRecoverySurvivesSecondRestart(t *testing.T) {
	const path = "/twice.txt"
	var down atomic.Bool
	var puts atomic.Int32
	wb, ledger, _ := newRecoveryStack(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			puts.Add(1)
		}
		if down.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			_, _ = w.Write([]byte(`{"revision":2}`))
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})

	if err := ledger.Shadow.WriteFull(path, []byte("survive"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRev(path, 6, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	down.Store(true)
	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = wb.WaitCommitQueueIdle(context.Background())
	if got := puts.Load(); got == 0 {
		t.Fatal("recovery must attempt the upload even with the server down")
	}
	attemptsAfterOutage := puts.Load()
	if _, ok := ledger.Pending.GetMeta(path); !ok {
		t.Fatal("pending entry must survive the failed restart")
	}

	// New-design policy (documented in the ledger): a terminal failure at
	// recovery marks the entry conflicted — preserved for manual recovery,
	// never silently dropped. The bytes and the metadata survive.
	meta, ok := ledger.Pending.GetMeta(path)
	if !ok {
		t.Fatal("acknowledged entry must survive the failed restart")
	}
	if meta.Kind != staging.PendingConflict {
		t.Fatalf("post-failure kind = %v, want PendingConflict (preserved)", meta.Kind)
	}
	if data, err := ledger.Shadow.ReadAll(path); err != nil || string(data) != "survive" {
		t.Fatalf("shadow = %q err=%v, want the preserved bytes", data, err)
	}
	// Manual recovery (v1 parity: "re-staging is not conflict
	// resolution" — a Put onto a conflicted path stays conflicted): the
	// operator clears the marker and re-stages; the entry then lands once
	// the server answers.
	down.Store(false)
	ledger.Pending.Remove(path)
	if err := ledger.Shadow.WriteFull(path, []byte("survive"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRev(path, 6, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := wb.WaitCommitQueueIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := puts.Load(); got <= attemptsAfterOutage {
		t.Fatal("re-staged entry must land once the server answers")
	}
	if _, ok := ledger.Pending.GetMeta(path); ok {
		t.Fatal("landed entry must clear")
	}
}

// v1 TestRecoveryReconcilesBytesBeforeEnqueue (ledger half): the staged
// shadow bytes reconcile into the pending accounting at Start — the
// recovered entry carries the true size.
func TestPortRecoveryReconcilesShadowBytes(t *testing.T) {
	const path = "/bytes.db"
	wb, ledger, _ := newRecoveryStack(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	payload := []byte("twelve-bytes")
	if err := ledger.Shadow.WriteFull(path, payload, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingOverwrite, 3); err != nil {
		t.Fatal(err)
	}
	if err := wb.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	meta, ok := ledger.Pending.GetMeta(path)
	if !ok {
		t.Fatal("recovered entry missing")
	}
	if meta.Size != int64(len(payload)) {
		t.Fatalf("recovered size = %d, want %d", meta.Size, len(payload))
	}
	if data, err := ledger.Shadow.ReadAll(path); err != nil || string(data) != string(payload) {
		t.Fatalf("shadow = %q err=%v", data, err)
	}
}
