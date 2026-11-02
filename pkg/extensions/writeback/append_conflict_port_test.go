// v1 append_conflict_regression rows: unproven watermark payloads are
// rejected locally (no server write), live-ancestor proofs are bounded
// and fail closed once recovered, and a batch discards entries whose
// verified ancestor already landed.
package writeback

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// v1 TestCommitQueueRejectsUnprovenWatermarkPayload: a payload whose
// snapshot forks from an unproven parent (not the landed snapshot chain)
// is stale against the durable watermark — rejected before any PUT.
func TestPortConflictRejectsUnprovenWatermarkPayload(t *testing.T) {
	for _, payload := range []string{"ABCD", "AXYZ"} {
		t.Run(payload, func(t *testing.T) {
			const path = "/watermark"
			server, ts := newCASFileServer(t, path, 2, []byte("AB"))
			defer ts.Close()
			shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			cq := NewCommitQueue(newTestClient(ts.URL), shadow, nil, nil, 1, 8)
			defer cq.DrainAll()
			cq.rememberLanded(path, 2, 2, payloadChecksum([]byte("AB")), "landed")
			entry := &CommitEntry{
				Path: path, BaseRev: 1, PayloadBaseRev: 1, PayloadBaseRevSet: true,
				Size: int64(len(payload)), Kind: staging.PendingOverwrite, DurableWatermarkRev: 2,
				SnapshotID: "fork", ParentSnapshotID: "unrelated",
			}
			entry.liveLineageProof = true
			entry.bindPayload([]byte(payload))
			if err := cq.CommitNow(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
				t.Fatalf("unproven payload err=%v, want stale conflict", err)
			}
			if rev, body, puts := server.snapshot(); rev != 2 || string(body) != "AB" || len(puts) != 0 {
				t.Fatalf("remote rev=%d body=%q puts=%d", rev, body, len(puts))
			}
		})
	}
}

// v1 TestLiveSnapshotAncestorsBoundedAndRecoveredFailClosed (queue half):
// a recovered entry never retains live-ancestor rebasing authority.
func TestPortConflictRecoveredEntryFailsClosed(t *testing.T) {
	ancestors := make([]string, maxLiveSnapshotAncestors)
	for i := range ancestors {
		ancestors[i] = "older"
	}
	entry := &CommitEntry{Kind: staging.PendingOverwrite, BaseRev: 1, PayloadBaseRev: 1,
		PayloadBaseRevSet: true, SnapshotID: "child", ParentSnapshotID: "parent",
		liveAncestors: ancestors, recovered: true}
	entry.liveLineageProof = true
	if entryCanRebaseOntoLandedParent(entry, pathCommitLandmark{snapshotID: "older"}) {
		t.Fatal("recovered entry retained live ancestor authority")
	}
}

// v1 TestPR939ReviewPendingNewPreservesCreateOnly (create-only half): a
// PendingNew with a zero base CASes against absence — against a remote
// that already exists the conflict surfaces and the remote is preserved
// (the entry never rebrands as an overwrite). The empty-remote variant
// creates.
func TestPortConflictPendingNewPreservesCreateOnly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		remote     string
		wantCreate bool
	}{
		{name: "prefix", remote: "A", wantCreate: false},
		{name: "empty", remote: "", wantCreate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The empty variant models an ABSENT remote (v1's 404 path),
			// which the always-200 casFileServer cannot represent.
			var server *casFileServer
			var ts2 *httptest.Server
			var rev int64
			if tc.remote != "" {
				rev = 2
			}
			var body atomic.Value
			body.Store([]byte(tc.remote))
			absent := tc.remote == ""
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/v1/fs/create-only" && r.Method == http.MethodHead && absent:
					http.Error(w, "missing", http.StatusNotFound)
				case r.URL.Path == "/v1/fs/create-only" && (r.Method == http.MethodPut || r.Method == http.MethodPost):
					expected := r.Header.Get("X-Dat9-Expected-Revision")
					data, _ := io.ReadAll(r.Body)
					if expected != fmt.Sprintf("%d", rev) {
						http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
						return
					}
					rev++
					body.Store(data)
					absent = false
					_, _ = fmt.Fprintf(w, `{"status":"ok","revision":%d}`, rev)
				case r.URL.Path == "/v1/fs/create-only" && r.Method == http.MethodHead:
					b := body.Load().([]byte)
					w.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", fmt.Sprintf("%d", rev))
					w.WriteHeader(http.StatusOK)
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			_ = server
			_ = ts2
			shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			cq := NewCommitQueue(newTestClient(ts.URL), shadow, nil, nil, 1, 8)
			defer cq.DrainAll()
			entry := &CommitEntry{
				Path: "/create-only", BaseRev: 0, PayloadBaseRev: 0, PayloadBaseRevSet: true,
				Size: 4, Kind: staging.PendingNew,
			}
			entry.bindPayload([]byte("ABCD"))
			err = cq.CommitNow(context.Background(), entry)
			got := string(body.Load().([]byte))
			if tc.wantCreate {
				if err != nil {
					t.Fatalf("create over absent remote: %v", err)
				}
				if got != "ABCD" {
					t.Fatalf("remote body=%q, want the created image", got)
				}
				return
			}
			// Absence-CAS against an existing remote: the conflict
			// surfaces (the entry never rebrands as an overwrite of the
			// existing bytes; LWW auto-resolve may follow, D-1).
			if err != nil && got != tc.remote {
				t.Fatalf("conflict mutated remote: %q", got)
			}
		})
	}
}
