package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestCommitQueueBatchModeRequiresCapability(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    string
		combined  bool
		denyChmod bool
	}{
		{"supported-owner", `{"inline_threshold":50000,"storage_capabilities":{"batch_write_mode_v1":true}}`, true, false},
		{"legacy-owner", `{"inline_threshold":50000}`, false, false},
		{"legacy-scoped", `{"inline_threshold":50000}`, false, true},
		{"unsupported-owner", `{"inline_threshold":50000,"storage_capabilities":{"batch_write_mode_v1":false}}`, false, false},
		{"unsupported-scoped", `{"inline_threshold":50000,"storage_capabilities":{"batch_write_mode_v1":false}}`, false, true},
		{"malformed-status", `{"inline_threshold":50000,`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type remoteFile struct {
				data string
				mode uint32
				rev  int64
			}
			var mu sync.Mutex
			files := map[string]remoteFile{
				"/overwrite": {data: "old", mode: 0o755, rev: 8},
			}
			var batches, chmods, successes int
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
					_, _ = w.Write([]byte(tc.status))
				case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-write":
					batches++
					var req struct {
						Items []client.BatchWriteItem `json:"items"`
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					results := make([]client.BatchWriteResult, len(req.Items))
					for i, item := range req.Items {
						if item.HasMode != tc.combined || (!tc.combined && item.Mode != 0) {
							t.Errorf("unnegotiated mode fields: %+v, combined=%t", item, tc.combined)
						}
						file, exists := files[item.Path]
						if item.ExpectedRevision != file.rev {
							t.Errorf("CAS for %s = %d, want %d", item.Path, item.ExpectedRevision, file.rev)
						}
						if !exists {
							file.mode = defaultRegularFileMode
						}
						// Model an old endpoint that accepts mode without checking
						// authorization: the client must never send it that field.
						if item.HasMode {
							file.mode = item.Mode
						}
						file.data = string(item.Data)
						file.rev++
						files[item.Path] = file
						results[i] = client.BatchWriteResult{Path: item.Path, Status: http.StatusOK, Revision: file.rev}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
				case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
					chmods++
					if tc.denyChmod {
						http.Error(w, `{"error":"chmod is owner-only"}`, http.StatusForbidden)
						return
					}
					var req struct{ Mode uint32 }
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					path := r.URL.Path[len("/v1/fs"):]
					file := files[path]
					if file.data != "new" {
						t.Error("chmod ran before content committed")
					}
					file.mode = req.Mode
					files[path] = file
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			t.Cleanup(ts.Close)
			shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(shadow.Close)
			pending, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			entries := []*CommitEntry{
				{Path: "/executable", Size: 3, Kind: PendingNew, Mode: 0o755, HasMode: true},
				{Path: "/zero", Size: 3, Kind: PendingNew, Mode: 0, HasMode: true},
				{Path: "/overwrite", Size: 3, BaseRev: 8, Kind: PendingOverwrite, Mode: 0o644, HasMode: true},
			}
			for _, entry := range entries {
				if err := shadow.WriteFull(entry.Path, []byte("new"), entry.BaseRev); err != nil {
					t.Fatal(err)
				}
				if _, err := pending.PutWithBaseRevAndMode(entry.Path, entry.Size, entry.Kind, entry.BaseRev, entry.Mode, true); err != nil {
					t.Fatal(err)
				}
				attachTestStagingGens(shadow, pending, entry)
			}
			c := newTestClient(ts.URL)
			c.Warm(context.Background())
			cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
			t.Cleanup(cq.DrainAll)
			cq.OnSuccess = func(*CommitEntry, int64) { successes++ }
			cq.ConfigureBatchWrite(10*time.Millisecond, 64, client.MaxBatchWriteBytes)
			for _, entry := range entries {
				if err := cq.Enqueue(entry); err != nil {
					t.Fatal(err)
				}
			}
			cq.DrainAll()
			mu.Lock()
			defer mu.Unlock()
			wantChmods, wantSuccesses := len(entries), len(entries)
			if tc.combined {
				wantChmods = 0
			}
			if tc.denyChmod {
				wantSuccesses = 0
			}
			if batches != 1 || chmods != wantChmods || successes != wantSuccesses {
				t.Fatalf("batch=%d chmod=%d successes=%d, want 1/%d/%d", batches, chmods, successes, wantChmods, wantSuccesses)
			}
			for _, entry := range entries {
				wantMode := entry.Mode
				if tc.denyChmod {
					wantMode = defaultRegularFileMode
					if entry.Kind == PendingOverwrite {
						wantMode = 0o755
					}
				}
				if got := files[entry.Path]; got.data != "new" || got.mode != wantMode || got.rev != entry.BaseRev+1 {
					t.Errorf("remote %s = %+v, want new/%o/revision %d", entry.Path, got, wantMode, entry.BaseRev+1)
				}
				if pending.HasPending(entry.Path) != tc.denyChmod || shadow.Has(entry.Path) != tc.denyChmod {
					t.Errorf("staging for %s must be retained only when chmod fails", entry.Path)
				}
			}
		})
	}
}
