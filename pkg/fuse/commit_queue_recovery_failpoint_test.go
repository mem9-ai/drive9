//go:build failpoint

package fuse

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestRecoveredBatchAdmission(t *testing.T) {
	for _, mode := range []string{"all_valid", "mixed", "one_survivor", "all_rejected", "cleanup_shortcut", "initial_replacement", "prepare_conflict", "prepare_all_rejected", "fallback_replacement", "missing_metadata", "zero_generation"} {
		t.Run(mode, func(t *testing.T) {
			paths := []string{"/a.txt", "/b.txt", "/c.txt"}
			var mu sync.Mutex
			uploaded := make(map[string][]string)
			batches, puts := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
					_, _ = io.WriteString(w, `{"inline_threshold":50000}`)
				case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-write":
					var req struct {
						Items []client.BatchWriteItem `json:"items"`
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					results := make([]map[string]any, 0, len(req.Items))
					mu.Lock()
					batches++
					for _, item := range req.Items {
						uploaded[item.Path] = append(uploaded[item.Path], string(item.Data))
						results = append(results, map[string]any{"path": item.Path, "status": 200, "revision": 1})
					}
					mu.Unlock()
					_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
				case r.Method == http.MethodPut:
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
					mu.Lock()
					puts++
					uploaded[p] = append(uploaded[p], string(data))
					mu.Unlock()
					_, _ = io.WriteString(w, `{"revision":1}`)
				default:
					t.Errorf("unexpected remote call %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			shadow, err := NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			index, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			generation := make(map[string]uint64)
			payload := make(map[string]string)
			for _, p := range paths {
				payload[p] = "old:" + p
				if err := shadow.WriteFull(p, []byte(payload[p]), 0); err != nil {
					t.Fatal(err)
				}
				generation[p], err = index.PutWithBaseRev(p, int64(len(payload[p])), PendingNew, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			cq := NewCommitQueue(newTestClient(server.URL), shadow, index, nil, 1, 8)
			cq.ConfigureBatchWrite(time.Second, 3, client.MaxBatchWriteBytes)
			expectedKept := make(map[string]PendingKind)
			callbacks := map[string]map[string]int{"success": {}, "uploaded": {}, "cleanup": {}, "discard": {}}
			record := func(kind, p string) { callbacks[kind][p]++ }
			cq.OnSuccess = func(e *CommitEntry, _ int64) { record("success", e.Path) }
			cq.OnUploaded = func(e *CommitEntry, _ int64) { record("uploaded", e.Path) }
			cq.OnCleanup = func(e *CommitEntry) { record("cleanup", e.Path) }
			cq.OnDiscard = func(e *CommitEntry) { record("discard", e.Path) }
			if mode == "cleanup_shortcut" {
				cq.IsSuperseded = func(e *CommitEntry) bool { return e.Path == "/a.txt" }
			}
			locks := map[string]*sync.Mutex{}
			for _, p := range paths {
				locks[p] = &sync.Mutex{}
			}
			cq.PathLock = func(p string) func() { locks[p].Lock(); return locks[p].Unlock }
			mark := func(p string) {
				marked, err := index.MarkConflictIfGeneration(p, generation[p])
				if err != nil || !marked {
					t.Errorf("mark %s: %t/%v", p, marked, err)
				}
				expectedKept[p] = PendingConflict
			}
			restage := func(p string) {
				payload[p] = "new:" + p
				if err := shadow.WriteFull(p, []byte(payload[p]), 0); err != nil {
					t.Error(err)
				}
				var err error
				generation[p], err = index.PutWithBaseRev(p, int64(len(payload[p])), PendingNew, 0)
				if err != nil {
					t.Error(err)
				}
				expectedKept[p] = PendingNew
			}
			var preparedOnce, replacedOnce bool
			rejectedFallbacks := 0
			enableRecoveredBatchHook(t, cq, func(observed *CommitQueue, phase string, entry *CommitEntry) {
				if observed != cq {
					return
				}
				switch phase {
				case "batch_locked":
					for _, p := range paths {
						if locks[p].TryLock() {
							locks[p].Unlock()
							t.Errorf("batch lacks path exclusion: %s", p)
						}
					}
					switch mode {
					case "mixed", "cleanup_shortcut":
						mark("/a.txt")
					case "one_survivor":
						mark("/a.txt")
						mark("/b.txt")
					case "all_rejected":
						for _, p := range paths {
							mark(p)
						}
					case "initial_replacement":
						restage("/a.txt")
					case "missing_metadata":
						index.Remove("/a.txt")
						expectedKept["/a.txt"] = PendingNew
					case "zero_generation":
						// Legacy metadata can be recovered without a usable identity token.
						// The entry must be refused without an unconditional conflict write.
						cq.mu.Lock()
						for _, e := range cq.queue {
							if e.Path == "/a.txt" {
								e.PendingIndexGen = 0
							}
						}
						cq.mu.Unlock()
						expectedKept["/a.txt"] = PendingNew
					}
				case "payload_prepared":
					if preparedOnce {
						return
					}
					preparedOnce = true
					if mode == "prepare_conflict" || mode == "fallback_replacement" {
						// Invalidate an item that has not been read yet, after one request
						// item has already been assembled. Entry order may vary in recovery.
						victim := "/a.txt"
						if entry.Path == victim {
							victim = "/b.txt"
						}
						mark(victim)
					} else if mode == "prepare_all_rejected" {
						for _, p := range paths {
							mark(p)
						}
					}
				case "fallback":
					if _, rejected := expectedKept[entry.Path]; rejected {
						rejectedFallbacks++
					}
					// Batch exclusions must be gone before single-entry fallback starts.
					if !locks[entry.Path].TryLock() {
						t.Errorf("fallback holds batch path lock: %s", entry.Path)
					} else {
						locks[entry.Path].Unlock()
					}
					if mode == "fallback_replacement" && !replacedOnce {
						if _, invalid := expectedKept[entry.Path]; !invalid {
							replacedOnce = true
							restage(entry.Path)
						}
					}
				}
			})
			cq.RecoverPending()
			// Closing the channel after all three synchronous enqueues makes batch
			// collection deterministic without waiting for its timer to expire.
			done := make(chan struct{})
			go func() { cq.DrainAll(); close(done) }()
			select {
			case <-done:
			case <-time.After(8 * time.Second):
				t.Fatal("batch drain deadlocked")
			}
			mu.Lock()
			defer mu.Unlock()
			for _, p := range paths {
				kind, keep := expectedKept[p]
				if keep {
					if len(uploaded[p]) != 0 {
						t.Errorf("rejected source uploaded: %s %v", p, uploaded[p])
					}
					for label, counts := range callbacks {
						if counts[p] != 0 {
							t.Errorf("rejected %s got %s callback %d", p, label, counts[p])
						}
					}
					meta, ok := index.GetMeta(p)
					if mode == "missing_metadata" && p == "/a.txt" {
						if ok {
							t.Errorf("missing metadata was recreated: %+v", meta)
						}
					} else if !ok || meta.Generation != generation[p] || meta.Kind != kind {
						t.Errorf("retained metadata %s=%+v/%t want gen=%d kind=%v", p, meta, ok, generation[p], kind)
					}
					if data, err := shadow.ReadAll(p); err != nil || string(data) != payload[p] {
						t.Errorf("retained payload %s=%q/%v want=%q", p, data, err, payload[p])
					}
				} else {
					if len(uploaded[p]) != 1 || uploaded[p][0] != payload[p] {
						t.Errorf("valid upload %s=%v want one %q", p, uploaded[p], payload[p])
					}
					if callbacks["success"][p] != 1 || callbacks["uploaded"][p] != 1 || callbacks["cleanup"][p] != 1 || callbacks["discard"][p] != 0 {
						t.Errorf("valid callback counts %s: %+v", p, callbacks)
					}
					if index.HasPending(p) || shadow.Has(p) {
						t.Errorf("committed staging retained for %s", p)
					}
				}
				if !locks[p].TryLock() {
					t.Errorf("path lock leaked: %s", p)
				} else {
					locks[p].Unlock()
				}
			}
			wantBatch, wantPut := 1, 0
			switch mode {
			case "one_survivor":
				wantBatch, wantPut = 0, 1
			case "all_rejected", "prepare_all_rejected":
				wantBatch, wantPut = 0, 0
			case "prepare_conflict":
				wantBatch, wantPut = 0, 2
			case "fallback_replacement":
				wantBatch, wantPut = 0, 1
			}
			if batches != wantBatch || puts != wantPut {
				t.Errorf("RPCs batch=%d PUT=%d want=%d/%d", batches, puts, wantBatch, wantPut)
			}
			if rejectedFallbacks != 0 {
				t.Errorf("%d already-rejected entries entered fallback", rejectedFallbacks)
			}
			cq.mu.Lock()
			q, inflight, byPath := len(cq.queue), len(cq.inFlight), len(cq.queuedByPath)
			cq.mu.Unlock()
			if q != 0 || inflight != 0 || byPath != 0 {
				t.Errorf("queue leak: queued=%d inflight=%d index=%d", q, inflight, byPath)
			}
			if cq.batchConfigSnapshot().window <= 0 {
				t.Error("one rejected entry disabled batching globally")
			}
			t.Logf("BATCH mode=%s batch=%d PUT=%d retained=%d rejected_fallback=%d queue=%d inflight=%d", mode, batches, puts, len(expectedKept), rejectedFallbacks, q, inflight)
		})
	}
}

func enableRecoveredBatchHook(t *testing.T, cq *CommitQueue, hook func(*CommitQueue, string, *CommitEntry)) {
	t.Helper()
	name := "github.com/mem9-ai/drive9/pkg/fuse/recoveredBatchLocked"
	if err := failpoint.EnableCall(name, func(observed *CommitQueue) {
		if observed == cq {
			hook(observed, "batch_locked", nil)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(name) })
	for point, phase := range map[string]string{"batchPayloadPrepared": "payload_prepared", "batchFallbackEntry": "fallback"} {
		name := "github.com/mem9-ai/drive9/pkg/fuse/" + point
		if err := failpoint.EnableCall(name, func(observed *CommitQueue, entry *CommitEntry) {
			if observed == cq {
				hook(observed, phase, entry)
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = failpoint.Disable(name) })
	}
}
