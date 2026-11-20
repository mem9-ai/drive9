package writeback

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// This mock enforces the server's terminal session and reservation contract.
// Replaying initiation cannot revive an aborted session or reserve it twice.
func TestFixedQueueTransferFailureRenewsOnlyAfterConfirmedAbort(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown-cancel-%t", unknown), func(t *testing.T) {
			dir := t.TempDir()
			var mu sync.Mutex
			var cancelUnavailable atomic.Bool
			cancelUnavailable.Store(unknown)
			ids := map[string]string{}
			aborted := map[string]bool{}
			reserved := map[string]bool{}
			abortIDs := map[string][]string{}
			completeIDs := []string{}
			originalID := ""
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				id := r.Header.Get(viewprotocol.OperationHeader)
				path := strings.TrimPrefix(r.URL.Path, "/v1/views/a/v2/uploads/")
				if path == "initiate" {
					upload, ok := ids[id]
					if !ok {
						for _, active := range reserved {
							if active {
								t.Error("new reservation before old release")
							}
						}
						upload = fmt.Sprintf("upload-%d", len(ids)+1)
						ids[id] = upload
						reserved[upload] = true
						if originalID == "" {
							originalID = id
						}
					}
					w.WriteHeader(202)
					_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": upload, "content_layout": "extent", "part_size": 4 << 20, "total_parts": 1, "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "checksum_contract": map[string]any{"required": true, "supported": []string{"SHA-256"}}})
					return
				}
				pair := strings.Split(path, "/")
				if len(pair) != 2 {
					t.Error("unexpected request")
					w.WriteHeader(400)
					return
				}
				upload := pair[0]
				switch pair[1] {
				case "presign":
					if upload == "upload-1" {
						if aborted[upload] {
							w.WriteHeader(409)
						} else {
							w.WriteHeader(503)
						}
						return
					}
					var req struct {
						Number   int `json:"part_number"`
						Checksum struct{ Value string }
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					_ = json.NewEncoder(w).Encode(client.PartURL{Number: req.Number, Size: 6, ChecksumSHA256: req.Checksum.Value, SkipUpload: true, ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)})
				case "abort":
					aborted[upload] = true
					abortIDs[upload] = append(abortIDs[upload], id)
					if cancelUnavailable.Load() {
						w.WriteHeader(503)
						return
					}
					reserved[upload] = false
					_ = json.NewEncoder(w).Encode(client.FixedUploadOutcome{OperationID: id, UploadID: upload, Status: "ABORTED"})
				case "complete":
					if upload == "upload-1" || aborted[upload] {
						t.Error("completed canceled session")
						w.WriteHeader(409)
						return
					}
					reserved[upload] = false
					completeIDs = append(completeIDs, id)
					_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: id, View: client.FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 7, Size: 6})
				default:
					t.Error("unexpected request")
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			c, _ := client.New(server.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			w, l := fixedQueueRuntime(t, c, dir)
			j := l.Journal
			entry := stageFixedQueueImage(t, l, []byte("oldest"), 0, false)
			if err := w.queue.CommitNow(context.Background(), entry); err == nil {
				t.Fatal("transfer failure accepted")
			}
			if records, err := j.PendingFixedUploads(); err != nil || len(records) != 0 {
				t.Fatal("saved completion without parts", records, err)
			}
			if !l.Pending.HasPending("/file") {
				t.Fatal("lost local source")
			}
			_ = w.Stop(context.Background())
			l.Close()
			w2, l2 := fixedQueueRuntime(t, c, dir)
			j2 := l2.Journal
			if err := l2.Recover(); err != nil {
				t.Fatal(err)
			}
			entry2 := w2.queue.recoveredEntry("/file")
			if entry2 == nil {
				t.Fatal("missing recovered source")
			}
			if unknown {
				cancelUnavailable.Store(false)
				if err := w2.queue.CommitNow(context.Background(), entry2); err == nil {
					t.Fatal("reused canceled session")
				}
				mu.Lock()
				if len(ids) != 1 || reserved["upload-1"] {
					t.Error("changed identity before confirmed cleanup")
				}
				stable := fixedCacheModeID("abort:" + originalID)
				count := 0
				for _, id := range abortIDs["upload-1"] {
					if id == stable {
						count++
					}
				}
				if count != 2 {
					t.Error("lost deterministic cancellation identity", count)
				}
				mu.Unlock()
			}
			// Same local generation and bytes can now use a new, accounted session.
			if err := w2.queue.CommitNow(context.Background(), entry2); err != nil {
				t.Fatal(err)
			}
			records, err := j2.PendingFixedUploads()
			if err != nil || len(records) != 0 || l2.Pending.HasPending("/file") {
				t.Fatal("unfinished cleanup", records, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(ids) != 2 || len(completeIDs) != 1 {
				t.Fatal("wrong session count", len(ids), len(completeIDs))
			}
			for _, active := range reserved {
				if active {
					t.Fatal("leaked reservation")
				}
			}
		})
	}
}
