package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedExtentPartsSavedBeforeExplicitCompletion(t *testing.T) {
	data := bytes.Repeat([]byte("a"), (4<<20)+5)
	plan := extentUploadTestPlan(int64(len(data)))
	var transfers, completions atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(viewprotocol.ViewHeader) != "a" || r.Header.Get(viewprotocol.WorkHeader) != "work" {
			t.Error("lost fixed binding")
		}
		switch r.URL.Path {
		case "/v1/views/a/v2/uploads/upload/presign":
			transfers.Add(1)
			var req struct {
				Number   int   `json:"part_number"`
				Size     int64 `json:"size"`
				Checksum struct{ Value string }
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Number < 1 || req.Number > 2 {
				t.Error(req, err)
				w.WriteHeader(400)
				return
			}
			offset := int64(req.Number-1) * plan.PartSize
			size := min(plan.PartSize, int64(len(data))-offset)
			sum := sha256.Sum256(data[offset : offset+size])
			if req.Checksum.Value != hex.EncodeToString(sum[:]) {
				t.Error("wrong transfer checksum")
			}
			_ = json.NewEncoder(w).Encode(PartURL{Number: req.Number, Size: size, ChecksumSHA256: req.Checksum.Value, SkipUpload: true, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)})
		case "/v1/views/a/v2/uploads/upload/complete":
			if r.Header.Get(viewprotocol.OperationHeader) != "publish" {
				t.Error("completion identity changed")
			}
			var req struct{ Parts []FixedExtentPart }
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Parts) != 2 {
				t.Error(req, err)
				w.WriteHeader(400)
				return
			}
			for i, part := range req.Parts {
				offset := int64(i) * plan.PartSize
				sum := sha256.Sum256(data[offset:min(offset+plan.PartSize, int64(len(data)))])
				if part.Number != i+1 || part.ETag != hex.EncodeToString(sum[:]) {
					t.Error("saved completion changed", part)
				}
			}
			if completions.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			_ = json.NewEncoder(w).Encode(FixedUploadCompletion{Status: "completed", OperationID: "publish", View: FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 3, Size: int64(len(data))})
		default:
			t.Error("unexpected initiation, abort or fallback", r.URL.Path)
			w.WriteHeader(400)
		}
	}))
	defer api.Close()
	binding := ViewBinding{LayerID: "a", WorkID: "work", Fence: 1}
	c, _ := New(api.URL, "").WithView(binding)
	parts, err := c.UploadFixedExtentParts(context.Background(), plan, bytes.NewReader(data), int64(len(data)), nil)
	if err != nil || len(parts) != 2 || transfers.Load() != 2 || completions.Load() != 0 {
		t.Fatal(parts, err, transfers.Load(), completions.Load())
	}
	// A caller can persist completion BEFORE any publication. Rebuilding a
	// client and loading that record needs neither the old source nor its plan.
	type record struct {
		Binding               ViewBinding
		OperationID, UploadID string
		Size                  int64
		Parts                 []FixedExtentPart
	}
	saved := record{binding, "publish", plan.UploadID, int64(len(data)), parts}
	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "completion.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var loaded record
		if err := json.Unmarshal(raw, &loaded); err != nil {
			t.Fatal(err)
		}
		fresh, err := New(api.URL, "").WithView(loaded.Binding)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := fresh.CompleteFixedExtentUpload(context.Background(), loaded.OperationID, loaded.UploadID, loaded.Parts)
		if attempt == 0 {
			if err == nil || !IsCommitAttempted(err) || receipt.Revision != 0 {
				t.Fatal(receipt, err)
			}
		} else if err != nil || receipt.Size != loaded.Size || receipt.Revision != 3 {
			t.Fatal(receipt, err)
		}
	}
	if transfers.Load() != 2 || completions.Load() != 2 {
		t.Fatal(transfers.Load(), completions.Load())
	}
}

func TestFixedExtentPartsFailureNeverPublishes(t *testing.T) {
	for _, failure := range []string{"source", "presign", "canceled", "plan", "expired", "nil-source", "main", "validation"} {
		t.Run(failure, func(t *testing.T) {
			var requests, aborts atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch r.URL.Path {
				case "/v1/views/a/v2/uploads/upload/abort":
					aborts.Add(1)
					_ = json.NewEncoder(w).Encode(FixedUploadOutcome{OperationID: fixedUploadPhaseID("abort", "upload"), UploadID: "upload", Status: "ABORTED"})
				case "/v1/views/a/v2/uploads/upload/presign":
					w.WriteHeader(503)
				default:
					t.Error("unexpected publication", r.URL.Path)
					w.WriteHeader(400)
				}
			}))
			defer api.Close()
			binding := ViewBinding{LayerID: "a", WorkID: "work", Fence: 1}
			if failure == "main" {
				binding = ViewBinding{}
			}
			if failure == "validation" {
				binding.Validation = true
			}
			c, _ := New(api.URL, "").WithView(binding)
			plan := extentUploadTestPlan(6)
			var source io.ReaderAt = bytes.NewReader([]byte("abcdef"))
			if failure == "source" {
				source = bytes.NewReader(nil)
			}
			if failure == "nil-source" {
				source = nil
			}
			if failure == "plan" {
				plan.TotalParts = 2
			}
			if failure == "expired" {
				plan.ExpiresAt = time.Now().Add(-time.Hour).Format(time.RFC3339)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "canceled" {
				cancel()
			}
			parts, err := c.UploadFixedExtentParts(ctx, plan, source, 6, nil)
			if err == nil || parts != nil || IsCommitAttempted(err) {
				t.Fatal(parts, err)
			}
			if failure == "source" || failure == "presign" || failure == "canceled" {
				if aborts.Load() != 1 {
					t.Fatal("failed transfer not canceled", aborts.Load())
				}
			} else if requests.Load() != 0 {
				t.Fatal("invalid input made requests", requests.Load())
			}
		})
	}
}
