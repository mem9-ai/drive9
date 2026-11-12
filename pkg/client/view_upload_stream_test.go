package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedExtentStreamSharesTransferAndPreservesPublication(t *testing.T) {
	for _, failure := range []string{"", "init-missing", "init-truncated", "part", "source", "precheck", "complete-unknown", "complete-truncated", "complete-size"} {
		t.Run(failure, func(t *testing.T) {
			data := bytes.Repeat([]byte("a"), (4<<20)+5)
			var initiated, completed, aborted, puts atomic.Int32
			object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				puts.Add(1)
				if r.Header.Get("Authorization") != "" || r.Header.Get(viewprotocol.ViewHeader) != "" {
					t.Error("API identity leaked to object")
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				sum := sha256.Sum256(body)
				if r.URL.Path != "/"+hex.EncodeToString(sum[:]) {
					t.Error("incorrect object body")
				}
				w.WriteHeader(200)
			}))
			defer object.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(viewprotocol.ViewHeader) != "a" || r.Header.Get(viewprotocol.WorkHeader) != "work" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("missing binding", r.Header)
				}
				switch r.URL.Path {
				case "/v1/views/a/v2/uploads/initiate":
					initiated.Add(1)
					if r.Header.Get(viewprotocol.OperationHeader) != fixedUploadPhaseID("initiate", "publish") {
						t.Error("initiate identity")
					}
					if failure == "init-missing" {
						w.WriteHeader(404)
						return
					}
					if failure == "init-truncated" {
						w.Header().Set("Content-Length", "9999")
					}
					w.WriteHeader(202)
					_ = json.NewEncoder(w).Encode(extentUploadTestPlan(int64(len(data))))
				case "/v1/views/a/v2/uploads/upload/presign":
					if r.Header.Get(viewprotocol.OperationHeader) != "" {
						t.Error("presign reused mutation identity")
					}
					var req struct {
						Number   int `json:"part_number"`
						Checksum struct{ Value string }
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Number < 1 || req.Number > 2 {
						t.Error(req, err)
						w.WriteHeader(400)
						return
					}
					if failure == "part" {
						w.WriteHeader(503)
						return
					}
					offset := (req.Number - 1) * (4 << 20)
					size := min(4<<20, len(data)-offset)
					sum := sha256.Sum256(data[offset : offset+size])
					if req.Checksum.Value != hex.EncodeToString(sum[:]) {
						t.Error("wrong part hash")
					}
					_ = json.NewEncoder(w).Encode(PartURL{Number: req.Number, Size: int64(size), ChecksumSHA256: req.Checksum.Value, URL: object.URL + "/" + req.Checksum.Value, Headers: map[string]string{"If-None-Match": "*"}, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)})
				case "/v1/views/a/v2/uploads/upload/complete":
					completed.Add(1)
					if r.Header.Get(viewprotocol.OperationHeader) != "publish" {
						t.Error("completion identity")
					}
					var payload map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload) != 1 || payload["parts"] == nil {
						t.Error(payload, err)
					}
					var parts []completePart
					if err := json.Unmarshal(payload["parts"], &parts); err != nil || len(parts) != 2 || parts[0].Number != 1 || parts[1].Number != 2 {
						t.Error(parts, err)
					}
					if failure == "complete-unknown" {
						w.WriteHeader(503)
						return
					}
					if failure == "complete-truncated" {
						w.Header().Set("Content-Length", "9999")
					}
					size := int64(len(data))
					if failure == "complete-size" {
						size--
					}
					_ = json.NewEncoder(w).Encode(FixedUploadCompletion{Status: "completed", OperationID: "publish", View: FixedViewState{LayerID: "a", Generation: 4, Fence: 1, Phase: "OPEN"}, ResourceID: "inode", Revision: 1, Size: size})
				case "/v1/views/a/v2/uploads/upload/abort":
					aborted.Add(1)
					id := fixedUploadPhaseID("abort", "upload")
					if r.Header.Get(viewprotocol.OperationHeader) != id {
						t.Error("abort reused publication identity")
					}
					_ = json.NewEncoder(w).Encode(FixedUploadOutcome{OperationID: id, UploadID: "upload", Status: "ABORTED"})
				default:
					t.Error("unexpected API/fallback", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer api.Close()
			c, err := New(api.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := WithViewOperationID(context.Background(), "publish")
			if err != nil {
				t.Fatal(err)
			}
			source := data
			if failure == "source" {
				source = nil
			}
			check := func() error {
				if failure == "precheck" {
					return errors.New("source changed")
				}
				return nil
			}
			_, err = c.writeStreamConditionalWithSummaryAndPreCompleteCheck(ctx, "/file", bytes.NewReader(source), int64(len(data)), nil, 0, nil, "", "", check)
			pre := failure == "source" || failure == "part" || failure == "precheck"
			initFailed := strings.HasPrefix(failure, "init-")
			if failure == "" {
				if err != nil || puts.Load() != 2 || completed.Load() != 1 || aborted.Load() != 0 {
					t.Fatal(err, puts.Load(), completed.Load(), aborted.Load())
				}
			} else if err == nil {
				t.Fatal("failure returned success")
			}
			if pre && (completed.Load() != 0 || aborted.Load() != 1 || IsCommitAttempted(err)) {
				t.Fatal(err, completed.Load(), aborted.Load())
			}
			if initFailed || strings.HasPrefix(failure, "complete-") {
				var op *ViewOperationError
				want := "publish"
				if initFailed {
					want = fixedUploadPhaseID("initiate", want)
				}
				if !errors.As(err, &op) || op.OperationID != want || op.LayerID != "a" || aborted.Load() != 0 || IsCommitAttempted(err) == initFailed {
					t.Fatal(err, aborted.Load())
				}
			}
			if initiated.Load() != 1 {
				t.Fatal("re-initiated", initiated.Load())
			}
		})
	}
}

func TestFixedExtentStreamUnsupportedOptionsRejectBeforeInitiation(t *testing.T) {
	c, _ := New("http://unused.invalid", "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	for _, tc := range []struct {
		tags                  map[string]string
		description, checksum string
	}{{tags: map[string]string{"key": "value"}}, {description: "metadata"}, {checksum: strings.Repeat("a", 64)}} {
		if err := c.writeStreamV2WithSummary(context.Background(), "/file", bytes.NewReader([]byte("a")), 1, nil, 0, nil, tc.tags, tc.description, tc.checksum); err == nil {
			t.Fatal(tc)
		}
	}
}
