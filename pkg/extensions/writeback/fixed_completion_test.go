package writeback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedWritebackRecoversOriginalCompletion(t *testing.T) {
	for _, failure := range []string{"settlement", "truncated"} {
		for _, newerWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/newer=%v", failure, newerWrite), func(t *testing.T) {
				ctx := context.Background()
				data := bytes.Repeat([]byte("a"), (4<<20)+5)
				var mu sync.Mutex
				var initiated, requests, publications, puts int
				var reserved int64
				var completionID string
				var completionBody []byte
				var stored []byte
				blocks := map[string][]byte{}
				var api *httptest.Server
				api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					switch {
					case strings.HasPrefix(r.URL.Path, "/object/"):
						puts++
						body, _ := io.ReadAll(r.Body)
						sum := sha256.Sum256(body)
						key := hex.EncodeToString(sum[:])
						if r.URL.Path != "/object/"+key {
							t.Error("incorrect block body")
						}
						blocks[key] = body
					case r.URL.Path == "/v1/views/a/v1/status":
						_ = json.NewEncoder(w).Encode(map[string]any{"small_file_threshold": 1024})
					case r.Method == http.MethodHead:
						w.WriteHeader(404)
					case r.URL.Path == "/v1/views/a/v2/uploads/initiate":
						initiated++
						if initiated > 1 && reserved != 0 {
							t.Error("reinitiated before resolving old completion")
						}
						var req struct {
							Size     int64 `json:"total_size"`
							Revision int64 `json:"expected_revision"`
						}
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
						}
						if initiated > 1 && req.Revision != 1 {
							t.Error("lost completed revision", req.Revision)
						}
						reserved = req.Size
						w.WriteHeader(202)
						_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": fmt.Sprintf("upload-%d", initiated), "content_layout": "extent", "part_size": 4 << 20, "total_parts": 2, "expires_at": time.Now().Add(time.Hour).Format(time.RFC3339), "checksum_contract": map[string]any{"required": true, "supported": []string{"SHA-256"}}})
					case strings.HasSuffix(r.URL.Path, "/presign"):
						var req struct {
							Number   int   `json:"part_number"`
							Size     int64 `json:"size_bytes"`
							Checksum struct{ Value string }
						}
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
						}
						size := int64(4 << 20)
						if req.Number == 2 {
							size = 5
						}
						_ = json.NewEncoder(w).Encode(client.PartURL{Number: req.Number, Size: size, ChecksumSHA256: req.Checksum.Value, URL: api.URL + "/object/" + req.Checksum.Value, Headers: map[string]string{"If-None-Match": "*"}, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)})
					case strings.HasSuffix(r.URL.Path, "/complete"):
						requests++
						id := r.Header.Get(viewprotocol.OperationHeader)
						body, _ := io.ReadAll(r.Body)
						if requests == 1 || initiated == 2 {
							if requests > 1 && id == completionID {
								t.Error("new image reused completion ID")
							}
							publications++
							var req struct{ Parts []client.FixedExtentPart }
							if err := json.Unmarshal(body, &req); err != nil {
								t.Error(err)
							}
							stored = nil
							for _, part := range req.Parts {
								stored = append(stored, blocks[part.ETag]...)
							}
							if !bytes.Equal(stored, data) {
								t.Error("publication bytes differ")
							}
							if requests == 1 {
								completionID = id
								completionBody = append([]byte(nil), body...)
							}
						} else if id != completionID || !bytes.Equal(body, completionBody) || !strings.Contains(r.URL.Path, "/upload-1/") {
							t.Error("recovery changed completion request")
						}
						if requests <= 2 && failure == "settlement" {
							w.WriteHeader(503)
							return
						}
						if requests <= 2 {
							w.Header().Set("Content-Length", "99999")
						} else {
							reserved = 0
						}
						_ = json.NewEncoder(w).Encode(client.FixedUploadCompletion{Status: "completed", OperationID: id, View: client.FixedViewState{LayerID: "a", Generation: int64(publications + 1), Fence: 1, Phase: "OPEN"}, ResourceID: "inode", Revision: int64(publications), Size: int64(len(stored))})
					default:
						t.Error("unexpected request", r.Method, r.URL.Path)
						w.WriteHeader(500)
					}
				}))
				defer api.Close()
				bound, err := client.New(api.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				wb, err := New(Options{Client: bound, Policy: PolicyCloseSync, PreloadLimit: 1})
				if err != nil {
					t.Fatal(err)
				}
				terminal, err := core.New(bound, core.Options{})
				if err != nil {
					t.Fatal(err)
				}
				chain, err := fs.NewChain(wb, terminal)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = chain.Stop(ctx) }()
				h := open(t, chain, "/file")
				if err := h.Write(ctx, 0, data); err != nil {
					t.Fatal(err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					err := h.Flush(ctx)
					var pending *client.FixedExtentCompletionError
					var operation *client.ViewOperationError
					if !errors.As(err, &pending) || !errors.As(err, &operation) || !client.IsCommitAttempted(err) {
						t.Fatalf("lost recovery error: %v", err)
					}
					bh := writebackHandleOf(h)
					if !bh.dirty || bh.fixedCompletion == nil {
						t.Fatal("discarded unresolved image")
					}
					if attempt == 0 && newerWrite {
						mu.Lock()
						data[0] = 'b'
						mu.Unlock()
						if err := h.Write(ctx, 0, []byte("b")); err != nil {
							t.Fatal(err)
						}
					}
					got := make([]byte, len(data))
					n, err := h.Read(ctx, 0, got)
					if err != nil || n != len(data) || !bytes.Equal(got, data) {
						t.Fatal("lost buffered bytes", n, err)
					}
					mu.Lock()
					if initiated != 1 || publications != 1 || reserved != int64(len(data)) || puts != 2 {
						t.Error("unresolved state changed", initiated, publications, reserved, puts)
					}
					mu.Unlock()
				}
				if err := h.Release(ctx); err != nil {
					t.Fatal("recovery", err)
				}
				bh := writebackHandleOf(h)
				if bh.dirty || bh.fixedCompletion != nil {
					t.Fatal("completion not retired")
				}
				mu.Lock()
				want := 1
				if newerWrite {
					want = 2
				}
				if initiated != want || publications != want || reserved != 0 || !bytes.Equal(stored, data) || requests != want+2 {
					t.Error("wrong recovered result", initiated, publications, reserved, requests)
				}
				mu.Unlock()
			})
		}
	}
}
