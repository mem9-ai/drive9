package writeback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedDirectCompletionRecovery(t *testing.T) {
	for _, policy := range []Policy{PolicyCloseSync, PolicyWriteSync} {
		for _, shape := range []string{"small", "truncate", "create"} {
			for _, failure := range []string{"none", "lost", "truncated"} {
				for _, later := range []string{"none", "local", "foreign"} {
					if failure == "none" && later != "none" {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/%s/%s", policy, shape, failure, later), func(t *testing.T) {
						ctx := context.Background()
						backend := testbackend.NewBackend()
						if shape != "create" {
							backend.Seed("/f", []byte("before"))
						}
						var mu sync.Mutex
						var operation, method, condition string
						var body []byte
						var original *httptest.ResponseRecorder
						attempts, publications := 0, 0
						api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							r.URL.Path = strings.TrimPrefix(r.URL.Path, "/v1/views/a")
							if r.Method != http.MethodPut && r.Method != http.MethodPost {
								backend.ServeHTTP(w, r)
								return
							}
							mu.Lock()
							defer mu.Unlock()
							id := r.Header.Get(viewprotocol.OperationHeader)
							data, err := io.ReadAll(r.Body)
							if err != nil {
								t.Error(err)
								w.WriteHeader(500)
								return
							}
							r.Body = io.NopCloser(bytes.NewReader(data))
							var result *httptest.ResponseRecorder
							if operation == "" || operation != id {
								result = httptest.NewRecorder()
								backend.ServeHTTP(result, r)
								if result.Code < 300 {
									publications++
								}
								if operation == "" {
									operation, method, condition = id, r.Method+" "+r.URL.RequestURI(), r.Header.Get("X-Dat9-Expected-Revision")
									body, original = bytes.Clone(data), result
									if id == "" {
										t.Error("publication has no operation ID")
									}
								}
							} else {
								result = original
								if method != r.Method+" "+r.URL.RequestURI() || condition != r.Header.Get("X-Dat9-Expected-Revision") || !bytes.Equal(body, data) {
									t.Error("retry changed original request")
								}
							}
							if id == operation {
								attempts++
							}
							if id == operation && attempts <= 2 && failure != "none" {
								if failure == "lost" {
									w.Header().Set("Content-Length", "99999")
									w.WriteHeader(result.Code)
									return
								}
								w.Header().Set("Content-Length", "99999")
							}
							w.WriteHeader(result.Code)
							_, _ = w.Write(result.Body.Bytes())
						}))
						defer api.Close()
						bound, err := client.New(api.URL, "").WithView(client.ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
						if err != nil {
							t.Fatal(err)
						}
						wb, err := New(Options{Client: bound, Policy: policy})
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
						var h *bufferingHandle
						if shape == "create" {
							h = writebackHandleOf(open(t, chain, "/f"))
						} else {
							h = openExisting(t, chain, "/f")
						}
						initialRev := h.baseRev
						if shape == "small" {
							err = h.Write(ctx, 0, []byte("edited"))
							if policy == PolicyCloseSync {
								if err != nil {
									t.Fatal(err)
								}
								err = h.Flush(ctx)
							}
						} else {
							if shape == "truncate" {
								if err = h.truncateLocal(ctx, 0); err != nil {
									t.Fatal(err)
								}
							}
							err = h.Flush(ctx)
						}
						if failure == "none" {
							if err != nil {
								t.Fatal(err)
							}
						} else {
							mu.Lock()
							originalID := operation
							mu.Unlock()
							var operationErr *client.ViewOperationError
							if !errors.As(err, &operationErr) || operationErr.OperationID != originalID || h.fixedCompletion == nil || !h.dirty || h.baseRev != initialRev {
								t.Fatalf("lost unresolved publication: %v", err)
							}
							if later != "none" {
								err = h.Write(ctx, 0, []byte("newer!"))
								if policy == PolicyCloseSync {
									if err != nil {
										t.Fatal(err)
									}
									err = h.Flush(ctx)
								}
							} else {
								err = h.Flush(ctx)
							}
							if !errors.As(err, &operationErr) || operationErr.OperationID != originalID || h.fixedCompletion == nil || !h.dirty {
								t.Fatalf("lost retry identity: %v", err)
							}
							if later == "foreign" {
								// A separate ordinary writer changes the current row after the lost
								// successful response. Replaying our receipt must never overwrite it.
								req := httptest.NewRequest(http.MethodPut, "/v1/fs/f", strings.NewReader("foreign"))
								rec := httptest.NewRecorder()
								backend.ServeHTTP(rec, req)
								if rec.Code >= 300 {
									t.Fatal("foreign write", rec.Code, rec.Body.String())
								}
							}
							err = h.Flush(ctx)
							if later == "foreign" {
								if err == nil || !h.dirty {
									t.Fatal("accepted stale local write over foreign version")
								}
								node, _ := backend.Node("/f")
								if string(node.Data) != "foreign" {
									t.Fatalf("overwrote foreign content: %q", node.Data)
								}
								return
							}
							if err != nil {
								t.Fatal("replay did not recover", err)
							}
						}
						if err := h.Release(ctx); err != nil {
							t.Fatal(err)
						}
						if h.fixedCompletion != nil || h.dirty {
							t.Fatal("publication remained dirty")
						}
						want := ""
						if shape == "small" {
							want = "edited"
						}
						if later == "local" {
							want = "newer!"
						}
						node, ok := backend.Node("/f")
						if !ok || string(node.Data) != want || h.baseRev != node.Rev {
							t.Fatalf("node=%+v base=%d want=%q", node, h.baseRev, want)
						}
						mu.Lock()
						defer mu.Unlock()
						wantPublications := 1
						if later == "local" {
							wantPublications++
						}
						if publications != wantPublications {
							t.Fatalf("publications=%d want=%d", publications, wantPublications)
						}
						wantAttempts := 1
						if failure != "none" {
							wantAttempts = 3
						}
						if attempts != wantAttempts {
							t.Fatalf("original attempts=%d want=%d", attempts, wantAttempts)
						}
					})
				}
			}
		}
	}
}
