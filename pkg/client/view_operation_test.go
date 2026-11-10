package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestViewMutationRetainsIdentityAfterUnknownResponse(t *testing.T) {
	for _, failure := range []string{"status", "transport"} {
		t.Run(failure, func(t *testing.T) {
			var count atomic.Int32
			ids := make(chan string, 3)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id, err := viewprotocol.ParseOperationID(r.Header)
				if err != nil {
					t.Error(err)
				}
				ids <- id
				if r.Method != "DELETE" || r.URL.Path != "/v1/views/a/v1/fs/dir" || r.URL.Query().Get("recursive") != "1" {
					t.Error(r.Method, r.URL)
				}
				if count.Add(1) == 1 {
					if failure == "transport" {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
						return
					}
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(503)
					_, _ = io.WriteString(w, `{"code":"OUTCOME_UNKNOWN","error":"query same operation"}`)
					return
				}
				_, _ = io.WriteString(w, `{"status":"ok"}`)
			}))
			defer ts.Close()
			c, err := New(ts.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			err = c.RemoveAllCtx(context.Background(), "/dir")
			var outcome *ViewOperationError
			if !errors.As(err, &outcome) || outcome.LayerID != "a" || outcome.OperationID == "" || count.Load() != 1 {
				t.Fatal(err, count.Load())
			}
			original := <-ids
			if original != outcome.OperationID {
				t.Fatal(original, outcome)
			}
			if failure == "status" {
				var status *StatusError
				if !errors.As(err, &status) || status.StatusCode != 503 || status.Code != "OUTCOME_UNKNOWN" {
					t.Fatal(err)
				}
			}
			ctx, err := WithViewOperationID(context.Background(), outcome.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.RemoveAllCtx(ctx, "/dir"); err != nil {
				t.Fatal(err)
			}
			if id := <-ids; id != original {
				t.Fatal("replay changed ID", id, original)
			}
			if err := c.RemoveAllCtx(context.Background(), "/dir"); err != nil {
				t.Fatal(err)
			}
			if id := <-ids; id == original {
				t.Fatal("independent operation reused ID")
			}
		})
	}
}

func TestViewMutationIdentityScopeAndValidation(t *testing.T) {
	if _, err := WithViewOperationID(context.Background(), "invalid/id"); !errors.Is(err, viewprotocol.ErrOperationID) {
		t.Fatal(err)
	}
	ctx, err := WithViewOperationID(context.Background(), "fixed-id")
	if err != nil {
		t.Fatal(err)
	}
	root := New("http://127.0.0.1:1", "test-token")
	view, err := root.WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Client{root, view} {
		for _, method := range []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"} {
			req, err := c.newFSRequest(ctx, method, "/file", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			want := ""
			if c == view && method != "GET" && method != "HEAD" {
				want = "fixed-id"
			}
			if req.Header.Get(viewprotocol.OperationHeader) != want {
				t.Fatal(method, c.view, req.Header)
			}
		}
	}
}

type viewResponseTransport func(*http.Request) (*http.Response, error)

func (f viewResponseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type cancelViewResponseRead struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelViewResponseRead) Read(p []byte) (int, error) { b.cancel(); return b.ReadCloser.Read(p) }

func TestViewMutationBodyFailureRetainsOriginalOperation(t *testing.T) {
	for _, method := range []string{"create", "write", "stream"} {
		for _, failure := range []string{"truncated", "abort", "cancel", "empty", "missing-revision", "complete"} {
			t.Run(method+"/"+failure, func(t *testing.T) {
				var calls atomic.Int32
				ids := make(chan string, 1)
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					ids <- r.Header.Get(viewprotocol.OperationHeader)
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "application/json")
					switch failure {
					case "complete":
						_, _ = io.WriteString(w, `{"revision":7}`)
					case "empty":
						w.WriteHeader(200)
					case "missing-revision":
						_, _ = io.WriteString(w, `{}`)
					default:
						_, _ = io.WriteString(w, `{"revision":`)
						w.(http.Flusher).Flush()
						if failure == "abort" {
							panic(http.ErrAbortHandler)
						}
						if failure == "cancel" {
							<-r.Context().Done()
						}
					}
				}))
				defer ts.Close()
				c, err := New(ts.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if failure == "cancel" {
					underlying := http.DefaultTransport.(*http.Transport).Clone()
					defer underlying.CloseIdleConnections()
					c.httpClient.Transport = viewResponseTransport(func(r *http.Request) (*http.Response, error) {
						resp, err := underlying.RoundTrip(r)
						if err == nil {
							resp.Body = cancelViewResponseRead{resp.Body, cancel}
						}
						return resp, err
					})
				}
				var revision int64
				switch method {
				case "create":
					revision, err = c.CreateFileCtx(ctx, "/file")
				case "stream":
					revision, err = c.WriteServerStreamConditional(ctx, "/file", strings.NewReader("data"), 4, 1)
				default:
					revision, err = c.WriteCtxConditionalWithRevision(ctx, "/file", []byte("data"), 1)
				}
				if calls.Load() != 1 {
					t.Fatal("retried", calls.Load())
				}
				id := <-ids
				if failure == "complete" {
					if err != nil || revision != 7 {
						t.Fatal(revision, err)
					}
					return
				}
				var op *ViewOperationError
				if revision != 0 || !errors.As(err, &op) || op.LayerID != "a" || id == "" || op.OperationID != id {
					t.Fatal("lost outcome identity", revision, err, id)
				}
			})
		}
	}
}

func TestMainMutationRetainsLegacyResponseCompatibility(t *testing.T) {
	for _, body := range []string{"", `{"revision":`} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(viewprotocol.OperationHeader) != "" {
				t.Error("main received operation header")
			}
			_, _ = io.WriteString(w, body)
		}))
		c := New(ts.URL, "test-token")
		if revision, err := c.WriteCtxConditionalWithRevision(context.Background(), "/file", nil, 0); err != nil || revision != 0 {
			t.Fatal(revision, err)
		}
		if body == "" {
			if revision, err := c.CreateFileCtx(context.Background(), "/file"); err != nil || revision != 0 {
				t.Fatal(revision, err)
			}
		}
		ts.Close()
	}
}

func TestFixedAppendResponsesKeepOperationIdentity(t *testing.T) {
	for _, action := range []string{"append-log", "append"} {
		for _, body := range []string{`{"revision":`, `{"revision":0,"size_bytes":5}`, `{"revision":7,"size_bytes":6}`, `{"revision":7,"size_bytes":5} {}`, `{"revision":7,"size_bytes":5}`} {
			if action == "append" && body != `{"revision":` {
				continue
			}
			t.Run(action+"/"+body, func(t *testing.T) {
				id := make(chan string, 1)
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					id <- r.Header.Get(viewprotocol.OperationHeader)
					_, _ = io.Copy(io.Discard, r.Body)
					if action == "append" {
						w.WriteHeader(202)
					}
					_, _ = io.WriteString(w, body)
				}))
				defer ts.Close()
				c, err := New(ts.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				if action == "append-log" {
					_, err = c.AppendLog(context.Background(), "/file", strings.NewReader("abc"), 3, 1, 2)
				} else {
					_, err = c.initiateAppend(context.Background(), "/file", 3, 5, 1)
				}
				original := <-id
				if body == `{"revision":7,"size_bytes":5}` {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				var op *ViewOperationError
				if !errors.As(err, &op) || op.OperationID == "" || op.OperationID != original || op.LayerID != "a" {
					t.Fatal(err, original)
				}
			})
		}
	}
}
