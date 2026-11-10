package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
