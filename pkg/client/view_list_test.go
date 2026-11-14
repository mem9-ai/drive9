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

func TestBoundDirectoryListRequiresCompleteResponse(t *testing.T) {
	for _, mode := range []string{"complete", "truncated", "abort", "bounded", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			binding := ViewBinding{LayerID: "a", WorkID: "work", Fence: 3}
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				got, err := viewprotocol.Parse(r.Header)
				if err != nil || got != binding || r.URL.Path != "/v1/views/a/v1/fs/dir" || r.URL.RawQuery != "list=1" {
					t.Errorf("binding %s %+v %v", r.URL, got, err)
					w.WriteHeader(400)
					return
				}
				if mode == "rejected" {
					w.WriteHeader(409)
					_, _ = io.WriteString(w, `{"error":"work expired"}`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"entries":[{"name":"a","size":3,"revision":2},`)
				w.(http.Flusher).Flush()
				if mode == "abort" {
					panic(http.ErrAbortHandler)
				}
				if mode == "truncated" {
					return
				}
				_, _ = io.WriteString(w, `{"name":"b","is_dir":true}]}`)
			}))
			defer ts.Close()
			c, err := New(ts.URL, "test-token").WithView(binding)
			if err != nil {
				t.Fatal(err)
			}
			opts := ListOptions{}
			if mode == "bounded" {
				opts.MaxResponseBytes = 16
			}
			out, err := c.ListWithOptionsCtx(context.Background(), "/dir", opts)
			if calls.Load() != 1 {
				t.Fatal("retried or fell back", calls.Load())
			}
			if mode == "complete" {
				if err != nil || len(out.Entries) != 2 || out.Entries[0].Name != "a" || out.Entries[0].Revision != 2 || out.Entries[1].Name != "b" {
					t.Fatal(out, err)
				}
			} else {
				if err == nil || len(out.Entries) != 0 {
					t.Fatal("partial entries exposed", out, err)
				}
				if mode == "bounded" && !errors.Is(err, ErrListResponseTooLarge) {
					t.Fatal(err)
				}
			}
		})
	}
}
