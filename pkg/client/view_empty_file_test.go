package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedEmptyFileReceiptAndReplay(t *testing.T) {
	for _, bad := range []string{"", "http", "truncated", "missing-size", "null-size", "nonempty", "resource", "revision", "operation", "fence", "layer", "status"} {
		t.Run(bad, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if r.Method != "PUT" || r.URL.Path != "/v1/views/a/v1/fs/file" || len(body) != 0 || r.Header.Get("X-Dat9-Expected-Revision") != "7" || r.Header.Get(viewprotocol.OperationHeader) != "empty-original" || r.Header.Get(viewprotocol.WorkHeader) != "work" {
					t.Error("changed request")
				}
				if bad == "http" {
					w.WriteHeader(503)
					return
				}
				if bad == "truncated" {
					_, _ = w.Write([]byte(`{"status":`))
					return
				}
				v := FixedViewState{LayerID: "a", Generation: 2, Fence: 1}
				out := map[string]any{"status": "ok", "operation_id": "empty-original", "view": v, "resource_id": "inode", "revision": 8, "size_bytes": 0}
				switch bad {
				case "missing-size":
					delete(out, "size_bytes")
				case "null-size":
					out["size_bytes"] = nil
				case "nonempty":
					out["size_bytes"] = 1
				case "resource":
					out["resource_id"] = ""
				case "revision":
					out["revision"] = 0
				case "operation":
					out["operation_id"] = "different"
				case "fence":
					v.Fence++
					out["view"] = v
				case "layer":
					v.LayerID = "b"
					out["view"] = v
				case "status":
					out["status"] = "completed"
				}
				_ = json.NewEncoder(w).Encode(out)
			}))
			defer server.Close()
			c, _ := New(server.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
			for range 2 {
				out, err := c.WriteFixedEmptyFile(context.Background(), "empty-original", "/file", 7)
				if bad == "" {
					if err != nil || out.ResourceID != "inode" || out.Revision != 8 || out.Size != 0 {
						t.Fatal(out, err)
					}
					continue
				}
				var op *ViewOperationError
				if err == nil || !errors.As(err, &op) || op.OperationID != "empty-original" || !IsCommitAttempted(err) || out != (FixedUploadCompletion{}) {
					t.Fatal(out, err)
				}
			}
			if calls.Load() != 2 {
				t.Fatal("unexpected requests", calls.Load())
			}
		})
	}
}

func TestFixedEmptyFileInvalidBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	fixed, _ := New(server.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	for _, tc := range []struct {
		c        *Client
		id, path string
		rev      int64
	}{
		{nil, "op", "/file", 0}, {New(server.URL, ""), "op", "/file", 0}, {fixed, "bad/id", "/file", 0}, {fixed, "op", "/", 0}, {fixed, "op", "/file/", 0}, {fixed, "op", "/file", -1},
	} {
		if _, err := tc.c.WriteFixedEmptyFile(context.Background(), tc.id, tc.path, tc.rev); err == nil {
			t.Fatal("accepted invalid request")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("sent invalid request")
	}
}

func TestFixedCreateFileCtxUsesOriginalRequest(t *testing.T) {
	var calls atomic.Int32
	var originalID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		id := r.Header.Get(viewprotocol.OperationHeader)
		if r.Method != "PUT" || r.URL.RawQuery != "" || r.Header.Get("X-Dat9-Expected-Revision") != "0" || r.Header.Get("X-Dat9-Mode") != "420" || r.ContentLength != 0 {
			t.Error("incorrect fixed creation request")
		}
		if n == 1 {
			originalID = id
			w.Header().Set("Content-Length", "99999")
			return
		}
		if id == "" || id != originalID {
			t.Error("changed operation identity")
		}
		_ = json.NewEncoder(w).Encode(FixedUploadCompletion{Status: "ok", OperationID: id, View: FixedViewState{LayerID: "a", Fence: 1, Generation: 1}, ResourceID: "file", Revision: 1})
	}))
	defer server.Close()
	c, _ := New(server.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
	rev, err := c.CreateFileCtx(t.Context(), "/file")
	var op *ViewOperationError
	if rev != 0 || !errors.As(err, &op) || op.OperationID == "" || !IsCommitAttempted(err) {
		t.Fatal(rev, err)
	}
	ctx, err := WithViewOperationID(t.Context(), op.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	rev, err = c.CreateFileCtx(ctx, "/file")
	if err != nil || rev != 1 || calls.Load() != 2 {
		t.Fatal(rev, err, calls.Load())
	}
}

func TestFixedCreateModeInvalidBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	c, _ := New(server.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
	for _, mode := range []uint32{0o1000, 0xffffffff} {
		if _, err := c.CreateFixedFileCtx(t.Context(), "/file", mode); err == nil {
			t.Fatal("accepted mode", mode)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("sent invalid request")
	}
}
