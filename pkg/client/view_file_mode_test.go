package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedFileModeRequestAndReceipt(t *testing.T) {
	for _, response := range []string{"changed", "noop", "missing-size", "wrong-resource", "wrong-revision", "wrong-id", "truncated", "unknown"} {
		t.Run(response, func(t *testing.T) {
			calls := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/v1/views/a/v1/fs/file" || r.URL.RawQuery != "chmod" || r.Header.Get(viewprotocol.OperationHeader) != "chmod-original" || r.Header.Get(viewprotocol.WorkHeader) != "work" {
					t.Error("wrong binding", r.URL, r.Header)
				}
				var req struct {
					Mode       uint32
					ResourceID string `json:"resource_id"`
					Revision   int64  `json:"expected_revision"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Mode != 0 || req.ResourceID != "inode" || req.Revision != 8 {
					t.Error(req, err)
				}
				out := map[string]any{"status": "ok", "operation_id": "chmod-original", "view": FixedViewState{LayerID: "a", Generation: 4, Fence: 2}, "resource_id": "inode", "revision": 9, "size_bytes": 0}
				switch response {
				case "noop":
					out["revision"] = 8
				case "missing-size":
					delete(out, "size_bytes")
				case "wrong-resource":
					out["resource_id"] = "other"
				case "wrong-revision":
					out["revision"] = 10
				case "wrong-id":
					out["operation_id"] = "other"
				case "truncated":
					_, _ = fmt.Fprint(w, `{"status":`)
					return
				case "unknown":
					w.WriteHeader(503)
					return
				}
				_ = json.NewEncoder(w).Encode(out)
			}))
			defer api.Close()
			c, _ := New(api.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 2})
			rev, err := c.ChmodFixedFile(context.Background(), "chmod-original", "/file", "inode", 8, 0)
			if response == "changed" || response == "noop" {
				want := int64(9)
				if response == "noop" {
					want = 8
				}
				if err != nil || rev != want {
					t.Fatal(rev, err)
				}
			} else {
				var op *ViewOperationError
				if rev != 0 || !errors.As(err, &op) || op.OperationID != "chmod-original" || !IsCommitAttempted(err) {
					t.Fatal(rev, err)
				}
			}
			if calls != 1 {
				t.Fatal("unexpected retry", calls)
			}
		})
	}
}

func TestFixedFileModeRejectsBeforeRequest(t *testing.T) {
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500) }))
	defer api.Close()
	main := New(api.URL, "")
	fixed, _ := main.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	for _, tc := range []struct {
		c                  *Client
		id, path, resource string
		rev                int64
	}{
		{main, "op", "/file", "inode", 1}, {fixed, "bad/id", "/file", "inode", 1}, {fixed, "op", "/file", "", 1}, {fixed, "op", "/file", "inode", 0}, {fixed, "op", "/../file", "inode", 1},
	} {
		if rev, err := tc.c.ChmodFixedFile(context.Background(), tc.id, tc.path, tc.resource, tc.rev, 0600); err == nil || rev != 0 {
			t.Fatal(rev, err)
		}
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}
