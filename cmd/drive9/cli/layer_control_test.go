package cli

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestFixedLayerCLIControlRoutes(t *testing.T) {
	const work = `{"work":{"layer_id":"a","work_id":"w","fence":3,"lease_until":"2026-11-10T12:00:00Z"},"launch":false}`
	cases := []struct {
		args                   []string
		method, path, response string
		body                   map[string]any
	}{
		{[]string{"create", "--operation", "create", "--parent", "parent", "--name", "demo", "a"}, "POST", "/v1/views", `{"layer_id":"a","phase":"OPEN"}`, map[string]any{"operation_id": "create", "layer_id": "a", "parent_layer_id": "parent", "name": "demo"}},
		{[]string{"list", "--after", "a", "--limit", "2"}, "GET", "/v1/views?after=a&limit=2", `{"layers":[{"layer_id":"b"}],"next_after":"b"}`, nil},
		{[]string{"status", "a"}, "GET", "/v1/views/a", `{"layer_id":"a","phase":"OPEN"}`, nil},
		{[]string{"work", "--operation", "work", "--ttl", "30m", "a"}, "POST", "/v1/views/a/work", work, map[string]any{"operation_id": "work", "ttl_ms": float64(1800000), "validation": false}},
		{[]string{"renew", "--work", "w", "--fence", "3", "--ttl", "1m", "a"}, "POST", "/v1/views/a/renew", `{"layer_id":"a","work_id":"w","fence":3,"lease_until":"2026-11-10T12:01:00Z"}`, nil},
		{[]string{"seal", "--operation", "seal", "--work", "w", "--fence", "3", "a"}, "POST", "/v1/views/a/seal", `{"state":"DONE","layer":{"layer_id":"a","phase":"READY"}}`, nil},
		{[]string{"cancel", "--operation", "cancel", "a"}, "POST", "/v1/views/a/cancel", `{"state":"DONE","layer":{"layer_id":"a","phase":"ABORTED"}}`, map[string]any{"operation_id": "cancel"}},
		{[]string{"delete", "--operation", "delete", "a"}, "DELETE", "/v1/views/a", `{"state":"DONE","layer":{"layer_id":"a","phase":"DELETED"}}`, map[string]any{"operation_id": "delete"}},
		{[]string{"operation", "a", "original"}, "GET", "/v1/views/a/operations/original", `{"operation_id":"original","layer_id":"a","kind":"seal","state":"DONE"}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.args[0], func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != tc.method || r.URL.RequestURI() != tc.path {
					t.Errorf("wrong route %s %s", r.Method, r.URL)
				}
				if tc.body != nil {
					var got map[string]any
					if err := json.NewDecoder(r.Body).Decode(&got); err != nil || !reflect.DeepEqual(got, tc.body) {
						t.Errorf("body %v error %v", got, err)
					}
				}
				_, _ = w.Write([]byte(tc.response))
			}))
			defer srv.Close()
			out, err := captureStdoutE(t, func() error { return Layer(client.New(srv.URL, "test-token"), tc.args) })
			if err != nil || calls.Load() != 1 {
				t.Fatal(out, err, calls.Load())
			}
			var value map[string]any
			if err := json.Unmarshal([]byte(out), &value); err != nil {
				t.Fatal("output is not JSON", out, err)
			}
			if tc.args[0] == "work" && value["launch"] != false {
				t.Fatal("replay granted launch", out)
			}
			if tc.args[0] == "list" && value["next_after"] != "b" {
				t.Fatal("pagination cursor lost", out)
			}
			if tc.args[0] == "renew" && (value["work_id"] != "w" || value["fence"] != float64(3) || value["lease_until"] != "2026-11-10T12:01:00Z") {
				t.Fatal("renewed work identity or deadline lost", out)
			}
			if tc.args[0] == "operation" && (value["operation_id"] != "original" || value["kind"] != "seal" || value["state"] != "DONE") {
				t.Fatal("operation receipt lost", out)
			}
		})
	}
}

func TestFixedLayerCLIInvalidControlsNeverReachServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid command made a request", r.URL)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := client.New(srv.URL, "test-token")
	for _, args := range [][]string{
		{"create", "a"}, {"create", "--operation", "op", "--tag", "x=y", "a"},
		{"create", "--operation", "op", "--parent", "main", "a"},
		{"work", "--operation", "op", "a"}, {"work", "--operation", "op", "--ttl", "2h", "a"},
		{"seal", "--operation", "op", "a"}, {"seal", "--operation", "op", "--work", "w", "--fence", "0", "a"},
		{"renew", "--work", "w", "--fence", "1", "a"}, {"cancel", "a"}, {"delete", "--operation", "../op", "a"},
		{"list", "--limit", "257"}, {"list", "extra"}, {"status", "main"}, {"operation", "a"},
		{"commit", "a"}, {"rollback", "a"}, {"checkpoint", "a"}, {"diff", "a"}, {"fork", "a"},
	} {
		if err := Layer(c, args); err == nil {
			t.Fatal("accepted", args)
		}
	}
}

func TestFixedLayerCLIPreservesFailureReceiptAndUnknownIdentity(t *testing.T) {
	for _, mode := range []string{"failed", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if mode == "unknown" {
					w.WriteHeader(503)
					_, _ = w.Write([]byte(`{"error":"query original operation"}`))
					return
				}
				_, _ = w.Write([]byte(`{"state":"FAILED","code":"UPLOAD_PENDING","layer":{"layer_id":"a","phase":"OPEN"}}`))
			}))
			defer srv.Close()
			out, err := captureStdoutE(t, func() error {
				return Layer(client.New(srv.URL, "test-token"), []string{"seal", "--operation", "original", "--work", "w", "--fence", "1", "a"})
			})
			if err == nil || calls.Load() != 1 {
				t.Fatal(out, err, calls.Load())
			}
			if mode == "failed" {
				if !strings.Contains(out, "UPLOAD_PENDING") || !strings.Contains(err.Error(), "original") {
					t.Fatal(out, err)
				}
			} else {
				var op *client.ViewOperationError
				if !errors.As(err, &op) || op.OperationID != "original" || op.LayerID != "a" || out != "" {
					t.Fatal(out, err)
				}
			}
		})
	}
}

func TestFixedLayerCLIRejectsIncompleteControlReceipts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		response string
	}{
		{"renew without deadline", []string{"renew", "--work", "w", "--fence", "3", "--ttl", "1m", "a"}, `{"layer_id":"a","work_id":"w","fence":3}`},
		{"operation without kind", []string{"operation", "a", "original"}, `{"operation_id":"original","layer_id":"a","state":"DONE"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = w.Write([]byte(tc.response))
			}))
			defer srv.Close()
			out, err := captureStdoutE(t, func() error {
				return Layer(client.New(srv.URL, "test-token"), tc.args)
			})
			if err == nil || out != "" || calls.Load() != 1 {
				t.Fatalf("incomplete receipt printed or retried: output=%q error=%v calls=%d", out, err, calls.Load())
			}
			if tc.args[0] == "operation" {
				var identified *client.ViewOperationError
				if !errors.As(err, &identified) || identified.LayerID != "a" || identified.OperationID != "original" {
					t.Fatalf("original operation identity lost: %v", err)
				}
			}
		})
	}
}
