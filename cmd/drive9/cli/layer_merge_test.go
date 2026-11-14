package cli

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedLayerCLIMerge(t *testing.T) {
	for _, state := range []string{"DONE", "RUNNING", "FAILED", "ACCOUNTING_PENDING"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var req struct {
					OperationID string          `json:"operation_id"`
					Work        client.ViewWork `json:"work"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.OperationID != "original" || req.Work.LayerID != "a" || req.Work.ID != "validate" || req.Work.Fence != 3 || !req.Work.Validation || r.URL.Path != "/v1/views/a/merge" {
					t.Errorf("request %+v %v", req, err)
				}
				w.Header().Set(viewprotocol.OperationHeader, "original")
				out := client.FixedViewOutcome{State: state, Layer: client.FixedViewState{LayerID: "a", Phase: "MERGED", AdmissionClosed: true}}
				switch state {
				case "RUNNING":
					out.Layer.Phase = "MERGING"
					w.WriteHeader(202)
				case "FAILED":
					out.Layer.Phase = "READY"
					out.Code = "CONFLICT"
				case "ACCOUNTING_PENDING":
					w.WriteHeader(503)
					_, _ = w.Write([]byte(`{"code":"ACCOUNTING_PENDING","error":"retry original POST"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(out)
			}))
			defer srv.Close()
			out, err := captureStdoutE(t, func() error {
				return Layer(client.New(srv.URL, "test-token"), []string{"merge", "--operation", "original", "--work", "validate", "--fence", "3", "a"})
			})
			if calls != 1 || (err == nil) != (state == "DONE") {
				t.Fatal(out, err, calls)
			}
			if state == "ACCOUNTING_PENDING" {
				var op *client.ViewOperationError
				if out != "" || !errors.As(err, &op) || op.OperationID != "original" || op.LayerID != "a" {
					t.Fatal(out, err)
				}
			} else if !strings.Contains(out, state) {
				t.Fatal(out)
			}
		})
	}
}

func TestFixedLayerCLIMergeRequiresOriginalIdentity(t *testing.T) {
	for _, args := range [][]string{
		{"merge", "a"},
		{"merge", "--operation", "op", "a"},
		{"merge", "--operation", "op", "--work", "w", "--fence", "0", "a"},
		{"merge", "--operation", "../op", "--work", "w", "--fence", "1", "a"},
	} {
		if _, _, err := parseFixedLayerOptions(args); err == nil {
			t.Fatal(args)
		}
	}
}
