package client

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMergeFixedViewRoutesValidationAndReceipts(t *testing.T) {
	for _, state := range []string{"DONE", "RUNNING", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			work := ViewWork{LayerID: "a", ID: "validation", Fence: 3, Validation: true}
			outcome := FixedViewOutcome{State: state, Layer: FixedViewState{LayerID: "a", Phase: "MERGED", AdmissionClosed: true}}
			status := 200
			switch state {
			case "RUNNING":
				status, outcome.Layer.Phase = 202, "MERGING"
			case "FAILED":
				outcome.Code, outcome.Layer.Phase = "CONFLICT", "READY"
			}
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/v1/views/a/merge" || r.Header.Get(viewprotocol.ViewHeader) != "" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Errorf("wrong request: %s %s", r.Method, r.URL)
				}
				var req struct {
					OperationID string   `json:"operation_id"`
					Work        ViewWork `json:"work"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.OperationID != "original" || req.Work != work {
					t.Errorf("wrong body: %+v %v", req, err)
				}
				w.Header().Set(viewprotocol.OperationHeader, "original")
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(outcome)
			}))
			defer srv.Close()
			c, err := New(srv.URL, "test-token").WithView(ViewBinding{LayerID: "other", WorkID: "other-work", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.MergeFixedView(t.Context(), work, "original")
			if err != nil || got != outcome || calls != 1 || c.view.LayerID != "other" {
				t.Fatal(got, err, calls)
			}
		})
	}
}

func TestMergeFixedViewRejectsInvalidInputBeforeTransport(t *testing.T) {
	c := New("http://127.0.0.1:1", "test-token")
	c.httpClient.Transport = viewControlRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid input reached transport")
		return nil, nil
	})
	for _, work := range []ViewWork{
		{LayerID: "a", ID: "w", Fence: 1},
		{LayerID: "main", ID: "w", Fence: 1, Validation: true},
		{LayerID: "a", Fence: 1, Validation: true},
		{LayerID: "a", ID: "w", Validation: true},
	} {
		if _, err := c.MergeFixedView(t.Context(), work, "original"); err == nil {
			t.Fatal(work)
		}
	}
	if _, err := c.MergeFixedView(t.Context(), ViewWork{LayerID: "a", ID: "w", Fence: 1, Validation: true}, "../invalid"); err == nil {
		t.Fatal("accepted invalid ID")
	}
}

func TestMergeFixedViewFailuresKeepOriginalIdentity(t *testing.T) {
	const done = `{"state":"DONE","layer":{"layer_id":"a","phase":"MERGED","admission_closed":true}}`
	for _, tc := range []struct {
		name                  string
		status                int
		body, header          string
		transport, brokenBody bool
	}{
		{"pending accounting", 503, `{"code":"ACCOUNTING_PENDING","error":"main committed"}`, "original", false, false},
		{"conflict", 409, `{"error":"changed identity"}`, "original", false, false},
		{"redirect", 307, "", "original", false, false},
		{"transport", 0, "", "", true, false},
		{"body read", 200, done, "original", false, true},
		{"truncated", 200, done[:len(done)-1], "original", false, false},
		{"missing", 200, `{}`, "original", false, false},
		{"wrong layer", 200, strings.Replace(done, `"a"`, `"b"`, 1), "original", false, false},
		{"wrong operation", 200, done, "other", false, false},
		{"missing operation", 200, done, "", false, false},
		{"accepted done", 202, done, "original", false, false},
		{"wrong phase", 200, strings.Replace(done, "MERGED", "MERGING", 1), "original", false, false},
		{"open admission", 200, strings.Replace(done, "true", "false", 1), "original", false, false},
		{"failed without code", 200, `{"state":"FAILED","layer":{"layer_id":"a"}}`, "original", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New("http://127.0.0.1:1", "test-token")
			calls := 0
			c.httpClient.Transport = viewControlRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				if tc.transport {
					return nil, io.ErrUnexpectedEOF
				}
				body := io.NopCloser(strings.NewReader(tc.body))
				if tc.brokenBody {
					body = io.NopCloser(io.MultiReader(strings.NewReader(tc.body), mergeErrorReader{}))
				}
				header := http.Header{}
				header.Set(viewprotocol.OperationHeader, tc.header)
				header.Set("Location", "http://127.0.0.1:1/again")
				return &http.Response{StatusCode: tc.status, Header: header, Body: body}, nil
			})
			out, err := c.MergeFixedView(t.Context(), ViewWork{LayerID: "a", ID: "w", Fence: 1, Validation: true}, "original")
			var identified *ViewOperationError
			if !errors.As(err, &identified) || identified.LayerID != "a" || identified.OperationID != "original" || out != (FixedViewOutcome{}) || calls != 1 {
				t.Fatal(out, err, calls)
			}
		})
	}
}

type mergeErrorReader struct{}

func (mergeErrorReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
