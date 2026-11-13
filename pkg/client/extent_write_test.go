package client

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestWriteExtentStreamRejectsInvalidInputBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer api.Close()
	c := New(api.URL, "")
	fixed, err := c.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path     string
		client         *Client
		source         io.ReaderAt
		size, revision int64
	}{
		{"path", "/../escape", c, bytes.NewReader([]byte("x")), 1, 1},
		{"source", "/file", c, nil, 1, 1},
		{"empty", "/file", c, bytes.NewReader(nil), 0, 1},
		{"size", "/file", c, bytes.NewReader(nil), -1, 1},
		{"revision", "/file", c, bytes.NewReader([]byte("x")), 1, -1},
		{"fixed", "/file", fixed, bytes.NewReader([]byte("x")), 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.client.WriteExtentStreamConditional(t.Context(), tc.path, tc.source, tc.size, nil, tc.revision); err == nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached server")
	}
}

func TestWriteExtentStreamRejectsMissingProtocolAndWrongPlan(t *testing.T) {
	for _, failure := range []string{"missing", "single", "invalid"} {
		t.Run(failure, func(t *testing.T) {
			var calls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/v2/uploads/initiate" {
					t.Error("unexpected request", r.Method, r.URL.Path)
					w.WriteHeader(500)
					return
				}
				var in struct {
					Layout   string
					Revision int64 `json:"expected_revision"`
				}
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Layout != "extent" || in.Revision != 0 {
					t.Error(in, err)
				}
				if failure == "missing" {
					w.WriteHeader(404)
					return
				}
				plan := extentUploadTestPlan(1)
				if failure == "single" {
					plan.ContentLayout = ContentLayoutSingle
				} else {
					plan.PartSize = 0
				}
				w.WriteHeader(202)
				_ = json.NewEncoder(w).Encode(plan)
			}))
			defer api.Close()
			if err := New(api.URL, "").WriteExtentStreamConditional(t.Context(), "/file", bytes.NewReader([]byte("x")), 1, nil, 0); err == nil {
				t.Fatal("invalid protocol accepted")
			}
			if calls.Load() != 1 {
				t.Fatal("fell back or used invalid session", calls.Load())
			}
		})
	}
}

func TestOrdinaryUploadInitiateKeepsDefaultLayout(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
		}
		if _, ok := in["layout"]; ok {
			t.Error("ordinary upload acquired explicit layout")
		}
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{"upload_id":"ordinary"}`)
	}))
	defer api.Close()
	if _, err := New(api.URL, "").initiateUploadV2(t.Context(), "/file", 1, -1, ""); err != nil {
		t.Fatal(err)
	}
}
