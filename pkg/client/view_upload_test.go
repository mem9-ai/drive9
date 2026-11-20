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

func TestFixedUploadAbortPreservesOutcomeAndOriginalIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		short      bool
		valid      bool
	}{
		{"aborted", `{"operation_id":"abort","upload_id":"u","status":"ABORTED"}`, 200, false, true},
		{"completed", `{"operation_id":"abort","upload_id":"u","status":"COMPLETED"}`, 200, false, true},
		{"expired", `{"operation_id":"abort","upload_id":"u","status":"EXPIRED"}`, 200, false, true},
		{"running", `{"operation_id":"abort","upload_id":"u","status":"UPLOADING"}`, 200, false, false},
		{"rebound-upload", `{"operation_id":"abort","upload_id":"other","status":"ABORTED"}`, 200, false, false},
		{"rebound-operation", `{"operation_id":"other","upload_id":"u","status":"ABORTED"}`, 200, false, false},
		{"trailing-json", `{"operation_id":"abort","upload_id":"u","status":"ABORTED"} {}`, 200, false, false},
		{"truncated", `{"operation_id":"abort","upload_id":"u","status":"ABORTED"}`, 200, true, false},
		{"unknown", `{"error":"query original operation","code":"OUTCOME_UNKNOWN"}`, 503, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/v1/views/a/v2/uploads/u/abort" || r.Header.Get(viewprotocol.OperationHeader) != "abort" || r.Header.Get(viewprotocol.WorkHeader) != "w" {
					t.Error(r.Method, r.URL, r.Header)
				}
				if tc.short {
					w.Header().Set("Content-Length", "1024")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			c, err := New(server.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.AbortFixedUpload(context.Background(), "abort", "u")
			if tc.valid {
				if err != nil || got.OperationID != "abort" || got.UploadID != "u" {
					t.Fatal(got, err)
				}
			} else {
				var op *ViewOperationError
				if !errors.As(err, &op) || op.OperationID != "abort" || op.LayerID != "a" || got != (FixedUploadOutcome{}) {
					t.Fatal(got, err)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("request retried", calls.Load())
			}
		})
	}
}

func TestFixedUploadAbortRejectsInvalidAndImplicitCancellation(t *testing.T) {
	c := New("http://unused.invalid", "test-token")
	if _, err := c.AbortFixedUpload(context.Background(), "abort", "u"); err == nil {
		t.Fatal("main accepted")
	}
	c, err := c.WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"", "u"}, {"abort", "../u"}, {"abort/other", "u"}} {
		if _, err := c.AbortFixedUpload(context.Background(), pair[0], pair[1]); err == nil {
			t.Fatal(pair)
		}
	}
	if err := c.abortUploadV2(context.Background(), "u"); err == nil {
		t.Fatal("implicit fixed cancellation accepted")
	}
	validation, err := c.WithView(ViewBinding{LayerID: "a", WorkID: "w", Fence: 1, Validation: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validation.AbortFixedUpload(context.Background(), "abort", "u"); err == nil {
		t.Fatal("validation accepted")
	}
}
