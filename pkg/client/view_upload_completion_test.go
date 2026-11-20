package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedCompletionRetryPreservesRequestAndChecksReceipt(t *testing.T) {
	var attempts atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/views/a/v2/uploads/upload/complete" || r.Header.Get(viewprotocol.OperationHeader) != "original" || r.Header.Get(viewprotocol.WorkHeader) != "work" {
			t.Error("recovery changed bound request")
		}
		var payload struct{ Parts []completePart }
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Parts) != 1 || payload.Parts[0].ETag != strings.Repeat("a", 64) {
			t.Error(payload, err)
		}
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(503)
			return
		}
		size := int64(6)
		if attempt == 2 {
			size = 5
		}
		_ = json.NewEncoder(w).Encode(FixedUploadCompletion{Status: "completed", OperationID: "original", View: FixedViewState{LayerID: "a", Generation: 2, Fence: 1}, ResourceID: "inode", Revision: 3, Size: size})
	}))
	defer api.Close()
	c, err := New(api.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	parts := []completePart{{Number: 1, ETag: strings.Repeat("a", 64)}}
	_, err = c.CompleteFixedExtentUploadWithSize(context.Background(), "original", "upload", parts, 6)
	var pending *FixedExtentCompletionError
	if !errors.As(err, &pending) || !IsCommitAttempted(err) {
		t.Fatal(err)
	}
	// Caller mutation and a different ambient operation ID must not alter replay.
	parts[0].ETag = strings.Repeat("b", 64)
	ctx, err := WithViewOperationID(context.Background(), "different")
	if err != nil {
		t.Fatal(err)
	}
	result, err := pending.Retry(ctx)
	if err == nil || result.Revision != 0 || !IsCommitAttempted(err) {
		t.Fatal(result, err)
	}
	result, err = pending.Retry(ctx)
	if err != nil || result.Size != 6 || result.Revision != 3 || attempts.Load() != 3 {
		t.Fatal(result, err)
	}
	for _, empty := range []*FixedExtentCompletionError{nil, {}} {
		if _, err := empty.Retry(ctx); err == nil {
			t.Fatal("empty completion request accepted")
		}
	}
}

func TestFixedCompletionRejectsInvalidExpectedSizeBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer api.Close()
	c, _ := New(api.URL, "").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	parts := []FixedExtentPart{{Number: 1, ETag: strings.Repeat("a", 64)}}
	for _, size := range []int64{-1, 0, 256*(64<<20) + 1} {
		if result, err := c.CompleteFixedExtentUploadWithSize(context.Background(), "original", "upload", parts, size); err == nil || result.Revision != 0 || IsCommitAttempted(err) {
			t.Fatal(result, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid size issued completion", calls.Load())
	}
}
