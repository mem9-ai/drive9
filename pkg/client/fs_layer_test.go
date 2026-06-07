package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCommitFSLayerReturnsConflictBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/fs-layers/layer-1/commit" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(FSLayerCommit{
			Status:  "conflicted",
			LayerID: "layer-1",
			Conflicts: []FSLayerCommitConflict{{
				Path:         "/repo/a.txt",
				Reason:       "base revision changed",
				BaseRevision: 3,
				WantRevision: 2,
			}},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "")
	commit, err := c.CommitFSLayer(context.Background(), "layer-1")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("CommitFSLayer err=%v, want ErrConflict", err)
	}
	if commit == nil || commit.Status != "conflicted" || len(commit.Conflicts) != 1 {
		t.Fatalf("commit=%+v, want conflict body", commit)
	}
	if commit.Conflicts[0].Path != "/repo/a.txt" || commit.Conflicts[0].WantRevision != 2 {
		t.Fatalf("conflict=%+v, want decoded conflict details", commit.Conflicts[0])
	}
}

func TestCommitFSLayerConflictBodyReadError(t *testing.T) {
	c := New("http://drive9.test", "")
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/fs-layers/layer-1/commit" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusConflict,
			Status:     "409 Conflict",
			Header:     make(http.Header),
			Body:       failingReadCloser{},
			Request:    req,
		}, nil
	})}

	commit, err := c.CommitFSLayer(context.Background(), "layer-1")
	if commit != nil {
		t.Fatalf("commit = %+v, want nil", commit)
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("CommitFSLayer err type = %T, want *StatusError", err)
	}
	if statusErr.StatusCode != http.StatusConflict {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusConflict)
	}
	if !strings.Contains(statusErr.Message, "read fs layer commit conflict body") ||
		!strings.Contains(statusErr.Message, "body read failed") {
		t.Fatalf("status message = %q, want conflict body read failure", statusErr.Message)
	}
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("body read failed")
}

func (failingReadCloser) Close() error {
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
