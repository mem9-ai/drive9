package server

import (
	"net/http"
	"testing"
)

func TestRequestRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want string
	}{
		{path: "/healthz", want: "/healthz"},
		{path: "/metrics", want: "/metrics"},
		{path: "/v1/provision", want: "/v1/provision"},
		{path: "/v1/status", want: "/v1/status"},
		{path: "/v1/tokens", want: "/v1/tokens/*"},
		{path: "/v1/tokens/key1", want: "/v1/tokens/*"},
		{path: "/v1/sql", want: "/v1/sql"},
		{path: "/v1/events", want: "/v1/events"},
		{path: "/v1/fs/doc.txt", want: "/v1/fs/*"},
		{path: "/v1/uploads", want: "/v1/uploads"},
		{path: "/v1/uploads/u1/complete", want: "/v1/uploads/*"},
		{path: "/v2/uploads/u1/parts", want: "/v2/uploads/*"},
		{path: "/v1/vault/secrets", want: "/v1/vault/secrets/*"},
		{path: "/v1/vault/secrets/db-prod", want: "/v1/vault/secrets/*"},
		{path: "/v1/vault/tokens", want: "/v1/vault/tokens/*"},
		{path: "/v1/vault/grants/g1", want: "/v1/vault/grants/*"},
		{path: "/v1/vault/audit", want: "/v1/vault/audit"},
		{path: "/v1/vault/read", want: "/v1/vault/read/*"},
		{path: "/v1/vault/read/secret/path", want: "/v1/vault/read/*"},
		{path: "/s3/tenant-a/object", want: "/s3/*"},
		{path: "/unknown", want: "other"},
	}

	for _, tc := range tests {
		if got := requestRoute(tc.path); got != tc.want {
			t.Fatalf("requestRoute(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

type flushRecorder struct {
	header  http.Header
	flushed bool
}

type plainRecorder struct {
	header http.Header
}

func (r *flushRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *flushRecorder) Write(p []byte) (int, error) { return len(p), nil }

func (r *flushRecorder) WriteHeader(_ int) {}

func (r *flushRecorder) Flush() { r.flushed = true }

func (r *plainRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func (r *plainRecorder) Write(p []byte) (int, error) { return len(p), nil }

func (r *plainRecorder) WriteHeader(_ int) {}

func TestObservedResponseWriterDoesNotAdvertiseFlush(t *testing.T) {
	ow := &observedResponseWriter{ResponseWriter: &plainRecorder{}}

	if _, ok := interface{}(ow).(http.Flusher); ok {
		t.Fatal("observedResponseWriter should not implement http.Flusher")
	}
}

func TestFlusherResponseWriterDelegatesFlush(t *testing.T) {
	rec := &flushRecorder{}
	fw := &flusherResponseWriter{
		observedResponseWriter: &observedResponseWriter{ResponseWriter: rec},
		flusher:                rec,
	}

	if _, ok := interface{}(fw).(http.Flusher); !ok {
		t.Fatal("flusherResponseWriter should implement http.Flusher")
	}

	fw.Flush()
	if !rec.flushed {
		t.Fatal("expected Flush to delegate to wrapped writer")
	}
}
