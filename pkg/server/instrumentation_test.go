package server

import (
	"net/http"
	"testing"
)

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
