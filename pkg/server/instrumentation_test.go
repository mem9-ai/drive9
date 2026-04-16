package server

import (
	"net/http"
	"testing"
)

type flushRecorder struct {
	header  http.Header
	flushed bool
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

func TestObservedResponseWriterFlushDelegates(t *testing.T) {
	rec := &flushRecorder{}
	ow := &observedResponseWriter{ResponseWriter: rec}

	if _, ok := interface{}(ow).(http.Flusher); !ok {
		t.Fatal("observedResponseWriter should implement http.Flusher")
	}

	ow.Flush()
	if !rec.flushed {
		t.Fatal("expected Flush to delegate to wrapped writer")
	}
}
