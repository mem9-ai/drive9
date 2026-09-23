package fuse

import (
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/extent"
)

// extentShutdownRecorder is an object store that records Shutdown calls.
type extentShutdownRecorder struct {
	object.ObjectStorage
	shutdowns atomic.Int64
}

func (s *extentShutdownRecorder) Shutdown() { s.shutdowns.Add(1) }

// TestExtentTeardownStopsAndClosesRuntime pins the teardown hook: it marks the
// fs torn down, stops the compaction loop, closes the runtime exactly once,
// clears the pointer, and refuses a later ensureExtentRuntime.
func TestExtentTeardownStopsAndClosesRuntime(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rec := &extentShutdownRecorder{ObjectStorage: inner}
	var stopped atomic.Bool
	fs := &Dat9FS{}
	fs.extentRT = &extentRuntime{
		rt:   &extent.Runtime{Storage: rec},
		stop: func() { stopped.Store(true) },
	}

	fs.stopExtentRuntimeLoop()
	if !stopped.Load() {
		t.Fatal("compaction loop was not stopped")
	}
	if !fs.extentTornDown {
		t.Fatal("fs was not marked torn down")
	}

	fs.closeExtentRuntime()
	if fs.extentRT != nil {
		t.Fatal("extentRT still set after teardown")
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("runtime shutdowns = %d, want 1", got)
	}

	if err := fs.ensureExtentRuntime(); err == nil {
		t.Fatal("ensureExtentRuntime must refuse after teardown")
	}
}
