package extent

import (
	"context"
	"errors"
	"testing"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/extent"
)

type failingStopMeta struct {
	jfsmeta.Meta
	calls int
	err   error
}

func (m *failingStopMeta) CloseSession() error { m.calls++; return m.err }

func TestStopRetainsBusyRuntimeUntilMetadataDrains(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := &extentShutdownRecorder{ObjectStorage: inner}
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	hold := newExtentMetaHold()
	if !hold.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("fixture did not admit lookup")
	}
	runtime := &extentRuntime{rt: &extent.Runtime{Storage: store}, hold: hold}
	e.rt.Store(runtime)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := e.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unfinished Stop: %v", err)
	}
	if e.rt.Load() != runtime || store.shutdowns.Load() != 0 {
		t.Fatal("released runtime before metadata drained")
	}
	if hold.enter(jfsmeta.Drive9OpLookup) {
		t.Fatal("admitted new metadata after stop began")
	}
	hold.leave()
	if err := e.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if e.rt.Load() != nil || store.shutdowns.Load() != 1 {
		t.Fatal("completed Stop did not release exactly once")
	}
	if err := e.Stop(ctx); err != nil || store.shutdowns.Load() != 1 {
		t.Fatalf("completed Stop retry: %v", err)
	}
}

func TestStopDoesNotLoseCloseSessionFailureOnRetry(t *testing.T) {
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := &extentShutdownRecorder{ObjectStorage: inner}
	cause := errors.New("session close unconfirmed")
	meta := &failingStopMeta{err: cause}
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.rt.Store(&extentRuntime{rt: &extent.Runtime{Storage: store, Meta: meta}, hold: newExtentMetaHold()})
	for i := 0; i < 2; i++ {
		if err := e.Stop(t.Context()); !errors.Is(err, cause) {
			t.Fatalf("lost close error on attempt %d: %v", i, err)
		}
	}
	if meta.calls != 1 || store.shutdowns.Load() != 1 || e.rt.Load() != nil {
		t.Fatalf("repeated close: calls=%d shutdowns=%d", meta.calls, store.shutdowns.Load())
	}
}
