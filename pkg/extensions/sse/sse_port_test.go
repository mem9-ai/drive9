package sse

// Rows ported from v1 pkg/fuse/sse_test.go: change events invalidate the
// directory namespace of child directories, foreign structural resets sweep
// caches, and heartbeat resets do nothing.

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
)

// v1 TestSSEWatcherHandleChangeInvalidatesChildDirNamespace: a change under
// a child directory drops that directory's cached listing.
func TestPortSSEChangeInvalidatesChildDirNamespace(t *testing.T) {
	w, bus, _ := newSSE(t, "my-actor", "/")
	ns := dircache.NewNamespaceCache(0, 0, 0)
	ns.Put("/docs/subdir", []fs.DirEntryInfo{{Name: "stale.txt", Size: 50}})
	invalidate := func(e fs.Event) {
		if ns != nil {
			ns.Invalidate(e.Path)
		}
	}
	bus.On(fs.EventInvalidated, invalidate)
	bus.On(fs.EventDelete, invalidate)

	w.handleEvent(&client.ChangeEvent{Seq: 1, Path: "/docs/subdir", Op: "delete", Actor: "other"}, nil)
	if _, ok := ns.Get("/docs/subdir"); ok {
		t.Error("child dir namespace should be invalidated after directory change")
	}
}

// v1 TestSSEWatcherHandleEventReset + TestSSEWatcherForeignStructuralReset-
// ClearsCache: a foreign reset sweeps every cache and notifies the kernel
// wholesale.
func TestPortSSEForeignResetClearsCaches(t *testing.T) {
	w, _, notify := newSSE(t, "me", "/")

	w.handleEvent(nil, &client.ResetEvent{Reason: "structural_change", Actor: "someone-else"})
	if notify.all == 0 {
		t.Error("foreign structural reset must InvalidateAll")
	}
	if !w.current {
		t.Error("reset must mark the stream current")
	}
}

// v1 TestSSEWatcherSelfFilterSkipsOwnStructuralReset: the mount's own
// structural reset is filtered like its own writes.
func TestPortSSESkipOwnStructuralReset(t *testing.T) {
	w, _, notify := newSSE(t, "me", "/")

	w.handleEvent(nil, &client.ResetEvent{Reason: "structural_change", Actor: "me"})
	if notify.all != 0 {
		t.Error("own structural reset must be self-filtered")
	}
}

// v1 TestSSEWatcherHeartbeatDoesNotReset: empty-reason resets (heartbeats)
// are ignored.
func TestPortSSEHeartbeatDoesNotReset(t *testing.T) {
	w, _, notify := newSSE(t, "me", "/")

	w.handleEvent(nil, &client.ResetEvent{Reason: "", Actor: "other"})
	if notify.all != 0 {
		t.Error("heartbeat reset must not sweep caches")
	}
}
