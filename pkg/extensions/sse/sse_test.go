package sse

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

type recordingNotifier struct {
	content []uint64
	entries [][2]any // parentIno, name
	all     int
}

func (n *recordingNotifier) InvalidateEntry(parentIno uint64, name string) {
	n.entries = append(n.entries, [2]any{parentIno, name})
}

func (n *recordingNotifier) InvalidateContent(ino uint64, size uint64) {
	n.content = append(n.content, ino)
}

func (n *recordingNotifier) InvalidateAll() { n.all++ }

type fakeInodes struct{}

func (fakeInodes) Ino(path string) (uint64, bool) {
	switch path {
	case "/sub/f":
		return 7, true
	case "/sub":
		return 3, true
	}
	return 0, false
}
func (fakeInodes) Path(ino uint64) (string, bool) { return "", false }
func (fakeInodes) Epoch(path string) uint64       { return 0 }
func (fakeInodes) NextEpoch(path string) uint64   { return 1 }

func newSSE(t *testing.T, actor, root string) (*SSE, *fs.EventBus, *recordingNotifier) {
	t.Helper()
	bus := fs.NewEventBus()
	notify := &recordingNotifier{}
	w, err := New(Options{Actor: actor, RemoteRoot: root, Events: bus, Inodes: fakeInodes{}, Notify: notify})
	if err != nil {
		t.Fatal(err)
	}
	return w, bus, notify
}

func TestChangeEventInvalidatesAndNotifies(t *testing.T) {
	w, bus, notify := newSSE(t, "actor-1", "/")
	var got []fs.Event
	bus.On(fs.EventInvalidated, func(e fs.Event) { got = append(got, e) })

	w.handleEvent(&client.ChangeEvent{Path: "/sub/f", Op: "write", Actor: "other"}, nil)
	if len(got) != 1 || got[0].Path != "/sub/f" {
		t.Fatalf("events = %+v", got)
	}
	if len(notify.content) != 1 || notify.content[0] != 7 {
		t.Fatalf("content invalidations = %v", notify.content)
	}
	if len(notify.entries) != 1 || notify.entries[0] != [2]any{uint64(3), "f"} {
		t.Fatalf("entry invalidations = %v", notify.entries)
	}
}

func TestSelfFiltering(t *testing.T) {
	w, bus, notify := newSSE(t, "actor-1", "/")
	var got []fs.Event
	bus.On(fs.EventInvalidated, func(e fs.Event) { got = append(got, e) })

	w.handleEvent(&client.ChangeEvent{Path: "/sub/f", Op: "write", Actor: "actor-1"}, nil)
	if len(got) != 0 || len(notify.content) != 0 {
		t.Fatal("own actor event was not filtered")
	}
}

func TestRemoteRootMappingAndDelete(t *testing.T) {
	w, bus, _ := newSSE(t, "", "/tenant-a")
	var invalidated, deleted []fs.Event
	bus.On(fs.EventInvalidated, func(e fs.Event) { invalidated = append(invalidated, e) })
	bus.On(fs.EventDelete, func(e fs.Event) { deleted = append(deleted, e) })

	w.handleEvent(&client.ChangeEvent{Path: "/tenant-a/x", Op: "write"}, nil)
	w.handleEvent(&client.ChangeEvent{Path: "/tenant-a/y", Op: "delete"}, nil)
	w.handleEvent(&client.ChangeEvent{Path: "/other/z", Op: "write"}, nil)

	if len(invalidated) != 1 || invalidated[0].Path != "/x" {
		t.Fatalf("invalidated = %+v", invalidated)
	}
	if len(deleted) != 1 || deleted[0].Path != "/y" {
		t.Fatalf("deleted = %+v", deleted)
	}
}

func TestResetSweepsEverything(t *testing.T) {
	w, bus, notify := newSSE(t, "", "/")
	var checkout []fs.Event
	bus.On(fs.EventCheckout, func(e fs.Event) { checkout = append(checkout, e) })

	w.handleEvent(nil, &client.ResetEvent{Reason: "initial_sync", Seq: 9})
	if len(checkout) != 1 || notify.all != 1 {
		t.Fatalf("checkout=%v all=%d", checkout, notify.all)
	}
	if !w.current {
		t.Fatal("reset did not mark stream current")
	}
}

func TestLifecycleStopWithoutStart(t *testing.T) {
	w, _, _ := newSSE(t, "", "/")
	if err := w.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
