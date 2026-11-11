//go:build !windows

package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func viewInstallFixture(t *testing.T) (*viewInstallation, *viewCacheProbe, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "switch.log")
	journal, err := mountstate.CreateSwitchJournal(path, "mount-1", "scope-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	v := &viewRequestFS{inner: &viewDispatchProbe{mode: 0700}}
	pause, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	nodes := NewInodeToPath()
	nodes.Lookup("/a", false, 1, time.Time{})
	plan, err := prepareViewCacheInvalidation(nodes.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Decide("switch-a", 1, viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}); err != nil {
		t.Fatal(err)
	}
	probe := &viewCacheProbe{}
	i, err := newViewInstallation(journal, v, pause, &viewDispatchProbe{mode: 0755}, plan, probe, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	// Mutation by the constructor's caller cannot alter this operation's plan.
	plan.inodes[0] = 99
	plan.entries[0].name = "changed"
	return i, probe, path
}

func assertViewInstallClosed(t *testing.T, i *viewInstallation, receipt *mountstate.SwitchReceipt, generation uint64) {
	t.Helper()
	if receipt == nil || receipt.State != mountstate.SwitchRecovering || receipt.OperationID != "switch-a" || receipt.Target.LayerID != "a" {
		t.Fatalf("lost durable decision: %+v", receipt)
	}
	if target := i.adapter.currentTarget(); target.generation != generation {
		t.Fatalf("generation=%d want=%d", target.generation, generation)
	}
	if st := i.adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &gofuse.AttrOut{}); st != gofuse.EAGAIN {
		t.Fatalf("admitted request during recovery: %v", st)
	}
	select {
	case <-i.pause.released:
		t.Fatal("pause released on failure")
	default:
	}
}

func TestViewInstallationCompletesAfterNotificationsAndReplaysWithoutSideEffects(t *testing.T) {
	i, probe, path := viewInstallFixture(t)
	probe.inode = func(uint64) gofuse.Status {
		r, found, err := i.journal.Lookup("switch-a")
		if err != nil || !found {
			t.Fatalf("lookup: %v %v", found, err)
		}
		assertViewInstallClosed(t, i, &r, 2)
		return gofuse.OK
	}
	r, err := i.finish(t.Context())
	if err != nil || r == nil || r.State != mountstate.SwitchDone {
		t.Fatalf("finish: %+v %v", r, err)
	}
	want := []viewCacheEntry{{parent: 1}, {parent: 2}, {parent: 1, name: "a"}}
	if !reflect.DeepEqual(probe.calls, want) {
		t.Fatalf("notification plan changed: %v", probe.calls)
	}
	var out gofuse.AttrOut
	if st := i.adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &out); st != gofuse.OK || out.Mode != 0755 {
		t.Fatalf("target not active: %v %+v", st, out)
	}
	if err := i.journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err := mountstate.OpenSwitchJournal(path, "mount-1", "scope-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	got, found, err := journal.Lookup("switch-a")
	if err != nil || !found || got != *r {
		t.Fatalf("not durable: %+v %v %v", got, found, err)
	}
	// A successful old operation cannot disturb a subsequent paused switch.
	later, _ := i.adapter.admission.pause()
	if err := i.adapter.installTarget(t.Context(), later, 2, 3, &viewDispatchProbe{mode: 0600}); err != nil {
		t.Fatal(err)
	}
	r.Target.LayerID = "caller-mutated"
	replay, err := i.finish(t.Context())
	if err != nil || replay == nil || *replay != got || len(probe.calls) != len(want) || i.adapter.currentTarget().generation != 3 {
		t.Fatalf("replay changed operation: %+v %v", replay, err)
	}
	if st := i.adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &out); st != gofuse.EAGAIN {
		t.Fatal("old replay reopened successor pause")
	}
}

func TestViewInstallationFenceFailureKeepsDecisionAndOldDispatch(t *testing.T) {
	i, probe, _ := viewInstallFixture(t)
	i.fenceRoot = func(context.Context) error { return syscall.EIO }
	r, err := i.finish(t.Context())
	if !errors.Is(err, syscall.EIO) || len(probe.calls) != 0 {
		t.Fatalf("fence: %+v %v %v", r, err, probe.calls)
	}
	assertViewInstallClosed(t, i, r, 1)
	i.fenceRoot = func(context.Context) error { return nil }
	if _, err := i.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestViewInstallationPartialInvalidationRetriesOriginalGeneration(t *testing.T) {
	i, probe, _ := viewInstallFixture(t)
	probe.entry = func(uint64, string) gofuse.Status { return gofuse.EIO }
	r, err := i.finish(t.Context())
	if !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	assertViewInstallClosed(t, i, r, 2)
	first := append([]viewCacheEntry(nil), probe.calls...)
	probe.entry = nil
	probe.calls = nil
	if _, err := i.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(probe.calls, first) || i.adapter.currentTarget().generation != 2 {
		t.Fatalf("retry changed plan or generation: %v", probe.calls)
	}
}

func TestViewInstallationCancellationBeforeAndAfterInstall(t *testing.T) {
	for _, afterInstall := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[afterInstall], func(t *testing.T) {
			i, probe, _ := viewInstallFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			generation := uint64(1)
			if afterInstall {
				generation = 2
				probe.inode = func(uint64) gofuse.Status { cancel(); return gofuse.OK }
			} else {
				cancel()
			}
			r, err := i.finish(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			assertViewInstallClosed(t, i, r, generation)
			probe.inode = nil
			if _, err := i.finish(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestViewInstallationCleanupAndUnmountCannotComplete(t *testing.T) {
	for _, unmount := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup", true: "unmount"}[unmount], func(t *testing.T) {
			i, probe, _ := viewInstallFixture(t)
			var leave func()
			probe.entry = func(uint64, string) gofuse.Status {
				if unmount {
					i.adapter.OnUnmount()
				} else {
					leave = i.adapter.admission.enterCleanup()
				}
				return gofuse.OK
			}
			r, err := i.finish(t.Context())
			want := errViewCallbacksActive
			if unmount {
				want = errViewUnmounted
			}
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
			assertViewInstallClosed(t, i, r, 2)
			if leave != nil {
				leave()
				probe.entry = nil
				if _, err := i.finish(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestViewInstallationUnconfirmedJournalCompletionStaysClosed(t *testing.T) {
	i, probe, path := viewInstallFixture(t)
	probe.entry = func(uint64, string) gofuse.Status {
		if err := i.journal.Close(); err != nil {
			t.Fatal(err)
		}
		return gofuse.OK
	}
	r, err := i.finish(t.Context())
	if err == nil || r != nil {
		t.Fatalf("unconfirmed result claimed: %+v %v", r, err)
	}
	journal, err := mountstate.OpenSwitchJournal(path, "mount-1", "scope-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	decision, found, err := journal.Lookup("switch-a")
	if err != nil || !found {
		t.Fatalf("lost recovery: %v %v", found, err)
	}
	assertViewInstallClosed(t, i, &decision, 2)
}

func TestViewInstallationSerializesAttempts(t *testing.T) {
	i, probe, _ := viewInstallFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	i.fenceRoot = func(context.Context) error { close(entered); <-release; return nil }
	done := make(chan error, 1)
	go func() { _, err := i.finish(t.Context()); done <- err }()
	<-entered
	r, err := i.finish(t.Context())
	if !errors.Is(err, mountstate.ErrSwitchPending) || r != nil || len(probe.calls) != 0 {
		t.Fatalf("concurrent finish accepted: %+v %v", r, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestViewInstallationRequiresDurableDecisionAndRoot(t *testing.T) {
	i, probe, _ := viewInstallFixture(t)
	if _, err := newViewInstallation(i.journal, i.adapter, i.pause, i.target, viewCacheInvalidation{}, probe, i.fenceRoot); err == nil {
		t.Fatal("missing root accepted")
	}
	if _, err := newViewInstallation(i.journal, i.adapter, nil, i.target, i.plan, probe, i.fenceRoot); err == nil {
		t.Fatal("missing ownership accepted")
	}
	if _, err := i.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	pause, _ := i.adapter.admission.pause()
	if _, err := newViewInstallation(i.journal, i.adapter, pause, i.target, i.plan, probe, i.fenceRoot); !errors.Is(err, mountstate.ErrSwitchConflict) {
		t.Fatalf("no pending decision accepted: %v", err)
	}
}
