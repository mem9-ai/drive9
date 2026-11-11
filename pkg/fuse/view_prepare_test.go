package fuse

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestViewPrepareDropsIdleReferencesButRetainsInvalidationPlan(t *testing.T) {
	current := newXattrFS(t)
	adapter := &viewRequestFS{inner: current}
	var found gofuse.EntryOut
	if st := adapter.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &found); st != gofuse.OK {
		t.Fatal(st)
	}
	probe := &viewCacheProbe{entry: func(parent uint64, name string) gofuse.Status {
		if parent != 1 || name != "f" {
			t.Fatalf("unexpected alias %d/%s", parent, name)
		}
		// Real Forget callback, counted through the admission adapter.
		adapter.Forget(found.NodeId, 1)
		return gofuse.OK
	}}
	fences := 0
	pause, plan, err := adapter.prepareViewSwitch(t.Context(), current, probe, func(context.Context) error { fences++; return nil })
	if err != nil || pause == nil || fences != 1 {
		t.Fatalf("prepare: %v %v fences=%d", pause, err, fences)
	}
	if current.nonRootKernelLookupCount() != 0 {
		t.Fatal("old lookup not released")
	}
	if st := adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &gofuse.AttrOut{}); st != gofuse.EAGAIN {
		t.Fatal("preparation resumed ordinary I/O")
	}
	if len(plan.inodes) != 2 || len(plan.entries) != 1 || plan.entries[0].name != "f" {
		t.Fatalf("lost old invalidation plan: %+v", plan)
	}
	if got := adapter.currentTarget(); got.fs != current || got.generation != 1 {
		t.Fatalf("preparation changed binding: %+v", got)
	}
	if err := pause.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewPreparePinnedNonRootReferencesAreBusyAndOldViewResumes(t *testing.T) {
	current := newXattrFS(t)
	adapter := &viewRequestFS{inner: current}
	var found gofuse.EntryOut
	if st := adapter.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &found); st != gofuse.OK {
		t.Fatal(st)
	}
	probe := &viewCacheProbe{}
	pause, _, err := adapter.prepareViewSwitch(t.Context(), current, probe, func(context.Context) error { return nil })
	if pause != nil || !errors.Is(err, syscall.EBUSY) || len(probe.calls) == 0 {
		t.Fatalf("pinned reference: pause=%v err=%v", pause, err)
	}
	var out gofuse.AttrOut
	if st := adapter.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: found.NodeId}}, &out); st != gofuse.OK || out.Size != 1 {
		t.Fatalf("old reference lost: %v %+v", st, out)
	}
}

func TestViewPrepareOpenHandleRejectsBeforeDataInvalidation(t *testing.T) {
	current := newXattrFS(t)
	adapter := &viewRequestFS{inner: current}
	var created gofuse.CreateOut
	if st := adapter.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0600}, "open", &created); st != gofuse.OK {
		t.Fatal(st)
	}
	defer adapter.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
	probe := &viewCacheProbe{}
	pause, _, err := adapter.prepareViewSwitch(t.Context(), current, probe, func(context.Context) error { t.Fatal("fenced open handle"); return nil })
	if pause != nil || !errors.Is(err, syscall.EBUSY) || len(probe.calls) != 0 {
		t.Fatalf("handle refused too late: %v %v", err, probe.calls)
	}
	if leave, err := adapter.admission.enter(); err != nil {
		t.Fatal("failed preparation retained pause")
	} else {
		leave()
	}
}

func TestViewPrepareFailuresResumeOldAdmission(t *testing.T) {
	for _, mode := range []string{"notify", "fence", "cancel", "unmount", "extension"} {
		t.Run(mode, func(t *testing.T) {
			current := newXattrFS(t)
			adapter := &viewRequestFS{inner: current}
			probe := &viewCacheProbe{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fence := func(context.Context) error { return nil }
			switch mode {
			case "notify":
				probe.inode = func(uint64) gofuse.Status { return gofuse.EIO }
			case "fence":
				fence = func(context.Context) error { return syscall.EIO }
			case "cancel":
				probe.inode = func(uint64) gofuse.Status { cancel(); return gofuse.OK }
			case "unmount":
				probe.inode = func(uint64) gofuse.Status { adapter.OnUnmount(); return gofuse.OK }
			case "extension":
				if st := adapter.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: gofuse.InHeader{NodeId: 1}}, "user.k", []byte("v")); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			pause, _, err := adapter.prepareViewSwitch(ctx, current, probe, fence)
			if pause != nil || err == nil {
				t.Fatalf("failure accepted: %v %v", pause, err)
			}
			if leave, err := adapter.admission.enter(); err != nil {
				t.Fatal("failed preparation retained pause")
			} else {
				leave()
			}
			if got := adapter.currentTarget(); got.fs != current || got.generation != 1 {
				t.Fatalf("changed binding on failure: %+v", got)
			}
		})
	}
}

func TestViewPrepareRequiresSameRuntimeAndRechecksBeforeDecision(t *testing.T) {
	current := newXattrFS(t)
	adapter := &viewRequestFS{inner: current}
	probe := &viewCacheProbe{}
	fence := func(context.Context) error { return nil }
	if pause, _, err := adapter.prepareViewSwitch(t.Context(), newXattrFS(t), probe, fence); pause != nil || !errors.Is(err, errViewGenerationConflict) {
		t.Fatalf("foreign runtime: %v", err)
	}
	pause, _, err := adapter.prepareViewSwitch(t.Context(), current, probe, fence)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pause.resume() }()
	// A preparation result cannot waive a later cleanup/lifecycle callback.
	leave := adapter.admission.enterCleanup()
	if err := pause.atIdle(t.Context(), func() error { return adapter.checkViewSwitch(current) }); !errors.Is(err, errViewCallbacksActive) {
		t.Fatalf("cleanup not rechecked: %v", err)
	}
	leave()
	adapter.unmounted.Store(true)
	if err := pause.atIdle(t.Context(), func() error { return adapter.checkViewSwitch(current) }); !errors.Is(err, errViewUnmounted) {
		t.Fatalf("mount not rechecked: %v", err)
	}
}

type viewPrepareDrainFailure struct{}

func (viewPrepareDrainFailure) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return next(ctx, op)
}
func (viewPrepareDrainFailure) Start(context.Context) error { return nil }
func (viewPrepareDrainFailure) Stop(context.Context) error  { return nil }
func (viewPrepareDrainFailure) Drain(context.Context) error {
	return errors.New("pending write failed")
}

func TestViewPrepareDrainFailureDoesNotInvalidateOrDecide(t *testing.T) {
	current := newXattrFS(t)
	handlers := append([]fs.Handler{viewPrepareDrainFailure{}}, current.chain.Handlers()...)
	chain, err := fs.NewChain(handlers...)
	if err != nil {
		t.Fatal(err)
	}
	current.chain = chain
	adapter := &viewRequestFS{inner: current}
	probe := &viewCacheProbe{}
	pause, _, err := adapter.prepareViewSwitch(t.Context(), current, probe, func(context.Context) error { t.Fatal("fenced failed drain"); return nil })
	if pause != nil || err == nil || !strings.Contains(err.Error(), "pending write failed") || len(probe.calls) != 0 {
		t.Fatalf("drain failure=%v pause=%v notifications=%v", err, pause, probe.calls)
	}
	if leave, err := adapter.admission.enter(); err != nil {
		t.Fatal(err)
	} else {
		leave()
	}
	if got := adapter.currentTarget(); got.fs != current || got.generation != 1 {
		t.Fatalf("changed on failed drain: %+v", got)
	}
}
