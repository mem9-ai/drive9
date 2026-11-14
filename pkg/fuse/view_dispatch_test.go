package fuse

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type viewDispatchProbe struct {
	gofuse.RawFileSystem
	mode    uint32
	getattr func()
	release func()
	init    func()
	unmount func()
}

func (p *viewDispatchProbe) GetAttr(_ <-chan struct{}, _ *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	if p.getattr != nil {
		p.getattr()
	}
	out.Mode = p.mode
	return gofuse.OK
}
func (p *viewDispatchProbe) Release(_ <-chan struct{}, _ *gofuse.ReleaseIn) {
	if p.release != nil {
		p.release()
	}
}
func (p *viewDispatchProbe) Init(_ *gofuse.Server) {
	if p.init != nil {
		p.init()
	}
}
func (p *viewDispatchProbe) OnUnmount() {
	if p.unmount != nil {
		p.unmount()
	}
}

func TestViewDispatchChangesOnlyAfterCompleteCallback(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	old := &viewDispatchProbe{mode: 0700, getattr: func() { close(entered); <-release }}
	next := &viewDispatchProbe{mode: 0755}
	v := &viewRequestFS{inner: old}
	var oldReply gofuse.AttrOut
	done := make(chan gofuse.Status, 1)
	go func() { done <- v.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: 1}}, &oldReply) }()
	<-entered
	pause, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := v.installTarget(t.Context(), pause, 1, 2, next); !errors.Is(err, errViewCallbacksActive) {
		t.Fatal(err)
	}
	close(release)
	if status := <-done; status != gofuse.OK || oldReply.Mode != 0700 {
		t.Fatalf("old callback changed: %v %+v", status, oldReply)
	}
	if err := v.installTarget(t.Context(), pause, 1, 2, next); err != nil {
		t.Fatal(err)
	}
	var reply gofuse.AttrOut
	if st := v.GetAttr(nil, &gofuse.GetAttrIn{}, &reply); st != gofuse.EAGAIN {
		t.Fatalf("opened admission before completion: %v", st)
	}
	if got := v.currentTarget(); got.generation != 2 || got.fs != next {
		t.Fatalf("target=%+v", got)
	}
	if err := pause.resume(); err != nil {
		t.Fatal(err)
	}
	if st := v.GetAttr(nil, &gofuse.GetAttrIn{}, &reply); st != gofuse.OK || reply.Mode != 0755 {
		t.Fatalf("new root=%+v status=%v", reply, st)
	}
	// Reusing an old pause cannot install an old operation over the new view.
	if err := v.installTarget(t.Context(), pause, 1, 2, old); !errors.Is(err, errViewPauseLost) {
		t.Fatal(err)
	}
}

func TestViewDispatchRejectsInvalidTransitions(t *testing.T) {
	old, next := &viewDispatchProbe{}, &viewDispatchProbe{}
	v := &viewRequestFS{inner: old}
	pause, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]uint64{{0, 1}, {1, 1}, {1, 3}, {2, 3}, {math.MaxUint64, 0}} {
		if err := v.installTarget(t.Context(), pause, pair[0], pair[1], next); !errors.Is(err, errViewGenerationConflict) {
			t.Fatalf("%v: %v", pair, err)
		}
	}
	if err := v.installTarget(t.Context(), pause, 1, 2, nil); !errors.Is(err, errViewGenerationConflict) {
		t.Fatal(err)
	}
	var missing *viewDispatchProbe
	if err := v.installTarget(t.Context(), pause, 1, 2, missing); !errors.Is(err, errViewGenerationConflict) {
		t.Fatal(err)
	}
	other := &viewAdmission{}
	otherPause, _ := other.pause()
	if err := v.installTarget(t.Context(), otherPause, 1, 2, next); !errors.Is(err, errViewPauseLost) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := v.installTarget(ctx, pause, 1, 2, next); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got := v.currentTarget(); got.generation != 1 || got.fs != old {
		t.Fatalf("changed on failure: %+v", got)
	}
}

func TestViewDispatchWaitsForCleanupAndLifecycle(t *testing.T) {
	for _, name := range []string{"release", "init", "unmount"} {
		t.Run(name, func(t *testing.T) {
			entered, finish := make(chan struct{}), make(chan struct{})
			block := func() { close(entered); <-finish }
			old := &viewDispatchProbe{}
			v := &viewRequestFS{inner: old}
			pause, _ := v.admission.pause()
			done := make(chan struct{})
			switch name {
			case "release":
				old.release = block
			case "init":
				old.init = block
			case "unmount":
				old.unmount = block
			}
			go func() {
				defer close(done)
				switch name {
				case "release":
					v.Release(nil, &gofuse.ReleaseIn{})
				case "init":
					v.Init(nil)
				case "unmount":
					v.OnUnmount()
				}
			}()
			<-entered
			next := &viewDispatchProbe{}
			if err := v.installTarget(t.Context(), pause, 1, 2, next); !errors.Is(err, errViewCallbacksActive) {
				t.Fatal(err)
			}
			close(finish)
			<-done
			err := v.installTarget(t.Context(), pause, 1, 2, next)
			if name == "unmount" {
				if !errors.Is(err, errViewUnmounted) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestViewDispatchSnapshotDoesNotRaceInstallation(t *testing.T) {
	v := &viewRequestFS{inner: &viewDispatchProbe{}}
	pause, _ := v.admission.pause()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				got := v.currentTarget()
				if got.fs == nil || got.generation == 0 {
					t.Error("torn binding")
				}
			}
		}()
	}
	for gen := uint64(1); gen < 100; gen++ {
		if err := v.installTarget(t.Context(), pause, gen, gen+1, &viewDispatchProbe{}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}
