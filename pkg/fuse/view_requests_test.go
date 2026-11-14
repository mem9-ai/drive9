package fuse

import (
	"context"
	"errors"
	"reflect"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Exercise every status-returning method from the actual go-fuse interface.
// A nil delegate makes an accidentally unguarded method fail instead of silently
// passing because the default filesystem happens to return an error too.
func TestViewRequestsPausedRejectsEveryOrdinaryCallback(t *testing.T) {
	v := &viewRequestFS{}
	pause, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pause.resume(); err != nil {
			t.Error(err)
		}
	}()
	raw := reflect.TypeFor[gofuse.RawFileSystem]()
	status := reflect.TypeFor[gofuse.Status]()
	for i := 0; i < raw.NumMethod(); i++ {
		method := raw.Method(i)
		n := method.Type.NumOut()
		if n == 0 || method.Type.Out(n-1) != status {
			continue
		}
		t.Run(method.Name, func(t *testing.T) {
			args := make([]reflect.Value, method.Type.NumIn())
			for j := range args {
				args[j] = reflect.Zero(method.Type.In(j))
			}
			out := reflect.ValueOf(v).MethodByName(method.Name).Call(args)
			if got := out[n-1].Interface().(gofuse.Status); got != gofuse.EAGAIN {
				t.Fatalf("status=%v", got)
			}
			for _, val := range out[:n-1] {
				if !val.IsZero() {
					t.Fatalf("paused callback returned payload %v", val)
				}
			}
		})
	}
}

type viewRequestProbe struct {
	gofuse.RawFileSystem
	lookup      func(*gofuse.EntryOut) gofuse.Status
	release     *gofuse.ReleaseIn
	onRelease   func()
	releaseDir  *gofuse.ReleaseIn
	forgotten   [2]uint64
	initialized bool
	unmounted   bool
}

func (p *viewRequestProbe) Lookup(_ <-chan struct{}, _ *gofuse.InHeader, _ string, out *gofuse.EntryOut) gofuse.Status {
	return p.lookup(out)
}
func (p *viewRequestProbe) Release(_ <-chan struct{}, in *gofuse.ReleaseIn) {
	p.release = in
	if p.onRelease != nil {
		p.onRelease()
	}
}
func (p *viewRequestProbe) ReleaseDir(in *gofuse.ReleaseIn) { p.releaseDir = in }
func (p *viewRequestProbe) Forget(ino, n uint64)            { p.forgotten = [2]uint64{ino, n} }
func (p *viewRequestProbe) Init(*gofuse.Server)             { p.initialized = true }
func (p *viewRequestProbe) OnUnmount()                      { p.unmounted = true }

func TestViewRequestsDrainIncludesCallbackLocalPublication(t *testing.T) {
	entered, remoteFinished, publish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	probe := &viewRequestProbe{lookup: func(out *gofuse.EntryOut) gofuse.Status {
		close(entered)
		// The remote operation finished, but the callback still has local
		// identity/entry publication to perform. Drain must cover this part too.
		close(remoteFinished)
		<-publish
		out.NodeId = 91
		return gofuse.OK
	}}
	v := &viewRequestFS{inner: probe}
	var out gofuse.EntryOut
	done := make(chan gofuse.Status, 1)
	go func() { done <- v.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &out) }()
	<-entered
	pause, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	<-remoteFinished
	select {
	case <-pause.idle:
		t.Fatal("drained before local publication")
	default:
	}
	var blocked gofuse.EntryOut
	if status := v.Lookup(nil, nil, "b", &blocked); status != gofuse.EAGAIN {
		t.Fatal(status)
	}
	close(publish)
	if status := <-done; status != gofuse.OK {
		t.Fatal(status)
	}
	if err := pause.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out.NodeId != 91 {
		t.Fatalf("entry not published: %+v", out)
	}
	if err := pause.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewRequestsFailureReleasesAdmission(t *testing.T) {
	for _, panics := range []bool{false, true} {
		v := &viewRequestFS{inner: &viewRequestProbe{lookup: func(*gofuse.EntryOut) gofuse.Status {
			if panics {
				panic("test callback failure")
			}
			return gofuse.EIO
		}}}
		func() {
			defer func() {
				if got := recover(); (got != nil) != panics {
					t.Errorf("panic=%v", got)
				}
			}()
			if status := v.Lookup(nil, nil, "", nil); status != gofuse.EIO {
				t.Fatal(status)
			}
		}()
		p, err := v.admission.pause()
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-p.idle:
		default:
			t.Fatal("failed callback leaked admission")
		}
		if err := p.resume(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestViewRequestsCleanupContinuesWhilePaused(t *testing.T) {
	probe := &viewRequestProbe{RawFileSystem: gofuse.NewDefaultRawFileSystem()}
	v := &viewRequestFS{inner: probe}
	p, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	file := &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: 91}, Fh: 10}
	dir := &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: 92}, Fh: 11}
	v.Release(nil, file)
	v.ReleaseDir(dir)
	v.Forget(93, 7)
	v.Init(nil)
	v.OnUnmount()
	if probe.release != file || probe.releaseDir != dir || probe.forgotten != [2]uint64{93, 7} || !probe.initialized || !probe.unmounted {
		t.Fatalf("lost cleanup/lifecycle: %+v", probe)
	}
	if err := p.resume(); err != nil {
		t.Fatal(err)
	}
}

func TestViewRequestsLateReleaseMustFinishBeforeDecision(t *testing.T) {
	dropped, finish := make(chan struct{}), make(chan struct{})
	v := &viewRequestFS{inner: &viewRequestProbe{onRelease: func() {
		// FS.Release drops the handle before doing its final release/writeback.
		close(dropped)
		<-finish
	}}}
	pause, err := v.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := pause.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { v.Release(nil, &gofuse.ReleaseIn{Fh: 9}); close(done) }()
	<-dropped
	called := false
	if err := pause.atIdle(context.Background(), func() error { called = true; return nil }); !errors.Is(err, errViewCallbacksActive) || called {
		t.Fatalf("unfinished release accepted: called=%v err=%v", called, err)
	}
	close(finish)
	<-done
	if err := pause.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pause.atIdle(context.Background(), func() error { called = true; return nil }); err != nil || !called {
		t.Fatalf("finished release: called=%v err=%v", called, err)
	}
	if err := pause.resume(); err != nil {
		t.Fatal(err)
	}
}
