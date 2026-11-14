//go:build !windows

package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type viewSwitchLifecycle struct{ stop func(context.Context) error }

func (p *viewSwitchLifecycle) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return next(ctx, op)
}
func (p *viewSwitchLifecycle) Start(context.Context) error    { return nil }
func (p *viewSwitchLifecycle) Drain(context.Context) error    { return nil }
func (p *viewSwitchLifecycle) Stop(ctx context.Context) error { return p.stop(ctx) }

func viewSwitchFixture(t *testing.T) (*mountViewSwitch, *viewCacheProbe, mountcontrol.SwitchRequest, *[]string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	j, err := mountstate.CreateSwitchJournal(filepath.Join(dir, "switch.log"), "mount-1", "scope-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	current := newXattrFS(t)
	events := []string{}
	probe := &viewCacheProbe{}
	s := &mountViewSwitch{journal: j, adapter: &viewRequestFS{inner: current}, current: &mountRuntime{fs: current}, notifier: probe, fence: func(context.Context) error { return nil }}
	s.validate = func(context.Context, viewprotocol.Binding) error { events = append(events, "validate"); return nil }
	stop := &viewSwitchLifecycle{stop: func(context.Context) error { events = append(events, "stop"); return nil }}
	current.chain, err = fs.NewChain(append([]fs.Handler{stop}, current.chain.Handlers()...)...)
	if err != nil {
		t.Fatal(err)
	}
	s.build = func(ctx context.Context, b viewprotocol.Binding, g mountRuntimeGeneration) (*mountRuntime, error) {
		events = append(events, "build")
		f := newXattrFS(t)
		return &mountRuntime{fs: f, binding: b, generation: g}, f.StartErr()
	}
	req := mountcontrol.SwitchRequest{MountID: "mount-1", OperationID: "switch-a", ExpectedGeneration: 1, Target: viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}}
	return s, probe, req, &events
}

func TestViewSwitchCoordinatesStopBuildInstallAndOldReplay(t *testing.T) {
	s, probe, req, events := viewSwitchFixture(t)
	old := s.current
	probe.inode = func(uint64) gofuse.Status {
		r, found, err := s.journal.Lookup(req.OperationID)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if r.State != mountstate.SwitchRecovering || s.adapter.currentTarget().generation != 2 {
				t.Fatal("invalidated without decided target")
			}
			*events = append(*events, "invalidate-new")
		}
		return gofuse.OK
	}
	resp := s.apply(t.Context(), req)
	if resp.Error != "" || resp.Receipt == nil || resp.Receipt.State != mountstate.SwitchDone {
		t.Fatalf("apply: %+v", resp)
	}
	if s.current == old || s.adapter.currentTarget().fs != s.current.fs || s.pending != nil {
		t.Fatal("target not selected")
	}
	if want := []string{"validate", "stop", "validate", "build", "invalidate-new"}; !reflect.DeepEqual(*events, want) {
		t.Fatalf("order=%v", *events)
	}
	probe.inode = nil
	next := req
	next.OperationID = "switch-main"
	next.ExpectedGeneration = 2
	next.Target = viewprotocol.Binding{}
	if got := s.apply(t.Context(), next); got.Error != "" || got.Receipt.State != mountstate.SwitchDone {
		t.Fatal(got)
	}
	final := s.current
	before := len(*events)
	if got := s.apply(t.Context(), req); !reflect.DeepEqual(got, resp) {
		t.Fatalf("old replay: %+v", got)
	}
	if s.current != final || s.adapter.currentTarget().generation != 3 || len(*events) != before {
		t.Fatal("replay switched back")
	}
}

func TestViewSwitchPreDecisionFailureKeepsOldRuntime(t *testing.T) {
	for _, mode := range []string{"invalid", "mount", "generation", "qualification", "handle", "reference", "drain", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, _, req, events := viewSwitchFixture(t)
			current := s.current
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "invalid":
				req.OperationID = "bad/id"
			case "mount":
				req.MountID = "different"
			case "generation":
				req.ExpectedGeneration = 2
			case "qualification":
				s.validate = func(context.Context, viewprotocol.Binding) error { return syscall.EACCES }
			case "handle":
				var out gofuse.CreateOut
				if st := s.adapter.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0600}, "open", &out); st != gofuse.OK {
					t.Fatal(st)
				}
				defer s.adapter.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
			case "reference":
				var out gofuse.EntryOut
				if st := s.adapter.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &out); st != gofuse.OK {
					t.Fatal(st)
				}
			case "drain":
				var err error
				current.fs.chain, err = fs.NewChain(append([]fs.Handler{viewPrepareDrainFailure{}}, current.fs.chain.Handlers()...)...)
				if err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			}
			got := s.apply(ctx, req)
			if got.Error == "" || got.Receipt != nil {
				t.Fatalf("failed: %+v", got)
			}
			snapshot, err := s.journal.Snapshot()
			if err != nil || snapshot.Generation != 1 || snapshot.Pending != nil || s.current != current {
				t.Fatalf("changed: %+v %v", snapshot, err)
			}
			if leave, err := s.adapter.admission.enter(); err != nil {
				t.Fatal(err)
			} else {
				leave()
			}
			for _, event := range *events {
				if event == "stop" || event == "build" {
					t.Fatalf("side effect: %v", *events)
				}
			}
		})
	}
}

func TestViewSwitchPendingRetriesSameTargetAndQueryDoesNoWork(t *testing.T) {
	for _, mode := range []string{"stop", "build", "notify"} {
		t.Run(mode, func(t *testing.T) {
			s, probe, req, events := viewSwitchFixture(t)
			fail := true
			switch mode {
			case "stop":
				stop := s.current.fs.chain.Handlers()[0].(*viewSwitchLifecycle)
				stop.stop = func(context.Context) error {
					*events = append(*events, "stop")
					if fail {
						return errors.New("still active")
					}
					return nil
				}
			case "build":
				build := s.build
				s.build = func(ctx context.Context, b viewprotocol.Binding, g mountRuntimeGeneration) (*mountRuntime, error) {
					if fail {
						return nil, errors.New("target unavailable")
					}
					return build(ctx, b, g)
				}
			case "notify":
				probe.inode = func(uint64) gofuse.Status {
					if s.adapter.currentTarget().generation == 2 && fail {
						return gofuse.EIO
					}
					return gofuse.OK
				}
			}
			got := s.apply(t.Context(), req)
			if got.ErrorKind != "RECOVERING" || got.Receipt == nil || got.Receipt.State != mountstate.SwitchRecovering {
				t.Fatalf("pending: %+v", got)
			}
			if st := s.adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &gofuse.AttrOut{}); st != gofuse.EAGAIN {
				t.Fatal("pending admitted I/O")
			}
			before := len(*events)
			if q := s.query(req); q.Error != "" || q.Receipt.State != mountstate.SwitchRecovering || len(*events) != before {
				t.Fatalf("query: %+v", q)
			}
			changed := req
			changed.Target.WorkID = "changed"
			if q := s.apply(t.Context(), changed); q.ErrorKind != "CONFLICT" {
				t.Fatal(q)
			}
			other := req
			other.OperationID = "different"
			if q := s.apply(t.Context(), other); q.ErrorKind != "CONFLICT" {
				t.Fatal(q)
			}
			fail = false
			if got = s.apply(t.Context(), req); got.Error != "" || got.Receipt.State != mountstate.SwitchDone {
				t.Fatalf("retry: %+v", got)
			}
			builds := 0
			for _, event := range *events {
				if event == "build" {
					builds++
				}
			}
			if builds != 1 {
				t.Fatalf("rebuilt installed target: %v", *events)
			}
		})
	}
}

func TestViewSwitchWaitsForDetachedOldNotification(t *testing.T) {
	s, _, req, events := viewSwitchFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	stop := s.current.fs.chain.Handlers()[0].(*viewSwitchLifecycle)
	stopCalls := 0
	stop.stop = func(context.Context) error {
		stopCalls++
		if stopCalls == 1 {
			s.current.fs.notifications.run(func() { close(entered); <-release })
			<-entered
			cancel()
		}
		return nil
	}
	got := s.apply(ctx, req)
	if got.ErrorKind != "RECOVERING" || got.Receipt == nil {
		t.Fatalf("not retained: %+v", got)
	}
	if s.pending == nil || s.pending.oldStopped {
		t.Fatal("reported old runtime fully stopped")
	}
	for _, event := range *events {
		if event == "build" {
			t.Fatal("built while old notifier active")
		}
	}
	releaseOnce.Do(func() { close(release) })
	if got := s.apply(t.Context(), req); got.Error != "" || got.Receipt == nil || got.Receipt.State != mountstate.SwitchDone {
		t.Fatalf("notification retry: %+v", got)
	}
	if stopCalls != 2 {
		t.Fatalf("stop attempts=%d", stopCalls)
	}
}

func TestViewSwitchConcurrentSameIDIsNotDefiniteRefusal(t *testing.T) {
	s, _, req, _ := viewSwitchFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	s.validate = func(context.Context, viewprotocol.Binding) error { close(entered); <-release; return syscall.EACCES }
	done := make(chan mountcontrol.SwitchResponse, 1)
	go func() { done <- s.apply(t.Context(), req) }()
	<-entered
	got := s.apply(t.Context(), req)
	if got.ErrorKind != "UNKNOWN" || got.Receipt != nil {
		t.Fatalf("concurrent: %+v", got)
	}
	close(release)
	if got := <-done; got.ErrorKind != "TARGET_UNAVAILABLE" {
		t.Fatal(got)
	}
}

type viewSwitchStartFailure struct {
	starts, stops int
	stopErr       error
}

func (p *viewSwitchStartFailure) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return next(ctx, op)
}
func (p *viewSwitchStartFailure) Start(context.Context) error {
	p.starts++
	return errors.New("start failed")
}
func (p *viewSwitchStartFailure) Drain(context.Context) error { return nil }
func (p *viewSwitchStartFailure) Stop(context.Context) error  { p.stops++; return p.stopErr }

func TestViewSwitchRetainsFailedStartUntilCleanupThenRebuilds(t *testing.T) {
	s, _, req, _ := viewSwitchFixture(t)
	original := s.build
	failed := &viewSwitchStartFailure{stopErr: errors.New("cleanup incomplete")}
	builds := 0
	var rejected *mountRuntime
	s.build = func(ctx context.Context, b viewprotocol.Binding, g mountRuntimeGeneration) (*mountRuntime, error) {
		builds++
		if builds > 1 {
			return original(ctx, b, g)
		}
		// Use the actual constructor, including cache, writeback queue and lazy
		// journal worker; append one failing startup handler at the chain edge.
		cfg := MountConfig{Server: "http://example.invalid", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core", "writeback", "cache"}, WritePolicy: "writeback", SyncMode: "strict", WritebackLazyStaging: true, WritebackSyncWindow: time.Hour}
		cl, err := client.New(cfg.Server, "").WithView(b)
		if err != nil {
			return nil, err
		}
		rt, err := buildMountRuntimeGeneration(cfg, cl, "/", "actor", g)
		if err != nil {
			return nil, err
		}
		rt.fs.chain, err = fs.NewChain(append([]fs.Handler{failed}, rt.fs.chain.Handlers()...)...)
		rejected = rt
		return rt, err
	}
	t.Cleanup(func() {
		if rejected != nil {
			failed.stopErr = nil
			_ = rejected.stop(context.Background())
		}
	})
	if got := s.apply(t.Context(), req); got.ErrorKind != "RECOVERING" || got.Receipt == nil {
		t.Fatalf("failed start: %+v", got)
	}
	if builds != 1 || failed.starts != 1 || failed.stops != 1 || s.pending.target != rejected {
		t.Fatal("lost failed runtime")
	}
	if s.adapter.currentTarget().generation != 1 {
		t.Fatal("installed partially started target")
	}
	entry := staging.JournalEntry{Op: staging.JournalCommit, Path: "/owner-retained"}
	if err := rejected.staging.Journal.Append(entry); err != nil {
		t.Fatal("closed resources while cleanup incomplete", err)
	}
	failed.stopErr = nil
	if got := s.apply(t.Context(), req); got.ErrorKind != "RECOVERING" || s.pending.target != nil {
		t.Fatalf("cleanup retry: %+v", got)
	}
	if builds != 1 || failed.starts != 1 || failed.stops != 2 {
		t.Fatal("restarted failed target before cleanup")
	}
	if err := rejected.staging.Journal.Append(entry); err == nil {
		t.Fatal("successful cleanup left journal open")
	}
	if got := s.apply(t.Context(), req); got.Error != "" || got.Receipt.State != mountstate.SwitchDone {
		t.Fatalf("rebuild: %+v", got)
	}
	if builds != 2 {
		t.Fatalf("build attempts=%d", builds)
	}
}
