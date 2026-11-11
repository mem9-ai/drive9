//go:build !windows

package fuse

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

// mountView owns the journal and all runtimes of one mount connection. ready
// stays false until the kernel mount is serving. A crashed pending decision is
// reported, never automatically completed or replaced with main.
type mountView struct {
	switcher *mountViewSwitch
	ready    atomic.Bool
	server   atomic.Pointer[gofuse.Server]
}

func prepareMountView(ctx context.Context, cfg MountConfig, cl *client.Client, actorID string) (_ *mountView, err error) {
	if cfg.LayerRef != "" || cfg.CheckpointRef != "" {
		return nil, errors.New("fixed mount views cannot use the legacy layer extension")
	}
	if cfg.Supervised && cfg.MountID == "" {
		return nil, ExitStartupPermanentErr("supervised mount requires an existing mount-view-id", nil)
	}
	root, err := mountpath.NormalizeRoot(cfg.RemoteRoot)
	if err != nil {
		return nil, err
	}
	var journal *mountstate.SwitchJournal
	if cfg.MountID == "" {
		journal, err = createMountViewJournal(ctx, cfg, cl)
	} else {
		journal, err = openMountViewJournal(ctx, cfg, cl)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = journal.Close()
		}
	}()
	snapshot, err := journal.Snapshot()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("mount %s selected layer=%q work=%q fence=%d generation=%d: %w", snapshot.MountID, snapshot.Binding.LayerID, snapshot.Binding.WorkID, snapshot.Binding.Fence, snapshot.Generation, err)
		}
	}()
	if snapshot.Pending != nil {
		return nil, ExitStartupPermanentErr(fmt.Sprintf("operation %s remains RECOVERING; original journal retained", snapshot.Pending.OperationID), mountstate.ErrSwitchPending)
	}
	bound, err := cl.WithView(snapshot.Binding)
	if err != nil {
		return nil, err
	}
	if err := validateRemoteRoot(ctx, bound, root); err != nil {
		return nil, err
	}
	runtime, err := buildMountRuntimeGeneration(cfg, bound, root, actorID, mountRuntimeGeneration{MountID: snapshot.MountID, Generation: snapshot.Generation})
	if err != nil {
		return nil, err
	}
	adapter := &viewRequestFS{inner: runtime.fs}
	adapter.selected.Store(&viewRequestTarget{fs: runtime.fs, generation: snapshot.Generation})
	v := &mountView{switcher: &mountViewSwitch{journal: journal, current: runtime, adapter: adapter}}
	v.switcher.notifier = v
	v.switcher.fence = func(ctx context.Context) error { return fenceViewRootUpdates(ctx, v) }
	v.switcher.validate = func(ctx context.Context, binding viewprotocol.Binding) error {
		if !v.ready.Load() || v.server.Load() == nil {
			return errors.New("mount connection is not ready for switching")
		}
		target, err := cl.WithView(binding)
		if err != nil {
			return err
		}
		return validateRemoteRoot(ctx, target, root)
	}
	v.switcher.build = func(ctx context.Context, binding viewprotocol.Binding, generation mountRuntimeGeneration) (*mountRuntime, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		server := v.server.Load()
		if !v.ready.Load() || server == nil {
			return nil, errors.New("mount connection ended")
		}
		target, err := cl.WithView(binding)
		if err != nil {
			return nil, err
		}
		next, err := buildMountRuntimeWithCounters(cfg, target, root, actorID, generation, runtime.fs.perf)
		if err != nil {
			return nil, err
		}
		// Do not invoke Init: the coordinator owns the one Start attempt and
		// failed-Start cleanup. Notifications use this same kernel connection.
		next.fs.mu.Lock()
		next.fs.server = server
		next.fs.mu.Unlock()
		return next, nil
	}
	return v, nil
}

func (v *mountView) InodeNotify(node uint64, off, length int64) gofuse.Status {
	if server := v.server.Load(); server != nil {
		return server.InodeNotify(node, off, length)
	}
	return gofuse.ENODEV
}

func (v *mountView) EntryNotify(parent uint64, name string) gofuse.Status {
	if server := v.server.Load(); server != nil {
		return server.EntryNotify(parent, name)
	}
	return gofuse.ENODEV
}

func (v *mountView) currentRuntime() *mountRuntime {
	v.switcher.mu.Lock()
	defer v.switcher.mu.Unlock()
	return v.switcher.current
}

// drainShutdown is called after closing/joining the control service. Keep
// ordinary admission closed through unmount, including if drain fails.
func (v *mountView) drainShutdown(ctx context.Context) error {
	v.ready.Store(false)
	v.switcher.mu.Lock()
	defer v.switcher.mu.Unlock()
	if v.switcher.pending != nil {
		return mountstate.ErrSwitchPending
	}
	pause, err := v.switcher.adapter.admission.pause()
	if err != nil {
		return err
	}
	if err := pause.wait(ctx); err != nil {
		return err
	}
	result := v.switcher.current.fs.Drain(ctx)
	if !result.OK {
		return fmt.Errorf("mount shutdown drain (%s): %s", result.ErrorKind, result.Error)
	}
	return nil
}

// close follows control-service join and the kernel serve loop's end (or a
// startup failure before serving). Stop both owned runtimes on partial switch.
func (v *mountView) close(ctx context.Context) error {
	v.ready.Store(false)
	v.switcher.mu.Lock()
	defer v.switcher.mu.Unlock()
	err := v.switcher.current.stop(ctx)
	if p := v.switcher.pending; p != nil && p.target != nil && p.target != v.switcher.current {
		err = errors.Join(err, p.target.stop(ctx))
	}
	if err != nil {
		return err // Retain journal ownership until the caller completes cleanup.
	}
	return v.switcher.journal.Close()
}
