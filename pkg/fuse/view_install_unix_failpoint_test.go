//go:build !windows && failpoint

package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestViewInstallationCancellationDuringJournalCompletion(t *testing.T) {
	for _, syncFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable-success", true: "uncertain-sync"}[syncFailure], func(t *testing.T) {
			i, probe, path := viewInstallFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			const point = "github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeSync"
			calls := 0
			if err := failpoint.EnableCall(point, func(syncErr *error) {
				calls++
				if i.adapter.currentTarget().generation != 2 || len(probe.calls) != 3 {
					t.Fatal("completion started before installation and notifications")
				}
				select {
				case <-i.pause.released:
					t.Fatal("admission opened before durability")
				default:
				}
				cancel()
				if syncFailure {
					*syncErr = syscall.EIO
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			r, err := i.finish(ctx)
			if calls != 1 || !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("completion boundary not exercised: calls=%d ctx=%v", calls, ctx.Err())
			}
			if syncFailure {
				if r != nil || !errors.Is(err, mountstate.ErrSwitchUncertain) || !errors.Is(err, syscall.EIO) {
					t.Fatalf("uncertain sync: %+v %v", r, err)
				}
				if i.completed != nil {
					t.Fatal("uncertain sync cached DONE")
				}
				if st := i.adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &gofuse.AttrOut{}); st != gofuse.EAGAIN {
					t.Fatal("uncertain sync reopened admission")
				}
				// Retry cannot turn a failed journal into a known success.
				if retry, err := i.finish(t.Context()); retry != nil || !errors.Is(err, mountstate.ErrSwitchUncertain) {
					t.Fatalf("uncertain retry: %+v %v", retry, err)
				}
			} else {
				if err != nil || r == nil || r.State != mountstate.SwitchDone {
					t.Fatalf("durable completion lost to cancellation: %+v %v", r, err)
				}
				var out gofuse.AttrOut
				if st := i.adapter.GetAttr(nil, &gofuse.GetAttrIn{}, &out); st != gofuse.OK || out.Mode != 0755 {
					t.Fatalf("completed view paused: %v %+v", st, out)
				}
				// The same canceled request still observes its original receipt.
				if retry, err := i.finish(ctx); err != nil || retry == nil || *retry != *r || calls != 1 {
					t.Fatalf("successful replay changed result: %+v %v", retry, err)
				}
			}
			if err := failpoint.Disable(point); err != nil {
				t.Fatal(err)
			}
			if err := i.journal.Close(); err != nil {
				t.Fatal(err)
			}
			// The uncertain write may have reached the file. Recovery syncs it
			// again and reports the original target; it never invents main.
			journal, err := mountstate.OpenSwitchJournal(path, "mount-1", "scope-1", viewprotocol.Binding{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.Close() }()
			recovered, found, err := journal.Lookup("switch-a")
			if err != nil || !found || recovered.State != mountstate.SwitchDone || recovered.Target.LayerID != "a" || recovered.Generation != 2 {
				t.Fatalf("recovery: %+v %v %v", recovered, found, err)
			}
		})
	}
}
