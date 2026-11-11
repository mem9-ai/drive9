//go:build linux

package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type rootFenceProbe func(uint64, string) gofuse.Status

func (p rootFenceProbe) EntryNotify(parent uint64, name string) gofuse.Status { return p(parent, name) }

func TestViewRootFenceResults(t *testing.T) {
	for _, status := range []gofuse.Status{gofuse.OK, gofuse.ENOENT, gofuse.ENOSYS, gofuse.EIO} {
		err := fenceViewRootUpdates(context.Background(), rootFenceProbe(func(parent uint64, name string) gofuse.Status {
			if parent != gofuse.FUSE_ROOT_ID || name != viewRootFenceName {
				t.Fatalf("target=%d/%s", parent, name)
			}
			return status
		}))
		if status == gofuse.OK || status == gofuse.ENOENT {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, syscall.Errno(status)) {
			t.Fatalf("status=%v err=%v", status, err)
		}
	}
	if err := fenceViewRootUpdates(context.Background(), nil); !errors.Is(err, errViewSwitchKernelUnsupported) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := fenceViewRootUpdates(ctx, rootFenceProbe(func(uint64, string) gofuse.Status { cancel(); return gofuse.OK })); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := fenceViewRootUpdates(ctx, rootFenceProbe(func(uint64, string) gofuse.Status { t.Fatal("entered kernel after cancellation"); return gofuse.OK })); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
