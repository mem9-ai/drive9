package fuse

import (
	"errors"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

var errViewSwitchKernelUnsupported = errors.New("mount view switching kernel barrier unavailable")

type viewRootNotifier interface {
	EntryNotify(parent uint64, name string) gofuse.Status
}
