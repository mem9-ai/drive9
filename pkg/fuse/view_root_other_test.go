//go:build !linux

package fuse

import (
	"context"
	"errors"
	"testing"
)

func TestViewRootFenceUnsupportedKernel(t *testing.T) {
	if err := fenceViewRootUpdates(context.Background(), nil); !errors.Is(err, errViewSwitchKernelUnsupported) {
		t.Fatal(err)
	}
}
