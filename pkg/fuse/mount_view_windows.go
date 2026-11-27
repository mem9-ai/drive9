//go:build windows

package fuse

import (
	"context"
	"errors"
	"sync/atomic"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
)

type mountViewSwitch struct {
	adapter *viewRequestFS
	current *mountRuntime
}

func (s *mountViewSwitch) currentRuntime() *mountRuntime { return s.current }

type mountView struct {
	switcher   *mountViewSwitch
	ready      atomic.Bool
	server     atomic.Pointer[gofuse.Server]
	remoteRoot string
}

func prepareMountView(context.Context, MountConfig, *client.Client, string) (*mountView, error) {
	return nil, errors.New("fuse mount view unsupported on windows")
}

func (v *mountView) drainShutdownResponse(context.Context) mountcontrol.DrainResponse {
	return mountcontrol.DrainResponse{}
}

func (v *mountView) close(context.Context) error { return nil }

func (v *mountView) bindProcessStateUpdater(func(string, bool)) {}

func startMountViewControlServer(string, *mountViewSwitch) (*mountControlServer, error) {
	return nil, nil
}
