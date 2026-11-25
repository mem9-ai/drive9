package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

const (
	defaultLayerLeaseRenewInterval   = 20 * time.Second
	defaultLayerLeaseSelfFenceMargin = 10 * time.Second
	defaultLayerLeaseRetryInterval   = 2 * time.Second
	defaultLayerLeaseCallTimeout     = 5 * time.Second
	LayerMountLeaseTTL               = 60 * time.Second
)

// ErrLayerLeaseSelfFenced reports that the mount's exclusive Layer lease
// reached its 10s pre-expiry safety margin or was revoked by the server.
var ErrLayerLeaseSelfFenced = errors.New("fs layer mount lease self-fenced; writes halted")

type layerLeaseTiming struct {
	renewInterval   time.Duration
	selfFenceMargin time.Duration
	retryInterval   time.Duration
	callTimeout     time.Duration
}

func defaultLayerLeaseTiming() layerLeaseTiming {
	return layerLeaseTiming{
		renewInterval:   defaultLayerLeaseRenewInterval,
		selfFenceMargin: defaultLayerLeaseSelfFenceMargin,
		retryInterval:   defaultLayerLeaseRetryInterval,
		callTimeout:     defaultLayerLeaseCallTimeout,
	}
}

type layerMountBinding struct {
	BaseClient   *client.Client
	ViewClient   *client.Client
	LayerID      string
	BaseRootPath string
	RemoteRoot   string
	MountID      string
	OperationID  string
	Fence        int64
	ReadOnly     bool

	fenced           atomic.Bool
	fenceMu          sync.Mutex
	onFenceCallbacks []func()

	renewCancel context.CancelFunc
	renewDone   chan struct{}
	unboundOnce sync.Once
}

func (b *layerMountBinding) checkWriteAllowed() error {
	if b != nil && b.fenced.Load() {
		return ErrLayerLeaseSelfFenced
	}
	return nil
}

func (b *layerMountBinding) OnSelfFence(fn func()) {
	if b == nil || fn == nil {
		return
	}
	b.fenceMu.Lock()
	already := b.fenced.Load()
	if !already {
		b.onFenceCallbacks = append(b.onFenceCallbacks, fn)
	}
	b.fenceMu.Unlock()
	if already {
		fn()
	}
}

func (b *layerMountBinding) triggerSelfFence(reason string) {
	if b == nil {
		return
	}
	b.fenceMu.Lock()
	if b.fenced.Swap(true) {
		b.fenceMu.Unlock()
		return
	}
	cbs := append([]func(){}, b.onFenceCallbacks...)
	b.onFenceCallbacks = nil
	b.fenceMu.Unlock()

	_, _ = fmt.Fprintf(osStderr, "drive9: layer %s mount lease self-fenced (fence=%d): %s\n", b.LayerID, b.Fence, reason)
	for _, fn := range cbs {
		fn()
	}
}

var osStderr io.Writer = os.Stderr

func (b *layerMountBinding) startRenewLoop(initialCallStart time.Time, initialTTLMS int64, timing layerLeaseTiming) {
	if b == nil || b.ReadOnly || b.LayerID == "" || b.Fence <= 0 {
		return
	}
	if timing.renewInterval <= 0 {
		timing.renewInterval = defaultLayerLeaseRenewInterval
	}
	if timing.selfFenceMargin <= 0 {
		timing.selfFenceMargin = defaultLayerLeaseSelfFenceMargin
	}
	if timing.retryInterval <= 0 {
		timing.retryInterval = defaultLayerLeaseRetryInterval
	}
	if timing.callTimeout <= 0 {
		timing.callTimeout = defaultLayerLeaseCallTimeout
	}

	ttl := LayerMountLeaseTTL
	if initialTTLMS > 0 {
		ttl = time.Duration(initialTTLMS) * time.Millisecond
	}
	deadline := initialCallStart.Add(ttl)

	ctx, cancel := context.WithCancel(context.Background())
	b.renewCancel = cancel
	b.renewDone = make(chan struct{})
	go b.runRenewLoop(ctx, deadline, timing, b.BaseClient.RenewLayerMount)
}

func (b *layerMountBinding) runRenewLoop(ctx context.Context, deadline time.Time, timing layerLeaseTiming,
	renew func(context.Context, string, client.LayerMountRenewRequest) (*client.LayerMountLease, error),
) {
	defer close(b.renewDone)
	next := timing.renewInterval
	for {
		fenceAt := deadline.Add(-timing.selfFenceMargin)
		if !sleepContext(ctx, min(next, time.Until(fenceAt))) {
			return
		}
		callStart := time.Now()
		if !callStart.Before(fenceAt) {
			b.triggerSelfFence("mount lease entered its pre-expiry safety margin")
			return
		}
		timeout := min(timing.callTimeout, fenceAt.Sub(callStart))
		callCtx, callCancel := context.WithDeadline(ctx, callStart.Add(timeout))
		resp, err := renew(callCtx, b.LayerID, client.LayerMountRenewRequest{
			MountID: b.MountID,
			Fence:   b.Fence,
		})
		callCancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			ttl := LayerMountLeaseTTL
			if resp != nil && resp.MountLeaseTTLMS > 0 {
				ttl = time.Duration(resp.MountLeaseTTLMS) * time.Millisecond
			}
			deadline = callStart.Add(ttl)
			next = timing.renewInterval
		} else if isTerminalLayerRenewError(err) {
			b.triggerSelfFence(fmt.Sprintf("renew-mount rejected: %v", err))
			return
		} else {
			next = timing.retryInterval
		}
		if !time.Now().Before(deadline.Add(-timing.selfFenceMargin)) {
			b.triggerSelfFence("mount lease entered its pre-expiry safety margin during renewal")
			return
		}
	}
}

func (b *layerMountBinding) StopRenew() {
	if b == nil {
		return
	}
	if b.renewCancel != nil {
		b.renewCancel()
	}
	if b.renewDone != nil {
		<-b.renewDone
	}
}

func (b *layerMountBinding) Unbind(parent context.Context) {
	if b == nil {
		return
	}
	b.unboundOnce.Do(func() {
		b.StopRenew()
		if b.ReadOnly || b.LayerID == "" || b.Fence <= 0 || b.BaseClient == nil {
			return
		}
		if parent == nil {
			parent = context.Background()
		}
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), defaultLayerLeaseCallTimeout)
		defer cancel()
		_, _ = b.BaseClient.UnbindLayerMount(callCtx, b.LayerID, client.LayerMountUnbindRequest{
			MountID: b.MountID,
			Kind:    "fence",
			Fence:   b.Fence,
		})
	})
}

func isTerminalLayerRenewError(err error) bool {
	if client.IsNotFound(err) || errors.Is(err, client.ErrConflict) || client.IsUnauthorized(err) || client.IsForbidden(err) {
		return true
	}
	var st *client.StatusError
	if errors.As(err, &st) {
		switch st.StatusCode {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusNotImplemented:
			return true
		}
	}
	return false
}

func classifyLayerBindError(action string, err error) *MountExitError {
	var status *client.StatusError
	if errors.Is(err, client.ErrConflict) || (errors.As(err, &status) && status.StatusCode == http.StatusConflict) {
		return newMountExit(ExitLayerLeaseBusy, ExitReasonLayerLeaseBusy, action, err)
	}
	return ClassifyLayerStartupError(action, err)
}

// ClassifyLayerStartupError preserves the worker exit code for Layer control-plane failures.
func ClassifyLayerStartupError(action string, err error) *MountExitError {
	if isTerminalLayerRenewError(err) || errors.Is(err, mountstate.ErrSwitchCorrupt) || errors.Is(err, mountstate.ErrSwitchInvalid) {
		return ExitStartupPermanentErr(action, err)
	}
	return ExitStartupTransientErr(action, err)
}
