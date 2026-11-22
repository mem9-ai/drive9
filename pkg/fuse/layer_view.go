package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

const (
	defaultLayerLeaseRenewInterval   = 20 * time.Second
	defaultLayerLeaseSelfFenceMargin = 10 * time.Second
	defaultLayerLeaseRetryInterval   = 2 * time.Second
	defaultLayerLeaseCallTimeout     = 5 * time.Second
	defaultLayerLeaseFallbackTTL     = 60 * time.Second
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

	fenced        atomic.Bool
	fenceMu       sync.Mutex
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

func resolveLayerMountView(
	ctx context.Context,
	baseClient *client.Client,
	layerRef string,
	rawRemoteRoot string,
	remoteRootDefaulted bool,
	mountID string,
	readOnly bool,
	timing layerLeaseTiming,
) (*layerMountBinding, error) {
	ref := strings.TrimSpace(layerRef)
	if ref == "" {
		rootInput := rawRemoteRoot
		if strings.TrimSpace(rootInput) == "" {
			rootInput = "/"
		}
		remoteRoot, err := mountpath.NormalizeRoot(rootInput)
		if err != nil {
			return nil, fmt.Errorf("fuse: normalize remote root: %w", err)
		}
		return &layerMountBinding{
			BaseClient: baseClient,
			ViewClient: baseClient,
			RemoteRoot: remoteRoot,
			MountID:    mountID,
			ReadOnly:   readOnly,
		}, nil
	}

	info, err := baseClient.GetLayer(ctx, ref)
	if err != nil {
		return nil, classifyLayerStartupError(fmt.Sprintf("resolve layer %q", ref), err)
	}
	baseRoot, err := mountpath.NormalizeRoot(info.BaseRootPath)
	if err != nil {
		return nil, ExitStartupPermanentErr(fmt.Sprintf("invalid layer %s base_root_path %q", info.LayerID, info.BaseRootPath), err)
	}

	var remoteRoot string
	if strings.TrimSpace(rawRemoteRoot) == "" || remoteRootDefaulted {
		remoteRoot = baseRoot
	} else {
		remoteRoot, err = mountpath.NormalizeRoot(rawRemoteRoot)
		if err != nil {
			return nil, fmt.Errorf("fuse: normalize remote root: %w", err)
		}
		if !viewprotocol.PathWithinBaseRoot(baseRoot, remoteRoot) {
			return nil, ExitStartupPermanentErr(
				fmt.Sprintf("remote root %q is outside layer %s base_root_path %q", remoteRoot, info.LayerID, baseRoot),
				nil,
			)
		}
	}

	if readOnly {
		vc, verr := baseClient.WithView(client.ViewBinding{LayerID: info.LayerID, Fence: 0})
		if verr != nil {
			return nil, ExitStartupPermanentErr(fmt.Sprintf("bind read-only layer view %s", info.LayerID), verr)
		}
		return &layerMountBinding{
			BaseClient:   baseClient,
			ViewClient:   vc,
			LayerID:      info.LayerID,
			BaseRootPath: baseRoot,
			RemoteRoot:   remoteRoot,
			MountID:      mountID,
			Fence:        0,
			ReadOnly:     true,
		}, nil
	}

	expectedFence := info.Fence
	if expectedFence%2 == 1 && info.MountExpired {
		unb, uerr := baseClient.UnbindLayerMount(ctx, info.LayerID, client.LayerMountUnbindRequest{
			MountID: mountID,
			Kind:    "expire",
		})
		if uerr != nil {
			return nil, classifyLayerStartupError(fmt.Sprintf("expire stale mount lease on layer %s", info.LayerID), uerr)
		}
		if unb != nil {
			expectedFence = unb.Fence
		}
	}
	if expectedFence < 0 || expectedFence%2 != 0 {
		expectedFence = 0
	}

	opID := "bind_" + generateMountID()
	bindStart := time.Now()
	lease, err := baseClient.BindLayerMount(ctx, info.LayerID, client.LayerMountBindRequest{
		MountID:       mountID,
		OperationID:   opID,
		ExpectedFence: expectedFence,
	})
	if err != nil {
		return nil, classifyLayerStartupError(fmt.Sprintf("bind exclusive mount lease on layer %s", info.LayerID), err)
	}

	binding := &layerMountBinding{
		BaseClient:   baseClient,
		LayerID:      info.LayerID,
		BaseRootPath: baseRoot,
		RemoteRoot:   remoteRoot,
		MountID:      mountID,
		OperationID:  opID,
		Fence:        lease.Fence,
		ReadOnly:     false,
	}
	vc, verr := baseClient.WithView(client.ViewBinding{
		LayerID:    info.LayerID,
		Fence:      lease.Fence,
		WriteGuard: binding.checkWriteAllowed,
	})
	if verr != nil {
		binding.Unbind(context.Background())
		return nil, ExitStartupPermanentErr(fmt.Sprintf("bind writable layer view %s", info.LayerID), verr)
	}
	binding.ViewClient = vc
	binding.startRenewLoop(bindStart, lease.MountLeaseTTLMS, timing)
	return binding, nil
}

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

	ttl := defaultLayerLeaseFallbackTTL
	if initialTTLMS > 0 {
		ttl = time.Duration(initialTTLMS) * time.Millisecond
	}
	deadline := initialCallStart.Add(ttl)

	ctx, cancel := context.WithCancel(context.Background())
	b.renewCancel = cancel
	b.renewDone = make(chan struct{})
	go b.runRenewLoop(ctx, deadline, timing)
}

func (b *layerMountBinding) runRenewLoop(ctx context.Context, deadline time.Time, timing layerLeaseTiming) {
	defer close(b.renewDone)
	for {
		remaining := time.Until(deadline)
		if remaining <= timing.selfFenceMargin {
			b.triggerSelfFence("mount_lease_ttl_ms entered 10s pre-expiry safety margin")
			return
		}
		wait := timing.renewInterval
		if untilMargin := remaining - timing.selfFenceMargin; untilMargin < wait {
			wait = untilMargin
		}
		if !sleepContext(ctx, wait) {
			return
		}
		if time.Until(deadline) <= timing.selfFenceMargin {
			b.triggerSelfFence("mount_lease_ttl_ms entered 10s pre-expiry safety margin")
			return
		}

		callStart := time.Now()
		callCtx, callCancel := context.WithTimeout(ctx, timing.callTimeout)
		resp, err := b.BaseClient.RenewLayerMount(callCtx, b.LayerID, client.LayerMountRenewRequest{
			MountID: b.MountID,
			Fence:   b.Fence,
		})
		callCancel()
		if err == nil {
			ttl := defaultLayerLeaseFallbackTTL
			if resp != nil && resp.MountLeaseTTLMS > 0 {
				ttl = time.Duration(resp.MountLeaseTTLMS) * time.Millisecond
			}
			deadline = callStart.Add(ttl)
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if isTerminalLayerRenewError(err) {
			b.triggerSelfFence(fmt.Sprintf("renew-mount rejected: %v", err))
			return
		}
		remaining = time.Until(deadline)
		if remaining <= timing.selfFenceMargin {
			b.triggerSelfFence(fmt.Sprintf("renew-mount failed before safety margin: %v", err))
			return
		}
		retryWait := timing.retryInterval
		if untilMargin := remaining - timing.selfFenceMargin; untilMargin < retryWait {
			retryWait = untilMargin
		}
		if !sleepContext(ctx, retryWait) {
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

func classifyLayerStartupError(action string, err error) *MountExitError {
	if isTerminalLayerRenewError(err) {
		return ExitStartupPermanentErr(action, err)
	}
	return ExitStartupTransientErr(action, err)
}
