package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	mountLeaseTTL       = 60 * time.Second
	mountLeaseHeartbeat = 20 * time.Second
	mountLeaseRetry     = 5 * time.Second
	// mountLeaseRetryMax caps the re-registration backoff. A lease that the
	// server keeps rejecting must not spin at the fixed retry interval forever,
	// but it must also not give up: the credential can be renewed server-side
	// (or the policy widened) while the mount is still serving.
	mountLeaseRetryMax = 5 * time.Minute
	mountLeaseTimeout  = 5 * time.Second
)

// mountLeaseReporter keeps the server-side mount lease alive while this FUSE
// process serves. Registration is best-effort: an older server without the
// endpoint must not make mounting fail, but a live server gets a lease that the
// migration controller can trust.
type mountLeaseReporter struct {
	cl     *client.Client
	req    client.MountLeaseRequest
	cancel context.CancelFunc
	done   chan struct{}
}

func startMountLease(parent context.Context, cl *client.Client, mountID, mountPoint string, readOnly bool) *mountLeaseReporter {
	if cl == nil || mountID == "" || mountPoint == "" {
		return nil
	}
	mode := "read_write"
	if readOnly {
		mode = "read_only"
	}
	host, _ := os.Hostname()
	ctx, cancel := context.WithCancel(parent)
	l := &mountLeaseReporter{
		cl: cl,
		req: client.MountLeaseRequest{
			MountID:    mountID,
			MountPoint: mountPoint,
			Host:       host,
			PID:        int64(os.Getpid()),
			Mode:       mode,
			TTLSeconds: int(mountLeaseTTL / time.Second),
		},
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go l.run(ctx)
	return l
}

func (l *mountLeaseReporter) run(ctx context.Context) {
	defer close(l.done)
	registered := false
	reportedFailure := false
	heartbeatInterval := mountLeaseHeartbeat
	retryDelay := mountLeaseRetry
	for {
		if !registered {
			callCtx, cancel := context.WithTimeout(ctx, mountLeaseTimeout)
			lease, err := l.cl.RegisterMountLease(callCtx, l.req)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if mountLeaseUnsupported(err) {
					return
				}
				if !reportedFailure {
					fmt.Fprintf(os.Stderr, "drive9: mount lease registration unavailable: %v\n", err)
					reportedFailure = true
				}
				if !sleepContext(ctx, retryDelay) {
					return
				}
				retryDelay = nextMountLeaseRetry(retryDelay)
				continue
			}
			registered = true
			reportedFailure = false
			retryDelay = mountLeaseRetry
			if lease != nil && lease.HeartbeatIntervalSeconds > 0 {
				heartbeatInterval = time.Duration(lease.HeartbeatIntervalSeconds) * time.Second
			}
		}
		if !sleepContext(ctx, heartbeatInterval) {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, mountLeaseTimeout)
		_, err := l.cl.HeartbeatMountLease(callCtx, l.req.MountID, l.req.TTLSeconds)
		cancel()
		if err == nil {
			reportedFailure = false
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if client.IsNotFound(err) || isStatus(err, 404) {
			// The lease expired (server restart or a missed heartbeat window).
			// Re-register on the next loop instead of extending a missing row.
			registered = false
			continue
		}
		if isStatus(err, 401) || isStatus(err, 403) {
			// The credential was rejected: it expired or was rotated server-side,
			// or the server no longer accepts this mount identity. Exiting here
			// used to lose the lease silently: `mount health` only reads local
			// state, so nothing surfaced the loss while the migration controller
			// happily moved the region out from under a live mount. Report it and
			// go back through registration with backoff so a server-side change is
			// picked up instead of parking the mount without a lease forever.
			if !reportedFailure {
				fmt.Fprintf(os.Stderr, "drive9: mount lease rejected (%v); re-registering with backoff\n", err)
				reportedFailure = true
			}
			registered = false
			continue
		}
		if mountLeaseUnsupported(err) {
			return
		}
		if !reportedFailure {
			fmt.Fprintf(os.Stderr, "drive9: mount lease heartbeat failed: %v\n", err)
			reportedFailure = true
		}
	}
}

// nextMountLeaseRetry doubles the re-registration delay up to the cap so a
// permanently rejected credential costs one attempt per five minutes.
func nextMountLeaseRetry(current time.Duration) time.Duration {
	next := current * 2
	if next > mountLeaseRetryMax {
		return mountLeaseRetryMax
	}
	return next
}

// Close releases the lease. It is safe to call on a nil reporter and more than
// once; the lease is deleted only after the heartbeat goroutine has stopped.
func (l *mountLeaseReporter) Close() {
	if l == nil {
		return
	}
	l.cancel()
	<-l.done
	ctx, cancel := context.WithTimeout(context.Background(), mountLeaseTimeout)
	defer cancel()
	_ = l.cl.DeleteMountLease(ctx, l.req.MountID)
}

func mountLeaseUnsupported(err error) bool {
	return isStatus(err, 404) || isStatus(err, 405) || isStatus(err, 501)
}

func isStatus(err error, code int) bool {
	var statusErr *client.StatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == code
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
