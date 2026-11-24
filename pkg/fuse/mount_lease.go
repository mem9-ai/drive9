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
	// mountLeaseRetryMax caps the re-registration backoff applied to a rejected
	// credential. It is deliberately above mountLeaseTTL: the mount keeps
	// serving without a lease while it waits, so a long cap here is a real cost,
	// and only the 401/403 class grows to it.
	mountLeaseRetryMax = 5 * time.Minute
	mountLeaseTimeout  = 5 * time.Second
)

// mountLeaseFailure classifies one lease call result so both call sites use the
// same vocabulary instead of two hand-rolled status checks. The class is only a
// name for the failure: run() decides what it means at each call site, and the
// same class can be terminal on one path and recoverable on the other.
type mountLeaseFailure int

const (
	leaseFailureNone mountLeaseFailure = iota
	// leaseFailureCancelled means the reporter is shutting down.
	leaseFailureCancelled
	// leaseFailureUnsupported means the server does not implement the call
	// (405/501). Registration treats it as "no lease route" and stops quietly;
	// a heartbeat logs once before stopping, because a live lease is being
	// dropped rather than never acquired.
	leaseFailureUnsupported
	// leaseFailureGone means the lease row no longer exists (404). A heartbeat
	// re-registers immediately at the base interval; on registration the same
	// status means the route is absent, so it is terminal like unsupported.
	leaseFailureGone
	// leaseFailureRejected means the server rejected this mount identity or key
	// (401/403). Both call sites re-register (a heartbeat immediately, a failed
	// registration after a growing delay): the same key is refused until its
	// server-side state changes (key re-enabled, tenant restored, scope widened).
	leaseFailureRejected
	// leaseFailureTransient is every other error (dial error, timeout, 5xx, 429).
	// Registration retries at the base interval; a heartbeat retries at the next
	// heartbeat interval, which is the lease's normal cadence anyway.
	leaseFailureTransient
)

// classifyMountLeaseFailure names a lease call result. It is the only place that
// maps status codes and transport errors to a class; what the caller does with
// the class (stop, re-register, retry) is decided at that call site.
func classifyMountLeaseFailure(ctx context.Context, err error) mountLeaseFailure {
	switch {
	case ctx.Err() != nil:
		return leaseFailureCancelled
	case err == nil:
		return leaseFailureNone
	case isStatus(err, 405) || isStatus(err, 501):
		return leaseFailureUnsupported
	case client.IsNotFound(err):
		return leaseFailureGone
	case client.IsUnauthorized(err) || client.IsForbidden(err):
		return leaseFailureRejected
	default:
		return leaseFailureTransient
	}
}

// mountLeaseReporter keeps the server-side mount lease alive while this FUSE
// process serves. Registration is best-effort: an older server without the
// endpoint must not make mounting fail, but a live server gets a lease that the
// migration controller can trust.
type mountLeaseReporter struct {
	cl     *client.Client
	req    client.MountLeaseRequest
	cancel context.CancelFunc
	done   chan struct{}

	// sleep and logf are seams for tests; production uses sleepContext and
	// stderr. Both are set by startMountLeaseWith.
	sleep func(context.Context, time.Duration) bool
	logf  func(format string, args ...any)
}

func startMountLease(parent context.Context, cl *client.Client, mountID, mountPoint string, readOnly bool) *mountLeaseReporter {
	mode := "read_write"
	if readOnly {
		mode = "read_only"
	}
	host, _ := os.Hostname()
	return startMountLeaseWith(parent, cl, client.MountLeaseRequest{
		MountID:    mountID,
		MountPoint: mountPoint,
		Host:       host,
		PID:        int64(os.Getpid()),
		Mode:       mode,
		TTLSeconds: int(mountLeaseTTL / time.Second),
	}, sleepContext, stderrLogf)
}

// stderrLogf is the production log seam. Tests inject their own logf, so the
// function is the only place that writes the reporter's lines.
func stderrLogf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func startMountLeaseWith(parent context.Context, cl *client.Client, req client.MountLeaseRequest, sleep func(context.Context, time.Duration) bool, logf func(string, ...any)) *mountLeaseReporter {
	if cl == nil || req.MountID == "" || req.MountPoint == "" {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	l := &mountLeaseReporter{
		cl:     cl,
		req:    req,
		cancel: cancel,
		done:   make(chan struct{}),
		sleep:  sleep,
		logf:   logf,
	}
	go l.run(ctx)
	return l
}

func (l *mountLeaseReporter) run(ctx context.Context) {
	defer close(l.done)
	registered := false
	reported := leaseFailureNone
	reportFailure := func(class mountLeaseFailure, msg string, err error) {
		// One line per distinct failure class: a long episode stays quiet, but a
		// class change (401 -> dial error -> 403) is not swallowed.
		if reported == class {
			return
		}
		reported = class
		l.logf(msg, err)
	}
	heartbeatInterval := mountLeaseHeartbeat
	retryDelay := mountLeaseRetry
	for {
		if !registered {
			callCtx, cancel := context.WithTimeout(ctx, mountLeaseTimeout)
			lease, err := l.cl.RegisterMountLease(callCtx, l.req)
			cancel()
			if err != nil {
				class := classifyMountLeaseFailure(ctx, err)
				switch class {
				case leaseFailureCancelled:
					return
				case leaseFailureUnsupported, leaseFailureGone:
					// An older server without the route (404 on registration means
					// the endpoint is absent, not that a lease expired): never
					// affect mounting.
					return
				case leaseFailureRejected:
					reportFailure(class, "drive9: mount lease registration rejected (%v); retrying with backoff\n", err)
					delay := retryDelay
					retryDelay = nextMountLeaseRetry(retryDelay)
					if !l.sleep(ctx, delay) {
						return
					}
					continue
				default:
					// A dial error, timeout or 5xx is not a verdict on the key:
					// retry at the base interval so a recovered server is picked
					// up within one retry, not at the end of a backoff schedule.
					reportFailure(class, "drive9: mount lease registration unavailable: %v\n", err)
					if !l.sleep(ctx, mountLeaseRetry) {
						return
					}
					continue
				}
			}
			registered = true
			// A successful registration does not prove the lease is healthy yet
			// (the first heartbeat may still be rejected), so the reported class
			// is only cleared by a successful heartbeat. That keeps one line per
			// episode instead of one per heartbeat interval.
			retryDelay = mountLeaseRetry
			if lease != nil && lease.HeartbeatIntervalSeconds > 0 {
				heartbeatInterval = time.Duration(lease.HeartbeatIntervalSeconds) * time.Second
			}
		}
		if !l.sleep(ctx, heartbeatInterval) {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, mountLeaseTimeout)
		_, err := l.cl.HeartbeatMountLease(callCtx, l.req.MountID, l.req.TTLSeconds)
		cancel()
		class := classifyMountLeaseFailure(ctx, err)
		switch class {
		case leaseFailureNone:
			if reported != leaseFailureNone {
				l.logf("drive9: mount lease heartbeat recovered\n")
				reported = leaseFailureNone
			}
			continue
		case leaseFailureCancelled:
			return
		case leaseFailureGone:
			// The lease expired (server restart or a missed heartbeat window).
			// Re-register on the next loop instead of extending a missing row.
			registered = false
			continue
		case leaseFailureRejected:
			// The server refuses this mount identity or key. Exiting here used to
			// lose the lease silently: `mount health` only reads local state, so
			// nothing surfaced the loss while the migration controller could move
			// the region out from under a live mount. Re-register instead: the
			// same key recovers when its server-side state does (re-enabled key,
			// restored tenant, widened scope); the backoff belongs to that
			// re-registration, so a still-refused key costs one attempt per five
			// minutes. A rotated credential is NOT picked up - the client's key
			// is fixed at mount time - so the message says "rejected", not
			// "recovered".
			reportFailure(class, "drive9: mount lease heartbeat rejected (%v); re-registering\n", err)
			registered = false
			continue
		case leaseFailureUnsupported:
			// A live lease that the server no longer knows how to keep: say so
			// once instead of dropping it silently, then stop retrying.
			reportFailure(class, "drive9: mount lease heartbeat unsupported (%v); no longer maintaining the lease\n", err)
			return
		default:
			reportFailure(class, "drive9: mount lease heartbeat failed: %v\n", err)
		}
	}
}

// nextMountLeaseRetry doubles the re-registration delay up to the cap so a
// rejected credential costs one attempt per five minutes.
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
