package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// classifyMountLeaseFailure is the single terminal set for both call sites, so
// the classes can be table-tested without a server or a sleep.
func TestClassifyMountLeaseFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want mountLeaseFailure
	}{
		{name: "success", ctx: context.Background(), err: nil, want: leaseFailureNone},
		{name: "cancelled", ctx: cancelledContext(), err: errors.New("boom"), want: leaseFailureCancelled},
		{name: "gone", ctx: context.Background(), err: statusErr(http.StatusNotFound), want: leaseFailureGone},
		{name: "unauthorized", ctx: context.Background(), err: statusErr(http.StatusUnauthorized), want: leaseFailureRejected},
		{name: "forbidden", ctx: context.Background(), err: statusErr(http.StatusForbidden), want: leaseFailureRejected},
		{name: "method not allowed", ctx: context.Background(), err: statusErr(http.StatusMethodNotAllowed), want: leaseFailureUnsupported},
		{name: "not implemented", ctx: context.Background(), err: statusErr(http.StatusNotImplemented), want: leaseFailureUnsupported},
		{name: "server error", ctx: context.Background(), err: statusErr(http.StatusBadGateway), want: leaseFailureTransient},
		{name: "throttled", ctx: context.Background(), err: statusErr(http.StatusTooManyRequests), want: leaseFailureTransient},
		{name: "dial error", ctx: context.Background(), err: errors.New("connection refused"), want: leaseFailureTransient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyMountLeaseFailure(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("class = %v, want %v", got, tc.want)
			}
		})
	}
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func statusErr(code int) error {
	return &client.StatusError{StatusCode: code, Message: fmt.Sprintf("status %d", code)}
}

// leaseHarness drives run() with injected sleep and log so the state machine is
// asserted directly instead of through wall-clock waits.
type leaseHarness struct {
	mu     sync.Mutex
	sleeps []time.Duration
	logs   []string
}

func (h *leaseHarness) sleep(_ context.Context, d time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sleeps = append(h.sleeps, d)
	return true
}

func (h *leaseHarness) logf(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, fmt.Sprintf(format, args...))
}

func (h *leaseHarness) snapshot() ([]time.Duration, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Duration(nil), h.sleeps...), append([]string(nil), h.logs...)
}

func (h *leaseHarness) sleepCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sleeps)
}

// leaseSequence is the fake mount-lease API. Once a list is exhausted the calls
// are accepted; heartbeatRepeat keeps returning the last heartbeat error so a
// failure episode stays open.
type leaseSequence struct {
	register        []error
	heartbeat       []error
	heartbeatRepeat bool
}

// leaseCalls counts the lease API calls the fake served, so tests can assert a
// re-registration happened instead of inferring it from the sleep pattern.
type leaseCalls struct {
	mu         sync.Mutex
	registers  int
	heartbeats int
}

func (c *leaseCalls) registerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.registers
}

func (c *leaseCalls) heartbeatCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.heartbeats
}

func newLeaseTestServer(t *testing.T, seq leaseSequence) (*httptest.Server, *leaseCalls) {
	t.Helper()
	var mu sync.Mutex
	regIdx, hbIdx := 0, 0
	calls := &leaseCalls{}
	next := func(list []error, idx *int) error {
		mu.Lock()
		defer mu.Unlock()
		if len(list) == 0 {
			return nil
		}
		if *idx >= len(list) {
			if seq.heartbeatRepeat {
				return list[len(list)-1]
			}
			return nil
		}
		err := list[*idx]
		*idx++
		return err
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var err error
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/mounts":
			calls.mu.Lock()
			calls.registers++
			calls.mu.Unlock()
			err = next(seq.register, &regIdx)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/heartbeat"):
			calls.mu.Lock()
			calls.heartbeats++
			calls.mu.Unlock()
			err = next(seq.heartbeat, &hbIdx)
		}
		if err != nil {
			var status *client.StatusError
			if errors.As(err, &status) {
				w.WriteHeader(status.StatusCode)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": status.Message})
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/v1/mounts" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": "t1", "mount_id": "m1", "mount_point": "/mnt/drive9", "mode": "read_write",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": "t1", "mount_id": "m1", "mode": "read_write"})
	}))
	return server, calls
}

func startHarness(t *testing.T, h *leaseHarness, seq leaseSequence) (*mountLeaseReporter, *leaseCalls) {
	t.Helper()
	ts, calls := newLeaseTestServer(t, seq)
	t.Cleanup(ts.Close)
	reporter := startMountLeaseWith(context.Background(), client.New(ts.URL, "key"), client.MountLeaseRequest{
		MountID:    "m1",
		MountPoint: "/mnt/drive9",
		Mode:       "read_write",
		TTLSeconds: 60,
	}, h.sleep, h.logf)
	if reporter == nil {
		t.Fatal("reporter is nil")
	}
	return reporter, calls
}

// A rejected heartbeat must re-register, stay alive, and print one line for the
// episode instead of one per heartbeat.
func TestMountLeaseRejectedHeartbeatReRegistersAndReportsOnce(t *testing.T) {
	h := &leaseHarness{}
	reporter, calls := startHarness(t, h, leaseSequence{heartbeat: []error{statusErr(http.StatusUnauthorized)}, heartbeatRepeat: true})
	defer reporter.Close()

	waitFor(t, 2*time.Second, func() bool { return h.sleepCount() >= 6 })
	select {
	case <-reporter.done:
		t.Fatal("reporter exited after a rejected heartbeat")
	default:
	}
	if regs := calls.registerCount(); regs < 2 {
		t.Fatalf("registrations = %d, want a re-registration after the rejected heartbeat", regs)
	}
	_, logs := h.snapshot()
	if len(logs) != 1 || !strings.Contains(logs[0], "heartbeat rejected") {
		t.Fatalf("logs = %#v, want exactly one rejected-heartbeat line", logs)
	}
}

// A failure episode that changes class (401 -> 5xx -> 401 -> recovered) logs one
// line per change and a recovery line, so an operator can read the episode from
// the log without a per-heartbeat flood.
func TestMountLeaseFailureClassChangeLogsEachTransition(t *testing.T) {
	h := &leaseHarness{}
	reporter, calls := startHarness(t, h, leaseSequence{
		register: []error{
			statusErr(http.StatusUnauthorized),
			statusErr(http.StatusBadGateway),
			statusErr(http.StatusUnauthorized),
			nil,
		},
	})
	defer reporter.Close()

	waitFor(t, 3*time.Second, func() bool { return h.sleepCount() >= 4 })
	if regs := calls.registerCount(); regs < 4 {
		t.Fatalf("registrations = %d, want the four scripted attempts", regs)
	}
	_, logs := h.snapshot()
	want := []string{"registration rejected", "registration unavailable", "registration rejected", "heartbeat recovered"}
	if len(logs) < len(want) {
		t.Fatalf("logs = %#v, want at least %d lines", logs, len(want))
	}
	for i, fragment := range want {
		if !strings.Contains(logs[i], fragment) {
			t.Fatalf("logs[%d] = %q, want %q", i, logs[i], fragment)
		}
	}
	if len(logs) != len(want) {
		t.Fatalf("logs = %#v, want exactly one line per transition", logs)
	}
}

// The backoff belongs to a rejected credential only; the delay grows by class.
func TestMountLeaseRejectedRegistrationBacksOff(t *testing.T) {
	h := &leaseHarness{}
	reporter, calls := startHarness(t, h, leaseSequence{
		register: []error{statusErr(http.StatusUnauthorized), statusErr(http.StatusUnauthorized), statusErr(http.StatusUnauthorized)},
	})
	defer func() {
		reporter.cancel()
		<-reporter.done
	}()

	waitFor(t, 2*time.Second, func() bool { return h.sleepCount() >= 3 })
	if regs := calls.registerCount(); regs < 3 {
		t.Fatalf("registrations = %d, want one attempt per rejection", regs)
	}
	sleeps, logs := h.snapshot()
	want := []time.Duration{mountLeaseRetry, 2 * mountLeaseRetry, 4 * mountLeaseRetry}
	for i, d := range want {
		if i >= len(sleeps) || sleeps[i] != d {
			t.Fatalf("registration sleeps = %#v, want prefix %#v", sleeps, want)
		}
	}
	rejected := 0
	for _, line := range logs {
		if strings.Contains(line, "registration rejected") {
			rejected++
		}
	}
	if rejected != 1 {
		t.Fatalf("logs = %#v, want one registration-rejected line for one failure class", logs)
	}
}

// A transient registration failure keeps the base interval: a mount that starts
// while the API is briefly unreachable still gets its lease within one retry of
// the server returning, rather than at the end of a backoff schedule.
func TestMountLeaseTransientRegistrationKeepsBaseRetry(t *testing.T) {
	h := &leaseHarness{}
	reporter, _ := startHarness(t, h, leaseSequence{
		register: []error{statusErr(http.StatusBadGateway), statusErr(http.StatusTooManyRequests), nil},
	})
	defer reporter.Close()

	waitFor(t, 2*time.Second, func() bool { return h.sleepCount() >= 2 })
	sleeps, _ := h.snapshot()
	for _, d := range sleeps[:2] {
		if d != mountLeaseRetry {
			t.Fatalf("transient registration sleeps = %#v, want base %v", sleeps, mountLeaseRetry)
		}
	}
}

// A successful registration resets the schedule, so the next rejection starts
// from the base delay again.
func TestMountLeaseRetryResetsAfterSuccessfulRegistration(t *testing.T) {
	h := &leaseHarness{}
	reporter, _ := startHarness(t, h, leaseSequence{
		register:  []error{statusErr(http.StatusUnauthorized), nil, statusErr(http.StatusUnauthorized)},
		heartbeat: []error{nil, statusErr(http.StatusUnauthorized)},
	})
	defer reporter.Close()

	waitFor(t, 3*time.Second, func() bool { return h.sleepCount() >= 4 })
	sleeps, _ := h.snapshot()
	var retries []time.Duration
	for _, d := range sleeps {
		if d != mountLeaseHeartbeat {
			retries = append(retries, d)
		}
	}
	if len(retries) < 2 || retries[0] != mountLeaseRetry || retries[1] != mountLeaseRetry {
		t.Fatalf("retry delays = %#v, want two base-interval retries", retries)
	}
}

// A server that drops the lease route mid-flight keeps mounting, but says so
// once instead of discarding a live lease silently.
func TestMountLeaseUnsupportedHeartbeatReportsAndStops(t *testing.T) {
	h := &leaseHarness{}
	reporter, _ := startHarness(t, h, leaseSequence{heartbeat: []error{statusErr(http.StatusMethodNotAllowed)}, heartbeatRepeat: true})
	select {
	case <-reporter.done:
	case <-time.After(2 * time.Second):
		t.Fatal("reporter kept retrying an unsupported heartbeat")
	}
	_, logs := h.snapshot()
	if len(logs) != 1 || !strings.Contains(logs[0], "unsupported") {
		t.Fatalf("logs = %#v, want one unsupported-heartbeat line", logs)
	}
}

func TestNextMountLeaseRetryDoublesAndCaps(t *testing.T) {
	if got := nextMountLeaseRetry(mountLeaseRetry); got != 2*mountLeaseRetry {
		t.Fatalf("first backoff = %v, want %v", got, 2*mountLeaseRetry)
	}
	if got := nextMountLeaseRetry(mountLeaseRetryMax); got != mountLeaseRetryMax {
		t.Fatalf("capped backoff = %v, want %v", got, mountLeaseRetryMax)
	}
	if got := nextMountLeaseRetry(3 * time.Minute); got != mountLeaseRetryMax {
		t.Fatalf("backoff above cap = %v, want %v", got, mountLeaseRetryMax)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition was not met before the deadline")
}
