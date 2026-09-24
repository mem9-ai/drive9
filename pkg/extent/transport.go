package extent

import (
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// MetaOpFunc is the server-side extent meta RPC (datastore.Store.RunExtentMetaOp
// or an HTTP client of POST /v1/extent/meta).
type MetaOpFunc func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error)

// Transport implements juicefs meta.Drive9Transport.
type Transport struct {
	CallFn MetaOpFunc
	Delay  time.Duration
	// Timeout bounds each non-blocking meta RPC when > 0. Blocking Flock/Setlk
	// are exempt: they legitimately wait for the lock. It must be set before the
	// transport is handed to extent.NewRuntime (never mutated afterwards).
	Timeout time.Duration
	// Enter/Leave, when set, bracket each meta RPC. A runtime sets them so its
	// teardown can wait for in-flight metadata before closing the session.
	// Enter receives the op name and reports false once the gate is closed, so
	// teardown can refuse new work while still admitting the session-cleanup RPC
	// it must run itself.
	Enter func(op string) bool
	Leave func()
}

// MetaCallTimeout bounds every non-blocking metadata RPC the extent runtime's
// transport serves, for its whole life: Init/Load/NewSession and the compaction
// loop's ClaimNextCompact, plus the steady-state FUSE read/write/getattr/
// setattr/lookup/unlink/rename calls that share this transport. A stalled
// endpoint therefore surfaces as ETIMEDOUT to the caller instead of hanging the
// request; blocking Flock/Setlk are exempt inside Call. The credential mint is
// a separate client call bounded by CredentialMintTimeout.
const MetaCallTimeout = 30 * time.Second

func NewTransport(fn MetaOpFunc) *Transport {
	// Default the bound here so every construction (FUSE, CLI/SDK, server-side
	// compactor) inherits it; a caller may override before use.
	return &Transport{CallFn: fn, Timeout: MetaCallTimeout}
}

// ExtentMetaClient is the HTTP meta RPC used by FUSE and drive9 fs.
type ExtentMetaClient interface {
	ExtentMeta(ctx context.Context, op string, req any) ([]byte, error)
}

func NewHTTPTransport(c ExtentMetaClient) *Transport {
	return NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		body, err := c.ExtentMeta(ctx, op, json.RawMessage(raw))
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return body, 0, nil
	})
}

// LockBlockCtxKey is set on a JuiceFS meta context for blocking Flock/Setlk.
// The HTTP engine injects `"block":true` so the server can loop like
// JuiceFS sql_lock instead of one EAGAIN RPC every 10ms.
type lockBlockKey struct{}

var LockBlockCtxKey = lockBlockKey{}

func lockBlockFromCtx(ctx jfsmeta.Context) bool {
	if ctx == nil {
		return false
	}
	v, ok := ctx.Value(LockBlockCtxKey).(bool)
	return ok && v
}

func (t *Transport) Call(ctx jfsmeta.Context, op string, req, resp any) syscall.Errno {
	if t == nil || t.CallFn == nil {
		return syscall.EIO
	}
	if t.Enter != nil && !t.Enter(op) {
		return syscall.EIO
	}
	if t.Leave != nil {
		defer t.Leave()
	}
	if t.Delay > 0 {
		time.Sleep(t.Delay)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return syscall.EINVAL
	}
	block := lockBlockFromCtx(ctx) && (op == jfsmeta.Drive9OpSetlk || op == jfsmeta.Drive9OpFlock)
	if block {
		var obj map[string]any
		if json.Unmarshal(raw, &obj) == nil {
			obj["block"] = true
			if patched, mErr := json.Marshal(obj); mErr == nil {
				raw = patched
			}
		}
	}
	std := context.Background()
	if ctx != nil {
		std = ctx
	}
	httpCtx := context.WithoutCancel(std)
	// Bound non-blocking RPCs; blocking Flock/Setlk must be allowed to wait.
	if !block && t.Timeout > 0 {
		var cancel context.CancelFunc
		httpCtx, cancel = context.WithTimeout(httpCtx, t.Timeout)
		defer cancel()
	}
	if block {
		var stop context.CancelFunc
		httpCtx, stop = context.WithCancel(httpCtx)
		defer stop()
		done := make(chan struct{})
		defer close(done)
		go func() {
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-done:
					return
				case <-tick.C:
					if ctx != nil && ctx.Canceled() {
						stop()
						return
					}
				}
			}
		}()
	}
	start := time.Now()
	body, _, err := t.CallFn(httpCtx, op, raw)
	if d := time.Since(start); d >= 50*time.Millisecond {
		// Debug, not stderr: a blocking lock acquisition is slow by
		// construction, so a sqlite workload would otherwise write this line
		// continuously to the mount's stderr.
		logger.Debug(ctx, "extent_meta_slow_op",
			zap.String("op", op),
			zap.Duration("duration", d))
	}
	if err != nil {
		switch {
		case errors.Is(httpCtx.Err(), context.DeadlineExceeded):
			// The RPC's own bound fired: report a timeout, not an interruption.
			return syscall.ETIMEDOUT
		case httpCtx.Err() != nil:
			return syscall.EINTR
		default:
			return syscall.EIO
		}
	}
	if resp == nil {
		return 0
	}
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, resp); err != nil {
		return syscall.EIO
	}
	return 0
}
