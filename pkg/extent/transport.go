package extent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

// MetaOpFunc is the server-side extent meta RPC (datastore.Store.RunExtentMetaOp
// or an HTTP client of POST /v1/extent/meta).
type MetaOpFunc func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error)

// Transport implements juicefs meta.Drive9Transport.
type Transport struct {
	CallFn MetaOpFunc
	Delay  time.Duration
}

func NewTransport(fn MetaOpFunc) *Transport {
	return &Transport{CallFn: fn}
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
		fmt.Fprintf(os.Stderr, "drive9: extent meta %s dur=%s\n", op, d)
	}
	if err != nil {
		if httpCtx.Err() != nil {
			return syscall.EINTR
		}
		return syscall.EIO
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
