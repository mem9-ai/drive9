package extent

import (
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

func TestTransportCallIgnoresCallerCancel(t *testing.T) {
	t.Parallel()
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	var sawCanceled bool
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		select {
		case <-ctx.Done():
			sawCanceled = true
		default:
		}
		return []byte(`{"errno":0}`), 0, nil
	})
	var resp struct {
		Errno int `json:"errno"`
	}
	if st := tr.Call(jfsmeta.WrapWithoutCancel(parent, 1, 0, []uint32{0}), "setlk", map[string]any{"inode": 1}, &resp); st != 0 {
		t.Fatalf("Call status=%v, want 0", st)
	}
	if sawCanceled {
		t.Fatal("HTTP meta RPC saw a canceled context; JuiceFS polls Canceled() between lock txns")
	}
	if resp.Errno != 0 {
		t.Fatalf("errno=%d", resp.Errno)
	}
}

func TestTransportCallInjectsBlockFlag(t *testing.T) {
	t.Parallel()
	var got json.RawMessage
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		got = append([]byte(nil), raw...)
		return []byte(`{"errno":0}`), 0, nil
	})
	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(LockBlockCtxKey, true)
	var resp struct {
		Errno int `json:"errno"`
	}
	if st := tr.Call(ctx, jfsmeta.Drive9OpSetlk, map[string]any{"inode": 1, "owner": 2}, &resp); st != 0 {
		t.Fatalf("Call status=%v", st)
	}
	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Fatal(err)
	}
	if obj["block"] != true {
		t.Fatalf("payload=%s, want block=true", got)
	}
}

// TestTransportTimeoutBoundsCall pins that a non-blocking meta RPC that never
// answers is cut off at Timeout instead of hanging forever.
func TestTransportTimeoutBoundsCall(t *testing.T) {
	t.Parallel()
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		<-ctx.Done()
		return nil, int(syscall.EIO), ctx.Err()
	})
	tr.Timeout = 20 * time.Millisecond
	start := time.Now()
	var resp struct{}
	if st := tr.Call(jfsmeta.Background(), "load", map[string]any{}, &resp); st != syscall.ETIMEDOUT {
		t.Fatalf("Call status=%v, want ETIMEDOUT (not EINTR)", st)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Call took %v, want it bounded by Timeout", elapsed)
	}
}

// TestNewTransportDefaultsMetaCallTimeout pins the default every transport
// (mount, CLI/SDK, server-side compactor) now relies on; deleting it would
// silently restore unbounded metadata RPCs.
func TestNewTransportDefaultsMetaCallTimeout(t *testing.T) {
	t.Parallel()
	tr := NewTransport(func(context.Context, string, json.RawMessage) (json.RawMessage, int, error) {
		return nil, 0, nil
	})
	if tr.Timeout != MetaCallTimeout {
		t.Fatalf("NewTransport Timeout = %v, want %v", tr.Timeout, MetaCallTimeout)
	}
}

// TestTransportCallPassesOpToEnter pins the teardown gate's plumbing: Call must
// hand Enter the op name so the hold can exempt the session-cleanup RPC.
func TestTransportCallPassesOpToEnter(t *testing.T) {
	t.Parallel()
	var got string
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		return []byte(`{"errno":0}`), 0, nil
	})
	tr.Enter = func(op string) bool {
		got = op
		return true
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	if st := tr.Call(jfsmeta.Background(), jfsmeta.Drive9OpCleanStaleSession, map[string]any{}, &resp); st != 0 {
		t.Fatalf("Call status=%v", st)
	}
	if got != jfsmeta.Drive9OpCleanStaleSession {
		t.Fatalf("Enter op = %q, want %q", got, jfsmeta.Drive9OpCleanStaleSession)
	}
}

// TestTransportCallMapsBackendErrorToEIO keeps a genuine transport failure
// distinct from the timeout and interrupt mappings.
func TestTransportCallMapsBackendErrorToEIO(t *testing.T) {
	t.Parallel()
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		return nil, 0, errors.New("backend down")
	})
	var resp struct{}
	if st := tr.Call(jfsmeta.Background(), "load", map[string]any{}, &resp); st != syscall.EIO {
		t.Fatalf("Call status=%v, want EIO", st)
	}
}

func TestTransportCallStillReturnsErrno(t *testing.T) {
	t.Parallel()
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		return []byte(`{"errno":11}`), 0, nil
	})
	var resp struct {
		Errno int `json:"errno"`
	}
	if st := tr.Call(jfsmeta.Background(), "setlk", map[string]any{}, &resp); st != 0 {
		t.Fatalf("Call status=%v, want 0 (errno lives in JSON)", st)
	}
	if resp.Errno != int(syscall.EAGAIN) && resp.Errno != 11 {
		t.Fatalf("errno=%d, want EAGAIN/11", resp.Errno)
	}
}

func TestTransportCarriesSessionIdentityWithoutChangingTarget(t *testing.T) {
	t.Parallel()
	const sid = uint64(1<<63 + 123)
	const owner = uint64(1<<63 + 321)
	var got map[string]json.RawMessage
	tr := NewTransport(func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, int, error) {
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		return []byte(`{"errno":0,"session_guard":true}`), 0, nil
	})
	call := func(op string, in any) {
		t.Helper()
		got = nil
		ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(LockBlockCtxKey, true)
		var resp struct {
			Errno int `json:"errno"`
		}
		if st := tr.Call(ctx, op, in, &resp); st != 0 {
			t.Fatal(st)
		}
	}
	call("init", map[string]any{})
	if _, ok := got["caller_sid"]; ok {
		t.Fatal("bootstrap has a session")
	}
	call("new_session", map[string]any{"sid": sid})
	if !tr.SessionGuardSupported() {
		t.Fatal("missing server capability")
	}
	for _, op := range []string{"write", "compact", "flock", "clean_stale_session"} {
		call(op, map[string]any{"sid": 77, "owner": owner})
		var caller, target, actualOwner uint64
		if err := json.Unmarshal(got["caller_sid"], &caller); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(got["sid"], &target); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(got["owner"], &actualOwner); err != nil {
			t.Fatal(err)
		}
		if caller != sid || target != 77 || actualOwner != owner {
			t.Fatalf("%s: caller=%d target=%d owner=%d", op, caller, target, actualOwner)
		}
		if op == "flock" && string(got["block"]) != "true" {
			t.Fatal("missing blocking flag")
		}
	}
}

func TestTransportSessionRegistrationFailureAndLegacyServer(t *testing.T) {
	t.Parallel()
	for _, reply := range []string{`{"errno":5}`, `{"errno":0}`, `{}`} {
		t.Run(reply, func(t *testing.T) {
			var got map[string]json.RawMessage
			tr := NewTransport(func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, int, error) {
				got = nil
				if err := json.Unmarshal(raw, &got); err != nil {
					t.Fatal(err)
				}
				return []byte(reply), 0, nil
			})
			st := tr.Call(jfsmeta.Background(), "new_session", map[string]any{"sid": 101}, nil)
			if (st != 0) != (reply == `{}`) {
				t.Fatalf("registration status=%v", st)
			}
			if tr.SessionGuardSupported() {
				t.Fatal("claimed unsupported session guard")
			}
			if st := tr.Call(jfsmeta.Background(), "write", map[string]any{}, nil); st != 0 {
				t.Fatal(st)
			}
			_, hasSID := got["caller_sid"]
			if hasSID != (reply == `{"errno":0}`) {
				t.Fatalf("payload=%v", got)
			}
		})
	}
}

func TestTransportCannotRebindAnExistingSession(t *testing.T) {
	t.Parallel()
	var caller uint64
	tr := NewTransport(func(_ context.Context, _ string, raw json.RawMessage) (json.RawMessage, int, error) {
		var in struct {
			CallerSID uint64 `json:"caller_sid"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			t.Fatal(err)
		}
		caller = in.CallerSID
		return []byte(`{"errno":0,"session_guard":true}`), 0, nil
	})
	ctx := jfsmeta.Background()
	if st := tr.Call(ctx, "new_session", map[string]any{"sid": 101}, nil); st != 0 {
		t.Fatal(st)
	}
	if st := tr.Call(ctx, "new_session", map[string]any{"sid": 102}, nil); st != syscall.EIO {
		t.Fatalf("another runtime rebound the transport: %v", st)
	}
	if st := tr.Call(ctx, "write", map[string]any{}, nil); st != 0 {
		t.Fatal(st)
	}
	if caller != 101 {
		t.Fatalf("old request acquired a new identity: %d", caller)
	}
}
