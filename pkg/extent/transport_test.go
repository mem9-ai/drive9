package extent

import (
	"context"
	"encoding/json"
	"syscall"
	"testing"

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
