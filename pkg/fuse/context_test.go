package fuse

import (
	"context"
	"testing"
	"time"
)

func TestFuseCtxNilCancel(t *testing.T) {
	ctx, cancel := fuseCtx(nil)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatalf("fuseCtx(nil) was canceled immediately: %v", ctx.Err())
	default:
	}

	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("cancel did not cancel fuseCtx(nil)")
	}
}

func TestFuseCtxCancelChannel(t *testing.T) {
	ch := make(chan struct{})
	ctx, cancel := fuseCtx(ch)
	defer cancel()

	close(ch)
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != context.Canceled {
			t.Fatalf("ctx err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel channel did not cancel fuseCtx")
	}
}
