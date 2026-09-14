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

func TestCountFuseInterruptOncePerChannel(t *testing.T) {
	ch := make(chan struct{})
	if !countFuseInterruptOnce(ch) {
		t.Fatal("first observer of a channel should count")
	}
	if countFuseInterruptOnce(ch) {
		t.Fatal("second observer of the same channel must not count again")
	}
	other := make(chan struct{})
	if !countFuseInterruptOnce(other) {
		t.Fatal("a different request channel should count")
	}
	if countFuseInterruptOnce(other) {
		t.Fatal("second observer of the other channel must not count again")
	}
}
