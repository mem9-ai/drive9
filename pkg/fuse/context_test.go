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
	first := observeFuseInterruptFlag(ch)
	second := observeFuseInterruptFlag(ch)
	if !first.countOnce() {
		t.Fatal("first observer of a channel should count")
	}
	if second.countOnce() {
		t.Fatal("second observer of the same channel must not count again")
	}
	other := make(chan struct{})
	if !observeFuseInterruptFlag(other).countOnce() {
		t.Fatal("a different request channel should count")
	}
	// Once every derived context has completed, the entry is reclaimed; a
	// later re-registration counts again as a new observation window.
	second.release(ch)
	first.release(ch)
	if _, ok := fuseInterruptFlags.Load(ch); ok {
		t.Fatal("entry must be reclaimed after all derived contexts completed")
	}
	if !observeFuseInterruptFlag(ch).countOnce() {
		t.Fatal("a re-registered channel should count again")
	}
}
