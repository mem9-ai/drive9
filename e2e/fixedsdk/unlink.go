package main

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Exercise the existing anonymous-handle path against a fixed Layer. The last
// name is removed and recreated; the open descriptor must keep its original
// bytes, and writes through it must not modify the replacement name.
func unlinkedQueueHandle(ctx context.Context, c *client.Client) error {
	w, err := writeback.New(writeback.Options{Client: c, Policy: writeback.PolicyCloseSync})
	if err != nil {
		return errors.New("unlink writeback initialization")
	}
	terminal, err := core.New(c, core.Options{})
	if err != nil {
		return errors.New("unlink core initialization")
	}
	chain, err := fs.NewChain(w, terminal)
	if err != nil {
		return errors.New("unlink chain")
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = chain.Stop(stopCtx)
	}()
	opened, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/queue-file", Flags: syscall.O_RDWR})
	if err != nil || opened.Handle == nil {
		return errors.New("unlink open descriptor")
	}
	h := opened.Handle
	defer func() {
		if h != nil {
			_ = h.Release(ctx)
		}
	}()
	want := queueProbeData()
	got := make([]byte, len(want))
	if n, err := h.Read(ctx, 0, got); err != nil || n != len(want) || !bytes.Equal(got, want) {
		return errors.New("unlink initial bytes")
	}
	if _, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/queue-file"}); err != nil {
		return errors.New("unlink last name")
	}
	if _, err := c.StatCtx(ctx, "/queue-file"); !client.IsNotFound(err) {
		return errors.New("unlink name remains visible")
	}
	if n, err := h.Read(ctx, 0, got); err != nil || n != len(want) || !bytes.Equal(got, want) {
		return errors.New("unlink lost descriptor bytes")
	}
	replacement := []byte("replacement")
	if err := c.WriteCtx(ctx, "/queue-file", replacement); err != nil {
		return errors.New("unlink replacement write")
	}
	if n, err := h.Read(ctx, int64(len(want)), got[:1]); err != nil || n != 0 {
		return errors.New("unlink descriptor eof")
	}
	if err := h.Write(ctx, 0, []byte("x")); err != nil {
		return errors.New("unlink anonymous write")
	}
	if err := h.Flush(ctx); err != nil {
		return errors.New("unlink anonymous flush")
	}
	want[0] = 'x'
	if n, err := h.Read(ctx, 0, got); err != nil || n != len(want) || !bytes.Equal(got, want) {
		return errors.New("unlink anonymous bytes")
	}
	if err := h.Release(ctx); err != nil {
		return errors.New("unlink descriptor release")
	}
	h = nil
	if !readEquals(ctx, c, "/queue-file", replacement) {
		return errors.New("unlink descriptor changed replacement")
	}
	return nil
}
