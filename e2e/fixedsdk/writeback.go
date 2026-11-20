package main

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Exercise the mount's actual writeback/core chain without requiring /dev/fuse.
// A partial edit and a sequential replacement both use the fixed full-file
// publication protocol. Main retains its existing PATCH/stream dispatch.
func writebackEdits(ctx context.Context, c *client.Client, data []byte) error {
	w, err := writeback.New(writeback.Options{Client: c, Policy: writeback.PolicyCloseSync, PreloadLimit: 1})
	if err != nil {
		return errors.New("writeback initialization")
	}
	terminal, err := core.New(c, core.Options{})
	if err != nil {
		return errors.New("core initialization")
	}
	chain, err := fs.NewChain(w, terminal)
	if err != nil {
		return errors.New("writeback chain")
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = chain.Stop(stopCtx)
	}()
	for i, sequential := range []bool{false, true} {
		opened, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/file", Flags: syscall.O_RDWR})
		if err != nil || opened.Handle == nil {
			return errors.New("writeback open")
		}
		h := opened.Handle
		var writeErr error
		if sequential {
			for off := 0; off < len(data); off += 256 << 10 {
				if writeErr = h.Write(ctx, int64(off), data[off:min(off+(256<<10), len(data))]); writeErr != nil {
					break
				}
			}
		} else {
			// Cross the upload boundary and leave the rest of the old image untouched.
			writeErr = h.Write(ctx, (4<<20)-2, []byte("edit"))
			copy(data[(4<<20)-2:], []byte("edit"))
		}
		flushErr := error(nil)
		if writeErr == nil {
			flushErr = h.Flush(ctx)
			if i == 0 {
				// The paired server fixture drops this completion response
				// AFTER the normal handler has committed and settled quota.
				if flushErr == nil || !client.IsCommitAttempted(flushErr) {
					_ = h.Release(ctx)
					return errors.New("writeback lost-completion injection")
				}
				flushErr = h.Flush(ctx)
			}
		}
		releaseErr := h.Release(ctx)
		if writeErr != nil || flushErr != nil || releaseErr != nil {
			return errors.New("writeback commit")
		}
		if !readEquals(ctx, c, "/file", data) {
			return errors.New("writeback preserved bytes")
		}
		if i == 0 {
			data[0] = 'z'
		}
	}
	return nil
}
