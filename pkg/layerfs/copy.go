// Deep copy of snapshot trees (fork and checkpoint materialization). The
// copy is client-side and content-independent: server-side CopyFile shares
// the source's file entity, and an overwrite of either side would mutate
// the shared entity — a fork must be self-contained.
package layerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// copyChunkBytes bounds the copy buffer.
const copyChunkBytes = 4 << 20

// CopySnapshot deep-copies the marker trees of srcBase into dstBase. It
// writes the destination BEFORE any registry row references it, so a crash
// mid-copy leaves an invisible orphan, never a half layer.
func CopySnapshot(ctx context.Context, cl *client.Client, srcBase, dstBase string) error {
	src := RootsOf(srcBase)
	dst := RootsOf(dstBase)
	snap, err := LoadSnapshot(ctx, cl, srcBase)
	if err != nil {
		return fmt.Errorf("load source snapshot: %w", err)
	}
	// Directories first (parents before children, lexical order gives it).
	ensureTreeDir := func(serverPath string, mode uint32) error {
		if _, err := cl.StatCtx(ctx, MarkerPath(dst.Tree, serverPath)); err == nil {
			return nil
		} else if !client.IsNotFound(err) {
			return err
		}
		if mode == 0 {
			mode = 0o755
		}
		return cl.MkdirCtx(ctx, MarkerPath(dst.Tree, serverPath), mode)
	}
	for _, entry := range snap.Tree {
		if !entry.IsDir {
			continue
		}
		if err := ensureTreeDir(entry.ServerPath, entry.Mode); err != nil {
			return fmt.Errorf("mkdir %s: %w", entry.ServerPath, err)
		}
	}
	for _, entry := range snap.Tree {
		if entry.IsDir {
			continue
		}
		if err := copyFileServer(ctx, cl, MarkerPath(src.Tree, entry.ServerPath), MarkerPath(dst.Tree, entry.ServerPath), entry.Size); err != nil {
			return fmt.Errorf("copy %s: %w", entry.ServerPath, err)
		}
		if entry.Mode != 0 && entry.Mode != 0o644 {
			if err := cl.ChmodCtx(ctx, MarkerPath(dst.Tree, entry.ServerPath), entry.Mode); err != nil {
				return fmt.Errorf("chmod %s: %w", entry.ServerPath, err)
			}
		}
	}
	for serverPath, marker := range snap.Blackouts {
		root := dst.Blackouts
		if marker.IsDir {
			root = dst.BlackoutDirs
		}
		if err := cl.WriteCtx(ctx, MarkerPath(root, serverPath), marker.Encode()); err != nil {
			return fmt.Errorf("blackout %s: %w", serverPath, err)
		}
	}
	for serverPath, anchor := range snap.Cas {
		if err := cl.WriteCtx(ctx, MarkerPath(dst.Cas, serverPath), EncodeCas(anchor.BaseRev, anchor.HasBase)); err != nil {
			return fmt.Errorf("cas %s: %w", serverPath, err)
		}
	}
	for serverPath, patch := range snap.Attrs {
		content, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		if err := cl.WriteCtx(ctx, MarkerPath(dst.Attrs, serverPath), content); err != nil {
			return fmt.Errorf("attrs %s: %w", serverPath, err)
		}
	}
	return nil
}

// copyInlineMax bounds the whole-image copy path; larger files stream as
// multipart uploads.
const copyInlineMax = 8 << 20

// copyFileServer streams one file from src to dst (symlinks ride the same
// content pipeline — drive9 stores a link's target as its content).
func copyFileServer(ctx context.Context, cl *client.Client, src, dst string, size int64) error {
	if size <= copyInlineMax {
		data, err := cl.ReadCtx(ctx, src)
		if err != nil {
			return err
		}
		return cl.WriteCtx(ctx, dst, data)
	}
	rc, err := cl.ReadStream(ctx, src)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	sw := cl.NewStreamWriter(ctx, dst, size)
	part := 0
	var last []byte
	abort := func() {
		abortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = sw.Abort(abortCtx)
		cancel()
	}
	buf := make([]byte, copyChunkBytes)
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			part++
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if last != nil {
				if werr := sw.WritePart(ctx, part-1, last); werr != nil {
					abort()
					return werr
				}
			}
			last = chunk
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			abort()
			return rerr
		}
	}
	if part == 0 {
		abort()
		return fmt.Errorf("copy %s: stream ended with no parts", src)
	}
	// The final part rides Complete's synchronous upload.
	return sw.Complete(ctx, part, last)
}
