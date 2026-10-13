// Deep copy of snapshot trees (fork and checkpoint materialization). The
// copy is client-side and content-independent: server-side CopyFile shares
// the source's file entity, and an overwrite of either side would mutate
// the shared entity — a fork must be self-contained.
package layerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mem9-ai/drive9/pkg/client"
)

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
	// Deterministic parent-first directory creation: map ranging order
	// would let a child's implicit parent race the explicit mkdir.
	dirs := make([]string, 0, len(snap.Tree))
	for serverPath, entry := range snap.Tree {
		if entry.IsDir {
			dirs = append(dirs, serverPath)
		}
	}
	sort.Strings(dirs)
	for _, serverPath := range dirs {
		if err := ensureTreeDir(serverPath, snap.Tree[serverPath].Mode); err != nil {
			return fmt.Errorf("mkdir %s: %w", serverPath, err)
		}
	}
	for _, entry := range snap.Tree {
		if entry.IsDir {
			continue
		}
		if entry.IsSymlink {
			// A copied snapshot must keep links as links: writing the
			// payload with WriteCtx would materialize a regular file.
			target, terr := cl.ReadCtx(ctx, MarkerPath(src.Tree, entry.ServerPath))
			if terr != nil {
				return fmt.Errorf("read symlink %s: %w", entry.ServerPath, terr)
			}
			if serr := cl.SymlinkCtx(ctx, string(target), MarkerPath(dst.Tree, entry.ServerPath)); serr != nil {
				return fmt.Errorf("symlink %s: %w", entry.ServerPath, serr)
			}
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

// copyFileServer streams one file from src to dst through the client's
// negotiated upload path: simple PUT below the server threshold, V2
// multipart (adaptive part sizes, part checksums) above it.
func copyFileServer(ctx context.Context, cl *client.Client, src, dst string, size int64) error {
	ra, err := newRangedReaderAt(ctx, cl, src, size)
	if err != nil {
		return err
	}
	return cl.WriteStream(ctx, dst, ra, size, nil)
}
