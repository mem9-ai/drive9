package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/layerfs"
)

// resolveLayerRoots resolves a layer ref to its marker roots.
func resolveLayerRoots(ctx context.Context, c *client.Client, layerRef string) (layerfs.Roots, error) {
	layer, err := c.GetFSLayer(ctx, layerRef)
	if err != nil {
		return layerfs.Roots{}, err
	}
	return layerfs.RootsOf(layerfs.LayerRoot(layer.LayerID)), nil
}

// layerBaseAnchor stats the classic target of a layer write: the revision
// the content is derived from, or absent.
func layerBaseAnchor(ctx context.Context, c *client.Client, remotePath string) (int64, bool, error) {
	stat, err := c.StatCtx(ctx, remotePath)
	if client.IsNotFound(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if stat.IsDir {
		return 0, false, fmt.Errorf("layer target %s is a directory", remotePath)
	}
	return stat.Revision, true, nil
}

// stampCas writes the commit-CAS marker before content materializes —
// but only when none exists: the anchor records the base revision the
// layer's content was FIRST derived from, and re-stamping on later
// rewrites would bless base revisions the layer never saw.
func stampCas(ctx context.Context, c *client.Client, roots layerfs.Roots, remotePath string, baseRev int64, hasBase bool) error {
	marker := layerfs.MarkerPath(roots.Cas, remotePath)
	if _, err := c.StatCtx(ctx, marker); err == nil {
		return nil // keep the original derivation anchor
	} else if !client.IsNotFound(err) {
		return err
	}
	return c.WriteCtx(ctx, marker, layerfs.EncodeCas(baseRev, hasBase))
}

// restampCas overwrites the anchor for a rename destination: the moved
// content's derivation base is the destination's base, not the source's.
func restampCas(ctx context.Context, c *client.Client, roots layerfs.Roots, remotePath string, baseRev int64, hasBase bool) error {
	return c.WriteCtx(ctx, layerfs.MarkerPath(roots.Cas, remotePath), layerfs.EncodeCas(baseRev, hasBase))
}

// clearBlackout removes a stale blackout marker of either flavor (a
// recreate does not care which root held it).
func clearBlackout(ctx context.Context, c *client.Client, roots layerfs.Roots, remotePath string) error {
	for _, root := range []string{roots.Blackouts, roots.BlackoutDirs} {
		if err := c.DeleteFileCtx(ctx, layerfs.MarkerPath(root, remotePath)); err != nil && !client.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func uploadLocalFileToLayer(ctx context.Context, c *client.Client, layerRef, localPath, remotePath string) error {
	f, size, err := openLocalFile(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", localPath, err)
	}
	return uploadReaderToLayer(ctx, c, layerRef, remotePath, f, size, uint32(info.Mode().Perm()), true)
}

func uploadBytesToLayer(ctx context.Context, c *client.Client, layerRef, remotePath string, data []byte, mode uint32, hasMode bool) error {
	return uploadReaderToLayer(ctx, c, layerRef, remotePath, bytes.NewReader(data), int64(len(data)), mode, hasMode)
}

// seekableReader is both a sequential reader and a random-access source:
// the small-file path streams via Read, the multipart path slices via
// ReadAt.
type seekableReader interface {
	io.Reader
	io.ReaderAt
}

// uploadReaderToLayer lands file content in the layer's tree through the
// client's negotiated upload path (simple PUT below the server threshold,
// V2 multipart with adaptive part sizes and checksums above it): CAS
// marker first, content second, stale blackouts cleared last. The source
// must be seekable for the multipart path.
func uploadReaderToLayer(ctx context.Context, c *client.Client, layerRef, remotePath string, r seekableReader, size int64, mode uint32, hasMode bool) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	baseRev, hasBase, err := layerBaseAnchor(ctx, c, remotePath)
	if err != nil {
		return err
	}
	if err := stampCas(ctx, c, roots, remotePath, baseRev, hasBase); err != nil {
		return err
	}
	dst := layerfs.MarkerPath(roots.Tree, remotePath)
	if werr := c.WriteStream(ctx, dst, io.NewSectionReader(r, 0, size), size, nil); werr != nil {
		return werr
	}
	if hasMode && mode&0o777 != 0 && mode&0o777 != 0o644 {
		if cerr := c.ChmodCtx(ctx, dst, mode&0o777); cerr != nil && !client.IsNotFound(cerr) {
			return cerr
		}
	}
	return clearBlackout(ctx, c, roots, remotePath)
}

func mkdirLayerPath(ctx context.Context, c *client.Client, layerRef, remotePath string, mode uint32) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	baseRev, hasBase, err := layerBaseAnchor(ctx, c, remotePath)
	if err != nil {
		return err
	}
	if err := stampCas(ctx, c, roots, remotePath, baseRev, hasBase); err != nil {
		return err
	}
	if err := c.MkdirCtx(ctx, layerfs.MarkerPath(roots.Tree, remotePath), mode&0o777); err != nil {
		return err
	}
	return clearBlackout(ctx, c, roots, remotePath)
}

func whiteoutLayerPath(ctx context.Context, c *client.Client, layerRef, remotePath string, recursive bool) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	classic, statErr := c.StatCtx(ctx, remotePath)
	if statErr != nil && !client.IsNotFound(statErr) {
		return statErr
	}
	treePath := layerfs.MarkerPath(roots.Tree, remotePath)
	treeStat, treeErr := c.StatCtx(ctx, treePath)
	if treeErr != nil && !client.IsNotFound(treeErr) {
		return treeErr
	}
	isDir := (classic != nil && classic.IsDir) || (treeStat != nil && treeStat.IsDir)
	if isDir && !recursive {
		return fmt.Errorf("%s is a directory (use -r/--recursive)", remotePath)
	}
	if classic != nil && classic.IsDir {
		entries, lerr := c.ListCtx(ctx, remotePath)
		if lerr == nil && len(entries) > 0 {
			return fmt.Errorf("layer directory whiteout requires empty directory: %s", remotePath)
		}
	}
	if treeStat != nil {
		if treeStat.IsDir {
			if derr := c.DeleteDirCtx(ctx, treePath); derr != nil && !client.IsNotFound(derr) {
				return derr
			}
		} else if derr := c.DeleteFileCtx(ctx, treePath); derr != nil && !client.IsNotFound(derr) {
			return derr
		}
	}
	if classic == nil && treeStat == nil {
		return nil // nothing to hide
	}
	info := layerfs.Blackout{IsDir: isDir}
	if classic != nil && !classic.IsDir {
		info.BaseRev, info.HasBase = classic.Revision, true
	}
	root := roots.Blackouts
	if isDir {
		root = roots.BlackoutDirs
	}
	return c.WriteCtx(ctx, layerfs.MarkerPath(root, remotePath), info.Encode())
}

func chmodLayerPath(ctx context.Context, c *client.Client, layerRef, remotePath string, mode uint32) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	treePath := layerfs.MarkerPath(roots.Tree, remotePath)
	if stat, serr := c.StatCtx(ctx, treePath); serr == nil && stat != nil {
		return c.ChmodCtx(ctx, treePath, mode&0o777)
	} else if !client.IsNotFound(serr) {
		return serr
	}
	if stat, serr := c.StatCtx(ctx, remotePath); serr == nil && stat != nil {
		patch := layerfs.AttrsPatch{Mode: mode & 0o7777}
		content, merr := json.Marshal(patch)
		if merr != nil {
			return merr
		}
		return c.WriteCtx(ctx, layerfs.MarkerPath(roots.Attrs, remotePath), content)
	} else if !client.IsNotFound(serr) {
		return serr
	}
	return fmt.Errorf("%s: not found", remotePath)
}

func symlinkLayerPath(ctx context.Context, c *client.Client, layerRef, target, linkPath string) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	baseRev, hasBase, err := layerBaseAnchor(ctx, c, linkPath)
	if err != nil {
		return err
	}
	if err := stampCas(ctx, c, roots, linkPath, baseRev, hasBase); err != nil {
		return err
	}
	if err := c.SymlinkCtx(ctx, target, layerfs.MarkerPath(roots.Tree, linkPath)); err != nil {
		return err
	}
	return clearBlackout(ctx, c, roots, linkPath)
}

func renameLayerPath(ctx context.Context, c *client.Client, layerRef, oldPath, newPath string) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	treeSrc := layerfs.MarkerPath(roots.Tree, oldPath)
	treeDst := layerfs.MarkerPath(roots.Tree, newPath)
	if _, serr := c.StatCtx(ctx, treeSrc); serr == nil {
		if rerr := c.RenameCtx(ctx, treeSrc, treeDst); rerr != nil {
			return rerr
		}
	} else if !client.IsNotFound(serr) {
		return serr
	} else {
		// Classic source: copy the content into the tree, then hide the
		// origin.
		data, rerr := c.ReadCtx(ctx, oldPath)
		if rerr != nil {
			return rerr
		}
		mode := uint32(0o644)
		if st, serr := c.StatCtx(ctx, oldPath); serr == nil && st.HasMode {
			mode = st.Mode & 0o777
		}
		if uerr := uploadBytesToLayer(ctx, c, layerRef, newPath, data, mode, true); uerr != nil {
			return uerr
		}
	}
	// Hide the classic origin, clear the destination blackout, and move
	// the CAS anchor.
	if st, serr := c.StatCtx(ctx, oldPath); serr == nil && st != nil {
		info := layerfs.Blackout{IsDir: st.IsDir, BaseRev: st.Revision, HasBase: true}
		root := roots.Blackouts
		if info.IsDir {
			root = roots.BlackoutDirs
		}
		if werr := c.WriteCtx(ctx, layerfs.MarkerPath(root, oldPath), info.Encode()); werr != nil {
			return werr
		}
	} else if !client.IsNotFound(serr) {
		return serr
	}
	if err := clearBlackout(ctx, c, roots, newPath); err != nil {
		return err
	}
	// The moved content derives from the destination's base: restamp a
	// destination-specific anchor (the source's anchor described the
	// source lineage and must not leak onto the destination).
	baseQ, hasQ, err := layerBaseAnchor(ctx, c, newPath)
	if err != nil {
		return err
	}
	if err := restampCas(ctx, c, roots, newPath, baseQ, hasQ); err != nil {
		return err
	}
	_ = c.DeleteFileCtx(ctx, layerfs.MarkerPath(roots.Cas, oldPath))
	return nil
}

func hardlinkLayerPath(ctx context.Context, c *client.Client, layerRef, srcPath, dstPath string) error {
	roots, err := resolveLayerRoots(ctx, c, layerRef)
	if err != nil {
		return err
	}
	// A layer cannot hardlink into the classic namespace: the link
	// materializes as a content copy in the tree.
	src := layerfs.MarkerPath(roots.Tree, srcPath)
	if _, serr := c.StatCtx(ctx, src); serr != nil {
		if !client.IsNotFound(serr) {
			return serr
		}
		src = srcPath
	}
	data, rerr := c.ReadCtx(ctx, src)
	if rerr != nil {
		return rerr
	}
	mode := uint32(0o644)
	if st, serr := c.StatCtx(ctx, src); serr == nil && st.HasMode {
		mode = st.Mode & 0o777
	}
	return uploadBytesToLayer(ctx, c, layerRef, dstPath, data, mode, true)
}

func requireNoLayerWithRemoteContext(layerRef string, rp RemotePath, raw string) error {
	if strings.TrimSpace(layerRef) == "" || rp.Context == "" {
		return nil
	}
	return fmt.Errorf("--layer with context-scoped remote path %q is not supported; switch context first or omit the prefix", raw)
}

func layerRefMustBeEmpty(layerRef, feature string) error {
	if strings.TrimSpace(layerRef) == "" {
		return nil
	}
	return fmt.Errorf("--layer cannot be combined with %s", feature)
}

func readAllStdin() ([]byte, error) {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, fmt.Errorf("read stdin: %w", err)
	}
	return data, nil
}
