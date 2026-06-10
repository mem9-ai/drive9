package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

const cliLayerInlineLimit = 96 << 20

func layerBaseRevision(ctx context.Context, c *client.Client, remotePath string) (int64, error) {
	stat, err := c.StatCtx(ctx, remotePath)
	if client.IsNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if stat.IsDir {
		return 0, fmt.Errorf("layer target %s is a directory", remotePath)
	}
	return stat.Revision, nil
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

func uploadReaderToLayer(ctx context.Context, c *client.Client, layerRef, remotePath string, r io.Reader, size int64, mode uint32, hasMode bool) error {
	baseRev, err := layerBaseRevision(ctx, c, remotePath)
	if err != nil {
		return err
	}
	if size <= cliLayerInlineLimit {
		data, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("read layer upload: %w", err)
		}
		if int64(len(data)) != size {
			return fmt.Errorf("layer upload size mismatch: metadata=%d actual=%d", size, len(data))
		}
		req := client.FSLayerEntryRequest{
			Path:         remotePath,
			Op:           "upsert",
			Kind:         "file",
			BaseRevision: baseRev,
			Content:      data,
			SizeBytes:    size,
		}
		if hasMode {
			req.Mode = mode & 0o777
		}
		_, err = c.UpsertFSLayerEntry(ctx, layerRef, req)
		return err
	}
	_, err = c.UploadFSLayerFile(ctx, layerRef, remotePath, r, size, baseRev, mode, hasMode)
	return err
}

func mkdirLayerPath(ctx context.Context, c *client.Client, layerRef, remotePath string, mode uint32) error {
	_, err := c.UpsertFSLayerEntry(ctx, layerRef, client.FSLayerEntryRequest{
		Path: remotePath,
		Op:   "mkdir",
		Kind: "dir",
		Mode: mode & 0o777,
	})
	return err
}

func whiteoutLayerPath(ctx context.Context, c *client.Client, layerRef, remotePath string, recursive bool) error {
	kind := "file"
	stat, err := c.StatCtx(ctx, remotePath)
	if err == nil && stat.IsDir {
		if !recursive {
			return fmt.Errorf("%s is a directory (use -r/--recursive)", remotePath)
		}
		entries, err := c.ListCtx(ctx, remotePath)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("layer directory whiteout requires empty directory: %s", remotePath)
		}
		kind = "dir"
	} else if err != nil && !client.IsNotFound(err) {
		return err
	}
	if recursive {
		kind = "dir"
	}
	_, err = c.UpsertFSLayerEntry(ctx, layerRef, client.FSLayerEntryRequest{
		Path: remotePath,
		Op:   "whiteout",
		Kind: kind,
	})
	return err
}

func chmodLayerPath(ctx context.Context, c *client.Client, layerRef, remotePath string, mode uint32) error {
	_, err := c.UpsertFSLayerEntry(ctx, layerRef, client.FSLayerEntryRequest{
		Path: remotePath,
		Op:   "chmod",
		Kind: "file",
		Mode: mode & 0o777,
	})
	return err
}

func symlinkLayerPath(ctx context.Context, c *client.Client, layerRef, target, linkPath string) error {
	_, err := c.UpsertFSLayerEntry(ctx, layerRef, client.FSLayerEntryRequest{
		Path:        linkPath,
		Op:          "symlink",
		Kind:        "symlink",
		ContentText: target,
		SizeBytes:   int64(len(target)),
		Mode:        0o120777,
	})
	return err
}

func renameLayerPath(ctx context.Context, c *client.Client, layerRef, oldPath, newPath string) error {
	_, err := c.UpsertFSLayerEntry(ctx, layerRef, client.FSLayerEntryRequest{
		Path:        oldPath,
		Op:          "rename",
		Kind:        "file",
		ContentText: newPath,
	})
	return err
}

func hardlinkLayerPath(ctx context.Context, c *client.Client, layerRef, srcPath, dstPath string) error {
	data, err := c.ReadCtx(ctx, srcPath)
	if err != nil {
		return err
	}
	mode := uint32(0)
	hasMode := false
	if stat, err := c.StatCtx(ctx, srcPath); err == nil && stat.HasMode {
		mode = stat.Mode & 0o777
		hasMode = true
	}
	return uploadBytesToLayer(ctx, c, layerRef, dstPath, data, mode, hasMode)
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
