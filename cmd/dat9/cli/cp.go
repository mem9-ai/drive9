package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Cp copies files between local and remote.
//
// Remote paths use a ":" prefix to distinguish from local paths:
//
//	dat9 cp local.txt :/remote/path       upload
//	dat9 cp :/remote/path local.txt       download
//	dat9 cp :/remote/a :/remote/b         server-side copy (zero-copy)
//	dat9 cp - :/remote/path               upload from stdin
//	dat9 cp :/remote/path -               download to stdout
//	dat9 cp --resume local.txt :/remote   resume interrupted upload
func Cp(c *client.Client, args []string) error {
	resume := false
	filtered := args[:0]
	for _, a := range args {
		if a == "--resume" {
			resume = true
		} else {
			filtered = append(filtered, a)
		}
	}
	args = filtered

	if len(args) != 2 {
		return fmt.Errorf("usage: dat9 cp [--resume] <src> <dst>")
	}
	src, dst := args[0], args[1]

	srcRemote := isRemote(src)
	dstRemote := isRemote(dst)

	if srcRemote {
		src = src[1:]
	}
	if dstRemote {
		dst = dst[1:]
	}

	ctx := context.Background()

	switch {
	case src == "-" && dstRemote:
		// stdin → remote
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		return c.WriteStream(ctx, dst, strings.NewReader(string(data)), int64(len(data)), printProgress)

	case srcRemote && dst == "-":
		// remote → stdout
		return streamToStdout(ctx, c, src)

	case !srcRemote && dstRemote:
		// local → remote (upload)
		if resume {
			return resumeUpload(ctx, c, src, dst)
		}
		return uploadFile(ctx, c, src, dst)

	case srcRemote && !dstRemote:
		// remote → local (download)
		return downloadFile(ctx, c, src, dst)

	case srcRemote && dstRemote:
		// remote → remote (server-side copy, zero-copy)
		return c.Copy(src, dst)

	default:
		return fmt.Errorf("at least one path must be remote (use : prefix, e.g. :/path)")
	}
}

func uploadFile(ctx context.Context, c *client.Client, localPath, remotePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", localPath, err)
	}

	return c.WriteStream(ctx, remotePath, f, info.Size(), printProgress)
}

func resumeUpload(ctx context.Context, c *client.Client, localPath, remotePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", localPath, err)
	}

	return c.ResumeUpload(ctx, remotePath, f, info.Size(), printProgress)
}

func downloadFile(ctx context.Context, c *client.Client, remotePath, localPath string) error {
	rc, err := c.ReadStream(ctx, remotePath)
	if err != nil {
		return err
	}
	defer rc.Close()

	out, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", localPath, err)
	}
	defer out.Close()

	_, err = io.Copy(out, rc)
	return err
}

func streamToStdout(ctx context.Context, c *client.Client, remotePath string) error {
	rc, err := c.ReadStream(ctx, remotePath)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(os.Stdout, rc)
	return err
}

// printProgress displays part-level upload progress.
func printProgress(partNumber, totalParts int, bytesUploaded int64) {
	fmt.Fprintf(os.Stderr, "\r  part %d/%d uploaded (%d bytes)", partNumber, totalParts, bytesUploaded)
	if partNumber == totalParts {
		fmt.Fprintln(os.Stderr)
	}
}

// isRemote returns true if a path refers to a remote dat9 path.
// Remote paths use a ":" prefix (e.g., ":/data/file.txt").
func isRemote(path string) bool {
	if path == "-" {
		return false
	}
	return strings.HasPrefix(path, ":")
}
