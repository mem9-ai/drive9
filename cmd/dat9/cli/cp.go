package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Cp copies files between local and remote.
//
// Remote paths use ":" or "<name>:" prefix:
//
//	dat9 fs cp local.txt :/remote/path          upload (current context)
//	dat9 fs cp local.txt mydb:/remote/path      upload (mydb context)
//	dat9 fs cp mydb:/remote/path local.txt      download
//	dat9 fs cp :/remote/a :/remote/b            server-side copy
//	dat9 fs cp - :/remote/path                  upload from stdin
//	dat9 fs cp :/remote/path -                  download to stdout
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
		return fmt.Errorf("usage: dat9 fs cp [--resume] <src> <dst>")
	}
	src, dst := args[0], args[1]

	srcRP, srcIsRemote := ParseRemote(src)
	dstRP, dstIsRemote := ParseRemote(dst)

	ctxName := ""
	if srcIsRemote && srcRP.Context != "" {
		ctxName = srcRP.Context
	}
	if dstIsRemote && dstRP.Context != "" {
		if ctxName != "" && ctxName != dstRP.Context {
			return fmt.Errorf("cross-context copy not supported: %s vs %s", ctxName, dstRP.Context)
		}
		ctxName = dstRP.Context
	}
	if ctxName != "" {
		c = NewClientForContext(ctxName)
	}

	ctx := context.Background()

	switch {
	case src == "-" && dstIsRemote:
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		return c.WriteStream(ctx, dstRP.Path, bytes.NewReader(data), int64(len(data)), printProgress)

	case srcIsRemote && dst == "-":
		return streamToStdout(ctx, c, srcRP.Path)

	case !srcIsRemote && dstIsRemote:
		if resume {
			return resumeUpload(ctx, c, src, dstRP.Path)
		}
		return uploadFile(ctx, c, src, dstRP.Path)

	case srcIsRemote && !dstIsRemote:
		return downloadFile(ctx, c, srcRP.Path, dst)

	case srcIsRemote && dstIsRemote:
		return c.Copy(srcRP.Path, dstRP.Path)

	default:
		return fmt.Errorf("at least one path must be remote (e.g. :/path or mydb:/path)")
	}
}

func uploadFile(ctx context.Context, c *client.Client, localPath, remotePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer func() { _ = f.Close() }()

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
	defer func() { _ = f.Close() }()

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
	defer func() { _ = rc.Close() }()

	out, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", localPath, err)
	}
	defer func() { _ = out.Close() }()

	_, err = io.Copy(out, rc)
	return err
}

func streamToStdout(ctx context.Context, c *client.Client, remotePath string) error {
	rc, err := c.ReadStream(ctx, remotePath)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	_, err = io.Copy(os.Stdout, rc)
	return err
}

func printProgress(partNumber, totalParts int, bytesUploaded int64) {
	fmt.Fprintf(os.Stderr, "\r  part %d/%d uploaded (%d bytes)", partNumber, totalParts, bytesUploaded)
	if partNumber == totalParts {
		fmt.Fprintln(os.Stderr)
	}
}
