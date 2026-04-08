package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Cp copies files between local and remote.
//
// Remote paths use ":" or "<name>:" prefix:
//
//	drive9 fs cp local.txt :/remote/path          upload (current context)
//	drive9 fs cp local.txt mydb:/remote/path      upload (mydb context)
//	drive9 fs cp mydb:/remote/path local.txt      download
//	drive9 fs cp :/remote/a :/remote/b            server-side copy
//	drive9 fs cp - :/remote/path                  upload from stdin
//	drive9 fs cp :/remote/path -                  download to stdout
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
		return fmt.Errorf("usage: drive9 fs cp [--resume] <src> <dst>")
	}
	src, dst := args[0], args[1]

	srcRP, srcIsRemote := ParseRemote(src)
	dstRP, dstIsRemote := ParseRemote(dst)

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
	st, err := c.Stat(remotePath)
	if err != nil {
		return err
	}
	if st.IsDir {
		return fmt.Errorf("cannot download directory %s with fs cp", remotePath)
	}
	summary, err := c.DownloadToFileWithSummary(ctx, remotePath, localPath, st.Size)
	if err != nil {
		return err
	}
	emitDownloadSummary(summary, remotePath, localPath)
	return nil
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

func emitDownloadSummary(summary *client.DownloadSummary, remotePath, localPath string) {
	if summary == nil || os.Getenv("DRIVE9_DOWNLOAD_SUMMARY_STDERR") == "" {
		return
	}

	payload := struct {
		Type          string  `json:"type"`
		RemotePath    string  `json:"remote_path"`
		LocalPath     string  `json:"local_path"`
		Mode          string  `json:"mode"`
		Concurrency   int     `json:"concurrency"`
		ChunkSize     int64   `json:"chunk_size_bytes"`
		RangeCount    int     `json:"range_count"`
		StartedAt     string  `json:"started_at"`
		FinishedAt    string  `json:"finished_at"`
		ElapsedSecond float64 `json:"elapsed_seconds"`
	}{
		Type:          "download_summary",
		RemotePath:    remotePath,
		LocalPath:     localPath,
		Mode:          summary.Mode,
		Concurrency:   summary.Concurrency,
		ChunkSize:     summary.ChunkSize,
		RangeCount:    summary.RangeCount,
		StartedAt:     summary.StartedAt.Format(time.RFC3339Nano),
		FinishedAt:    summary.FinishedAt.Format(time.RFC3339Nano),
		ElapsedSecond: summary.Elapsed.Seconds(),
	}

	encoder := json.NewEncoder(os.Stderr)
	if err := encoder.Encode(payload); err != nil {
		fmt.Fprintf(os.Stderr, "download summary encode failed: %v\n", err)
	}
}
