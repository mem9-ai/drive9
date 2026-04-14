package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
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
		summary, err := c.WriteStreamWithSummary(ctx, dstRP.Path, bytes.NewReader(data), int64(len(data)), printProgress)
		if err != nil {
			return err
		}
		emitUploadSummary(summary, "-")
		return nil

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

	summary, err := c.WriteStreamWithSummary(ctx, remotePath, f, info.Size(), printProgress)
	if err != nil {
		return err
	}
	emitUploadSummary(summary, localPath)
	return nil
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

	summary, err := c.ResumeUploadWithSummary(ctx, remotePath, f, info.Size(), printProgress)
	if err != nil {
		return err
	}
	emitUploadSummary(summary, localPath)
	return nil
}

func downloadFile(ctx context.Context, c *client.Client, remotePath, localPath string) error {
	info, err := c.Stat(remotePath)
	if err != nil {
		return err
	}

	summary, err := c.DownloadToFileWithSummary(ctx, remotePath, localPath, info.Size)
	if err != nil {
		return err
	}
	emitDownloadSummary(summary)
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

// emitDownloadSummary keeps benchmark-only metadata in the same structured CLI
// log stream as the rest of the command lifecycle, instead of inventing a
// second stderr-only output contract.
func emitDownloadSummary(summary *client.DownloadSummary) {
	if summary == nil || !logger.CLIEnabled() {
		return
	}
	// The benchmark harness reads this stable event from the CLI log file.
	logger.Info(
		context.Background(),
		"download_summary",
		zap.String("type", summary.Type),
		zap.String("mode", summary.Mode),
		zap.Int("concurrency", summary.Concurrency),
		zap.Int64("chunk_size_bytes", summary.ChunkSizeBytes),
		zap.Int("range_count", summary.RangeCount),
		zap.Time("started_at", summary.StartedAt),
		zap.Time("finished_at", summary.FinishedAt),
		zap.Float64("elapsed_seconds", summary.ElapsedSeconds),
		zap.String("remote_path", summary.RemotePath),
		zap.String("local_path", summary.LocalPath),
	)
}

func emitUploadSummary(summary *client.UploadSummary, localPath string) {
	if summary == nil || !logger.CLIEnabled() {
		return
	}
	logger.Info(
		context.Background(),
		"upload_summary",
		zap.String("type", summary.Type),
		zap.String("mode", summary.Mode),
		zap.Int64("total_bytes", summary.TotalBytes),
		zap.Int64("part_size_bytes", summary.PartSizeBytes),
		zap.Int("total_parts", summary.TotalParts),
		zap.Int("uploaded_parts", summary.UploadedParts),
		zap.Int("parallelism", summary.Parallelism),
		zap.Time("started_at", summary.StartedAt),
		zap.Time("finished_at", summary.FinishedAt),
		zap.Float64("elapsed_seconds", summary.ElapsedSeconds),
		zap.Float64("query_seconds", summary.QuerySeconds),
		zap.Float64("checksum_seconds", summary.ChecksumSeconds),
		zap.Float64("initiate_seconds", summary.InitiateSeconds),
		zap.Float64("resume_seconds", summary.ResumeSeconds),
		zap.Float64("presign_seconds", summary.PresignSeconds),
		zap.Float64("upload_seconds", summary.UploadSeconds),
		zap.Float64("complete_seconds", summary.CompleteSeconds),
		zap.Float64("direct_write_seconds", summary.DirectWriteSeconds),
		zap.String("remote_path", summary.RemotePath),
		zap.String("local_path", localPath),
	)
}
