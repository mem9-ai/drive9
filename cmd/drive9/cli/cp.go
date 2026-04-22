package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

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
//	drive9 fs cp --append tail.log :/remote/path  append local data to remote file
func Cp(c *client.Client, args []string) error {
	resume := false
	appendMode := false
	var tags map[string]string
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--resume":
			resume = true
		case a == "--append":
			appendMode = true
		case a == "--tag":
			if i+1 >= len(args) {
				return fmt.Errorf("--tag requires key=value argument")
			}
			i++
			var err error
			tags, err = parseAndMergeTag(tags, args[i])
			if err != nil {
				return err
			}
		case strings.HasPrefix(a, "--tag="):
			var err error
			tags, err = parseAndMergeTag(tags, strings.TrimPrefix(a, "--tag="))
			if err != nil {
				return err
			}
		default:
			filtered = append(filtered, a)
		}
	}
	args = filtered

	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs cp [--resume] [--append] [--tag key=value]... <src> <dst>")
	}
	if resume && appendMode {
		return fmt.Errorf("--resume and --append cannot be used together")
	}
	src, dst := args[0], args[1]

	srcRP, srcIsRemote := ParseRemote(src)
	dstRP, dstIsRemote := ParseRemote(dst)
	if appendMode && src == "-" {
		return fmt.Errorf("--append does not support stdin source; use a local file")
	}
	if appendMode && (srcIsRemote || !dstIsRemote) {
		return fmt.Errorf("--append only supports local file source to remote destination")
	}
	if len(tags) > 0 {
		if srcIsRemote || !dstIsRemote {
			return fmt.Errorf("--tag is only supported for uploads (local/stdin -> remote)")
		}
	}

	ctx := context.Background()

	switch {
	case src == "-" && dstIsRemote:
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		if appendMode {
			return c.AppendStreamWithTags(ctx, dstRP.Path, bytes.NewReader(data), int64(len(data)), printProgress, tags)
		}
		summary, err := c.WriteStreamWithSummaryAndTags(ctx, dstRP.Path, bytes.NewReader(data), int64(len(data)), printProgress, tags)
		if err != nil {
			return err
		}
		emitUploadSummary(ctx, summary, "-")
		return nil

	case srcIsRemote && dst == "-":
		return streamToStdout(ctx, c, srcRP.Path)

	case !srcIsRemote && dstIsRemote:
		if appendMode {
			return appendFileWithTags(ctx, c, src, dstRP.Path, tags)
		}
		if resume {
			return resumeUploadWithTags(ctx, c, src, dstRP.Path, tags)
		}
		return uploadFileWithTags(ctx, c, src, dstRP.Path, tags)

	case srcIsRemote && !dstIsRemote:
		return downloadFile(ctx, c, srcRP.Path, dst)

	case srcIsRemote && dstIsRemote:
		return c.Copy(srcRP.Path, dstRP.Path)

	default:
		return fmt.Errorf("at least one path must be remote (e.g. :/path or mydb:/path)")
	}
}

func parseAndMergeTag(tags map[string]string, raw string) (map[string]string, error) {
	key, value, ok := strings.Cut(raw, "=")
	if !ok {
		return nil, fmt.Errorf("invalid --tag %q (expected key=value)", raw)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("invalid --tag %q (expected key=value)", raw)
	}
	if tags == nil {
		tags = make(map[string]string)
	}
	if _, dup := tags[key]; dup {
		return nil, fmt.Errorf("duplicate --tag key %q", key)
	}
	tags[key] = value
	return tags, nil
}

func uploadFile(ctx context.Context, c *client.Client, localPath, remotePath string) error {
	return uploadFileWithTags(ctx, c, localPath, remotePath, nil)
}

func uploadFileWithTags(ctx context.Context, c *client.Client, localPath, remotePath string, tags map[string]string) error {
	f, size, err := openLocalFile(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	summary, err := c.WriteStreamWithSummaryAndTags(ctx, remotePath, f, size, printProgress, tags)
	if err != nil {
		return err
	}
	emitUploadSummary(ctx, summary, localPath)
	return nil
}

func resumeUploadWithTags(ctx context.Context, c *client.Client, localPath, remotePath string, tags map[string]string) error {
	f, size, err := openLocalFile(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	summary, err := c.ResumeUploadWithSummaryAndTags(ctx, remotePath, f, size, printProgress, tags)
	if err != nil {
		return err
	}
	emitUploadSummary(ctx, summary, localPath)
	return nil
}

func appendFileWithTags(ctx context.Context, c *client.Client, localPath, remotePath string, tags map[string]string) error {
	f, size, err := openLocalFile(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	return c.AppendStreamWithTags(ctx, remotePath, f, size, printProgress, tags)
}

func openLocalFile(localPath string) (*os.File, int64, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, 0, fmt.Errorf("open %s: %w", localPath, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("stat %s: %w", localPath, err)
	}

	return f, info.Size(), nil
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

func emitUploadSummary(ctx context.Context, summary *client.UploadSummary, localPath string) {
	if summary == nil || !logger.CLIEnabled() {
		return
	}
	logger.Info(
		ctx,
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
