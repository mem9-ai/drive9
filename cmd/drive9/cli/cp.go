package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/tagutil"
	"go.uber.org/zap"
)

// recursiveCopyConcurrency caps the per-leaf transfer parallelism for
// `drive9 fs cp -r`. Mirrors the existing `pkg/client/transfer/`
// 16-worker convention so we don't introduce a new tuning knob in
// this PR.
const recursiveCopyConcurrency = 16

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
	recursive := false
	layerRef := ""
	var tags map[string]string
	var description string
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--resume":
			resume = true
		case a == "--append":
			appendMode = true
		case a == "-r" || a == "--recursive":
			recursive = true
		case a == "--layer":
			if i+1 >= len(args) {
				return fmt.Errorf("--layer requires argument")
			}
			i++
			layerRef = strings.TrimSpace(args[i])
		case strings.HasPrefix(a, "--layer="):
			layerRef = strings.TrimSpace(strings.TrimPrefix(a, "--layer="))
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
		case a == "--description":
			if i+1 >= len(args) {
				return fmt.Errorf("--description requires argument")
			}
			i++
			description = args[i]
		default:
			filtered = append(filtered, args[i])
		}
	}
	args = filtered

	if utf8.RuneCountInString(description) > backend.MaxDescriptionLen {
		return fmt.Errorf("description exceeds %d characters", backend.MaxDescriptionLen)
	}

	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs cp [-r|--recursive] [--resume] [--append] [--tag key=value]... [--description <text>] <src> <dst>")
	}
	if resume && appendMode {
		return fmt.Errorf("--resume and --append cannot be used together")
	}
	if layerRef != "" {
		if recursive {
			return layerRefMustBeEmpty(layerRef, "-r/--recursive")
		}
		if appendMode {
			return layerRefMustBeEmpty(layerRef, "--append")
		}
		if resume {
			return layerRefMustBeEmpty(layerRef, "--resume")
		}
		if len(tags) > 0 {
			return layerRefMustBeEmpty(layerRef, "--tag")
		}
		if description != "" {
			return layerRefMustBeEmpty(layerRef, "--description")
		}
	}
	if appendMode && len(tags) > 0 {
		return fmt.Errorf("--append and --tag cannot be used together")
	}
	if appendMode && description != "" {
		return fmt.Errorf("--append and --description cannot be used together")
	}
	if resume && description != "" {
		return fmt.Errorf("--resume and --description cannot be used together")
	}
	// -r is intentionally orthogonal to the single-file ergonomics
	// flags. Tree copy doesn't compose with --append (per-file
	// append semantics ambiguous), --resume (resume is single-file
	// multipart upload state, not tree-level), or single-file
	// description/tags (those apply to one upload only). Reject up
	// front instead of silently picking which file gets the metadata.
	if recursive && (appendMode || resume || description != "" || len(tags) > 0) {
		return fmt.Errorf("-r/--recursive cannot be combined with --append, --resume, --tag, or --description")
	}
	src, dst := args[0], args[1]

	srcLoc, err := Parse(src)
	if err != nil {
		return err
	}
	dstLoc, err := Parse(dst)
	if err != nil {
		return err
	}
	if srcLoc.Kind == KindObject || dstLoc.Kind == KindObject {
		if layerRef != "" {
			return fmt.Errorf("--layer is drive9-only")
		}
		if appendMode {
			return fmt.Errorf("--append is only supported for uploads to drive9")
		}
		if resume {
			return fmt.Errorf("--resume is not supported for object-store URIs")
		}
		if dstLoc.Kind != KindDrive9 && (len(tags) > 0 || description != "") {
			return fmt.Errorf("--tag/--description are only supported for uploads to drive9")
		}
		return cpViaBackend(context.Background(), c, srcLoc, dstLoc, recursive, tags, description)
	}
	// D14: cross-context drive9 copy streams through the laptop.
	if srcLoc.Kind == KindDrive9 && dstLoc.Kind == KindDrive9 &&
		srcLoc.Context != dstLoc.Context {
		if layerRef != "" {
			return fmt.Errorf("--layer is not supported for cross-context copy")
		}
		if appendMode {
			return fmt.Errorf("--append is not supported for cross-context copy")
		}
		if resume {
			return fmt.Errorf("--resume is not supported for cross-context copy")
		}
		return cpViaBackend(context.Background(), c, srcLoc, dstLoc, recursive, tags, description)
	}

	if srcLoc.Scheme == SchemeFile && srcLoc.Local != "" {
		src = srcLoc.Local
	}
	if dstLoc.Scheme == SchemeFile && dstLoc.Local != "" {
		dst = dstLoc.Local
	}
	srcRP, srcIsRemote := locationAsRemotePath(srcLoc)
	dstRP, dstIsRemote := locationAsRemotePath(dstLoc)
	if srcLoc.Kind == KindStdin {
		srcIsRemote = false
	}
	if dstLoc.Kind == KindStdin {
		dstIsRemote = false
	}
	if description != "" {
		descriptionSupported := dstIsRemote && !appendMode && !resume && (src == "-" || !srcIsRemote)
		if !descriptionSupported {
			return fmt.Errorf("--description is only supported for local/stdin uploads to a remote path")
		}
	}
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

	// Recursive tree copy. Stdin/stdout sources have no meaningful
	// recursive semantics; reject up front rather than silently fall
	// back to the single-file paths below.
	if recursive {
		if src == "-" || dst == "-" {
			return fmt.Errorf("-r/--recursive does not support stdin/stdout endpoints")
		}
		switch {
		case !srcIsRemote && dstIsRemote:
			if dstRP.Context != "" {
				var err error
				c, err = newFSClientForContext(dstRP.Context)
				if err != nil {
					return err
				}
			}
			return copyTreeLocalToRemote(ctx, c, src, dstRP.Path)
		case srcIsRemote && !dstIsRemote:
			if srcRP.Context != "" {
				var err error
				c, err = newFSClientForContext(srcRP.Context)
				if err != nil {
					return err
				}
			}
			return copyTreeRemoteToLocal(ctx, c, srcRP.Path, dst)
		case srcIsRemote && dstIsRemote:
			switch {
			case srcRP.Context == "" && dstRP.Context == "":
			case srcRP.Context != "" && dstRP.Context != "" && srcRP.Context == dstRP.Context:
				var err error
				c, err = newFSClientForContext(srcRP.Context)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("cross-context copy not supported: %q -> %q", src, dst)
			}
			return copyTreeRemoteToRemote(ctx, c, srcRP.Path, dstRP.Path)
		default:
			return fmt.Errorf("-r/--recursive requires at least one remote path")
		}
	}

	switch {
	case src == "-" && dstIsRemote:
		if err := requireNoLayerWithRemoteContext(layerRef, dstRP, dst); err != nil {
			return err
		}
		if dstRP.Context != "" {
			var err error
			c, err = newFSClientForContext(dstRP.Context)
			if err != nil {
				return err
			}
		}
		resolvedDst, err := resolveRemoteCopyTarget(ctx, c, dstRP.Path, "")
		if err != nil {
			return err
		}
		data, err := readAllStdin()
		if err != nil {
			return err
		}
		if layerRef != "" {
			return uploadBytesToLayer(ctx, c, layerRef, resolvedDst, data, 0o644, true)
		}
		if appendMode {
			return c.AppendStream(ctx, resolvedDst, bytes.NewReader(data), int64(len(data)), printProgress)
		}
		var opts []client.WriteOption
		if tags != nil {
			opts = append(opts, client.WithTags(tags))
		}
		if description != "" {
			opts = append(opts, client.WithDescription(description))
		}
		summary, err := c.WriteStreamWithSummary(ctx, resolvedDst, bytes.NewReader(data), int64(len(data)), printProgress, opts...)
		if err != nil {
			return err
		}
		emitUploadSummary(ctx, summary, "-")
		return nil

	case srcIsRemote && dst == "-":
		if srcRP.Context != "" {
			var err error
			c, err = newFSClientForContext(srcRP.Context)
			if err != nil {
				return err
			}
		}
		return streamToStdout(ctx, c, srcRP.Path)

	case !srcIsRemote && dstIsRemote:
		if err := requireNoLayerWithRemoteContext(layerRef, dstRP, dst); err != nil {
			return err
		}
		if dstRP.Context != "" {
			var err error
			c, err = newFSClientForContext(dstRP.Context)
			if err != nil {
				return err
			}
		}
		resolvedDst, err := resolveRemoteCopyTarget(ctx, c, dstRP.Path, filepath.Base(src))
		if err != nil {
			return err
		}
		if layerRef != "" {
			return uploadLocalFileToLayer(ctx, c, layerRef, src, resolvedDst)
		}
		if appendMode {
			return appendFile(ctx, c, src, resolvedDst)
		}
		if resume {
			return resumeUploadWithTags(ctx, c, src, resolvedDst, tags)
		}
		return uploadFileWithTagsAndDescription(ctx, c, src, resolvedDst, tags, description)

	case srcIsRemote && !dstIsRemote:
		if layerRef != "" {
			return fmt.Errorf("--layer is only supported for uploads or remote-to-remote copy targets")
		}
		if srcRP.Context != "" {
			var err error
			c, err = newFSClientForContext(srcRP.Context)
			if err != nil {
				return err
			}
		}
		resolvedDst, err := resolveLocalCopyTarget(dst, pathpkg.Base(srcRP.Path))
		if err != nil {
			return err
		}
		return downloadFile(ctx, c, srcRP.Path, resolvedDst)

	case srcIsRemote && dstIsRemote:
		if layerRef != "" {
			if err := requireNoLayerWithRemoteContext(layerRef, srcRP, src); err != nil {
				return err
			}
			if err := requireNoLayerWithRemoteContext(layerRef, dstRP, dst); err != nil {
				return err
			}
		}
		switch {
		case srcRP.Context == "" && dstRP.Context == "":
		case srcRP.Context != "" && dstRP.Context != "" && srcRP.Context == dstRP.Context:
			var err error
			c, err = newFSClientForContext(srcRP.Context)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("cross-context copy not supported: %q -> %q", src, dst)
		}
		resolvedDst, err := resolveRemoteCopyTarget(ctx, c, dstRP.Path, pathpkg.Base(srcRP.Path))
		if err != nil {
			return err
		}
		if layerRef != "" {
			data, err := c.ReadCtx(ctx, srcRP.Path)
			if err != nil {
				return err
			}
			mode := uint32(0)
			hasMode := false
			if stat, err := c.StatCtx(ctx, srcRP.Path); err == nil && stat.HasMode {
				mode = stat.Mode & 0o777
				hasMode = true
			}
			return uploadBytesToLayer(ctx, c, layerRef, resolvedDst, data, mode, hasMode)
		}
		return c.Copy(srcRP.Path, resolvedDst)

	default:
		return fmt.Errorf("at least one path must be remote (e.g. :/path or mydb:/path)")
	}
}

func resolveRemoteCopyTarget(ctx context.Context, c *client.Client, remotePath, sourceBase string) (string, error) {
	if strings.HasSuffix(remotePath, "/") {
		if sourceBase == "" {
			return "", fmt.Errorf("destination %q requires a file name", remotePath)
		}
		return pathpkg.Join(remotePath, sourceBase), nil
	}
	info, err := c.StatCtx(ctx, remotePath)
	if err == nil && info.IsDir {
		if sourceBase == "" {
			return "", fmt.Errorf("destination %q requires a file name", remotePath)
		}
		return pathpkg.Join(remotePath, sourceBase), nil
	}
	if err != nil && !client.IsNotFound(err) {
		return "", err
	}
	return remotePath, nil
}

func resolveLocalCopyTarget(localPath, sourceBase string) (string, error) {
	if strings.HasSuffix(localPath, string(os.PathSeparator)) || strings.HasSuffix(localPath, "/") {
		if sourceBase == "" {
			return "", fmt.Errorf("destination %q requires a file name", localPath)
		}
		return filepath.Join(localPath, sourceBase), nil
	}
	info, err := os.Stat(localPath)
	if err == nil && info.IsDir() {
		if sourceBase == "" {
			return "", fmt.Errorf("destination %q requires a file name", localPath)
		}
		return filepath.Join(localPath, sourceBase), nil
	}
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("stat %s: %w", localPath, err)
	}
	return localPath, nil
}

func uploadFile(ctx context.Context, c *client.Client, localPath, remotePath string, description string) error {
	return uploadFileWithTagsAndDescription(ctx, c, localPath, remotePath, nil, description)
}

func parseAndMergeTag(tags map[string]string, raw string) (map[string]string, error) {
	key, value, ok := strings.Cut(raw, "=")
	if !ok {
		return nil, fmt.Errorf("invalid --tag %q (expected key=value)", raw)
	}
	if key == "" {
		return nil, fmt.Errorf("invalid --tag %q (empty key)", raw)
	}
	if err := tagutil.ValidateEntry(key, value); err != nil {
		return nil, err
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

func uploadFileWithTagsAndDescription(ctx context.Context, c *client.Client, localPath, remotePath string, tags map[string]string, description string) error {
	f, size, err := openLocalFile(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var opts []client.WriteOption
	if tags != nil {
		opts = append(opts, client.WithTags(tags))
	}
	if description != "" {
		opts = append(opts, client.WithDescription(description))
	}
	summary, err := c.WriteStreamWithSummary(ctx, remotePath, f, size, printProgress, opts...)
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

func appendFile(ctx context.Context, c *client.Client, localPath, remotePath string) error {
	f, size, err := openLocalFile(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	return c.AppendStream(ctx, remotePath, f, size, printProgress)
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

func cpViaBackend(ctx context.Context, defaultClient *client.Client, src, dst Location, recursive bool, tags map[string]string, description string) error {
	if src.Kind == KindStdin && dst.Kind == KindStdin {
		return fmt.Errorf("cannot copy stdin to stdout via stream copy")
	}
	if dst.Kind != KindStdin && dst.Query[QueryVersionID] != "" {
		return fmt.Errorf("versionId is only valid on a copy source")
	}
	if recursive && (src.Kind == KindStdin || dst.Kind == KindStdin) {
		return fmt.Errorf("-r/--recursive does not support stdin/stdout endpoints")
	}

	srcH, err := fsHandleForArgOpts(defaultClient, src.Raw, true)
	if err != nil {
		return err
	}
	if src.Kind == KindStdin {
		srcH.Loc = src
	}

	if dst.Kind == KindStdin {
		rc, err := srcH.Backend.OpenRead(ctx, srcH.Loc)
		if err != nil {
			return err
		}
		defer func() { _ = rc.Close() }()
		_, err = io.Copy(os.Stdout, rc)
		return err
	}

	dstH, err := fsHandleForArgOpts(defaultClient, dst.Raw, true)
	if err != nil {
		return err
	}

	if recursive {
		return cpTreeViaBackend(ctx, srcH, dstH)
	}

	if src.Kind == KindStdin {
		if dstH.Loc.DirHint || strings.HasSuffix(dstH.Loc.Path, "/") || strings.HasSuffix(dstH.Loc.Local, string(filepath.Separator)) {
			return fmt.Errorf("destination %q is a directory; supply a file name", dst.Raw)
		}
		wh, err := dstH.Backend.OpenWrite(ctx, dstH.Loc, WriteOpts{
			Size:        -1,
			Overwrite:   true,
			Tags:        tags,
			Description: description,
		})
		if err != nil {
			return err
		}
		if _, err := io.Copy(wh, os.Stdin); err != nil {
			_ = wh.Abort(ctx)
			return err
		}
		return wh.Close()
	}

	info, err := srcH.Backend.Stat(ctx, srcH.Loc)
	if err != nil {
		return err
	}
	if info.IsDir {
		return fmt.Errorf("source %q is a directory (use -r)", src.Raw)
	}
	finalDst := dstH.Loc
	if dstH.Loc.DirHint || strings.HasSuffix(dstH.Loc.Path, "/") || strings.HasSuffix(dstH.Loc.Local, "/") {
		finalDst = joinDest(dstH.Loc, copyLeafName(srcH.Loc))
	} else {
		st, err := dstH.Backend.Stat(ctx, dstH.Loc)
		if err == nil && st.IsDir {
			finalDst = joinDest(dstH.Loc, copyLeafName(srcH.Loc))
		}
	}
	return Copy(ctx, srcH.Backend, dstH.Backend, srcH.Loc, finalDst, info, CopyOpts{
		Overwrite:   true,
		Tags:        tags,
		Description: description,
	})
}

func copyLeafName(src Location) string {
	if src.Kind == KindLocal && src.Local != "" {
		return filepath.Base(src.Local)
	}
	p := src.Path
	if p == "" {
		p = src.Local
	}
	return pathpkg.Base(strings.TrimSuffix(p, "/"))
}

// joinDest appends rel under dst. KindLocal uses Local/filepath; other
// kinds use Path with slash separators.
func joinDest(dst Location, rel string) Location {
	out := dst
	out.DirHint = false
	if dst.Kind == KindLocal {
		base := dst.Local
		if base == "" {
			base = dst.Path
		}
		out.Local = filepath.Join(base, filepath.FromSlash(rel))
		out.Path = ""
		return out
	}
	if dst.Path == "" {
		out.Path = rel
	} else {
		out.Path = strings.TrimSuffix(dst.Path, "/") + "/" + rel
	}
	return out
}

func walkRoot(h fsHandle) string {
	if h.Path != "" {
		return h.Path
	}
	if h.Loc.Local != "" {
		return h.Loc.Local
	}
	return h.Loc.Path
}

func cpTreeViaBackend(ctx context.Context, srcH, dstH fsHandle) error {
	st, err := srcH.Backend.Stat(ctx, srcH.Loc)
	if err != nil {
		return err
	}
	if !st.IsDir {
		return fmt.Errorf("source %q is not a directory", srcH.Loc.Raw)
	}
	if dstSt, err := dstH.Backend.Stat(ctx, dstH.Loc); err == nil && !dstSt.IsDir {
		return fmt.Errorf("destination %q is not a directory", dstH.Loc.Raw)
	}
	root := strings.TrimSuffix(walkRoot(srcH), string(filepath.Separator))
	root = strings.TrimSuffix(root, "/")
	return Walk(ctx, srcH.Backend, srcH.Loc, func(info FileInfo) error {
		var rel string
		if srcH.Loc.Kind == KindLocal {
			rel, err = filepath.Rel(root, info.Path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
		} else {
			rel = strings.TrimPrefix(info.Path, root)
			rel = strings.TrimPrefix(rel, "/")
		}
		if rel == "" || rel == "." {
			return nil
		}
		child := joinDest(dstH.Loc, rel)
		child.DirHint = info.IsDir
		if info.IsDir {
			return dstH.Backend.Mkdir(ctx, child)
		}
		srcChild := srcH.Loc
		srcChild.Path = info.Path
		srcChild.DirHint = false
		return Copy(ctx, srcH.Backend, dstH.Backend, srcChild, child, &info, CopyOpts{Overwrite: true})
	})
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
