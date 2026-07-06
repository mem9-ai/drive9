package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// recursiveDownloadConcurrency caps the per-leaf transfer parallelism for
// DownloadDir, mirroring the CLI's recursiveCopyConcurrency (= 16) and the
// pkg/client/transfer 16-worker convention.
const recursiveDownloadConcurrency = 16

// DownloadDir downloads an entire remote directory tree to a local
// directory. The local directory is created if it does not exist; if it
// exists it must be a directory. Any pre-existing descendant path is
// treated as a conflict and aborts the download before any file is
// written, so DownloadDir never overwrites or truncates existing local
// files. Symlinks in the remote tree are rejected.
//
// If a download fails after the preflight phase, successfully-written
// files and created directories remain on disk; cleanup is the caller's
// responsibility.
func (c *Client) DownloadDir(remoteDir, localDir string) error {
	return c.DownloadDirCtx(context.Background(), remoteDir, localDir)
}

// DownloadDirCtx is the context-aware variant of DownloadDir.
func (c *Client) DownloadDirCtx(ctx context.Context, remoteDir, localDir string) error {
	// Source must exist and be a directory.
	srcInfo, err := c.StatCtx(ctx, remoteDir)
	if err != nil {
		return fmt.Errorf("stat remote source %q: %w", remoteDir, err)
	}
	if !srcInfo.IsDir {
		return fmt.Errorf("DownloadDir requires a directory source; %q is a file", remoteDir)
	}

	// Walk remote tree via ListCtx BFS, collecting dirs + files. The
	// walk runs BEFORE we touch the local filesystem so a malformed
	// list response or transport error can't leave a partial dst dir.
	var (
		dirs  []dirRel  // relative paths to mkdir locally
		files []fileRel // relative paths to download
	)
	walkErr := walkRemoteTreeBFS(ctx, c, remoteDir, func(rel string, info FileInfo) error {
		if remoteInfoIsSymlink(info) {
			return fmt.Errorf("DownloadDir does not support symlinks (path: %s)", rel)
		}
		if info.IsDir {
			dirs = append(dirs, dirRel{rel: rel})
			return nil
		}
		files = append(files, fileRel{
			remotePath: pathpkg.Join(remoteDir, rel),
			rel:        rel,
			size:       info.Size,
		})
		return nil
	})
	if walkErr != nil {
		return walkErr
	}

	// Pre-resolve every local destination path using joinLocalSafe so
	// a malicious/malformed remote name (e.g. "../escape.txt") cannot
	// escape localDir. Done as a separate pass so we fail fast before
	// any filesystem mutation.
	dstFiles := make([]downloadEntry, len(files))
	for i, f := range files {
		localPath, joinErr := joinLocalSafe(localDir, f.rel)
		if joinErr != nil {
			return joinErr
		}
		dstFiles[i] = downloadEntry{
			remotePath: f.remotePath,
			localPath:  localPath,
			size:       f.size,
		}
	}
	dstDirs := make([]string, 0, len(dirs))
	for _, d := range dirs {
		localDirPath, joinErr := joinLocalSafe(localDir, d.rel)
		if joinErr != nil {
			return joinErr
		}
		dstDirs = append(dstDirs, localDirPath)
	}

	// Destination root preflight (local side).
	dstInfo, statErr := os.Stat(localDir)
	dstExists := statErr == nil
	switch {
	case dstExists && !dstInfo.IsDir():
		return fmt.Errorf("local destination %q exists and is not a directory", localDir)
	case dstExists && dstInfo.IsDir():
		// Accept-as-dir; do NOT recreate. Descendant preflight below
		// catches per-leaf conflicts.
	case errors.Is(statErr, os.ErrNotExist):
		// Will create after descendant preflight passes.
	default:
		return fmt.Errorf("stat local destination %q: %w", localDir, statErr)
	}

	// Descendant preflight: any pre-existing dir/file under localDir
	// is a conflict. Uses Lstat (not Stat) so a pre-existing symlink
	// also counts as a conflict — we don't want to follow a symlink
	// into someone else's directory. This protects against the
	// os.Create(localPath) truncation surface that
	// DownloadToFileWithSummary opens up.
	if err := preflightLocalDestinations(append(append([]string{}, dstDirs...), downloadPaths(dstFiles)...)); err != nil {
		return err
	}

	// Now that preflight passed, create dst root if needed.
	if !dstExists {
		if mkErr := os.MkdirAll(localDir, 0o755); mkErr != nil {
			return fmt.Errorf("create local destination %q: %w", localDir, mkErr)
		}
	}

	// Create descendant dirs in parent-before-child order. Empty dirs
	// are preserved.
	sort.Slice(dstDirs, func(i, j int) bool {
		return len(dstDirs[i]) < len(dstDirs[j])
	})
	for _, d := range dstDirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("mkdir local %q: %w", d, err)
		}
	}

	return parallelDownload(ctx, dstFiles, func(ctx context.Context, e downloadEntry) error {
		_, err := c.DownloadToFileWithSummary(ctx, e.remotePath, e.localPath, e.size)
		return err
	})
}

// walkRemoteTreeBFS walks a remote directory tree breadth-first via
// ListCtx, invoking visit(rel, info) for every entry (dirs and files)
// under root. `rel` is the slash-separated path relative to root; root
// itself is not visited. The drive9 server has no recursive readdir
// primitive, so the BFS is fully client-driven.
//
// If visit returns the sentinel errArchiveSkipDir (defined in archive.go),
// the walker skips enqueuing that directory's children — this is how
// archive pruning drops excluded subtrees (e.g. node_modules under
// --profile coding-agent) without issuing ListCtx calls for them. Any
// other non-nil error aborts the walk.
func walkRemoteTreeBFS(ctx context.Context, c *Client, root string, visit func(rel string, info FileInfo) error) error {
	queue := []string{""} // relative paths to expand; "" = root
	for len(queue) > 0 {
		rel := queue[0]
		queue = queue[1:]

		absDir := root
		if rel != "" {
			absDir = pathpkg.Join(root, rel)
		}
		entries, err := c.ListCtx(ctx, absDir)
		if err != nil {
			return fmt.Errorf("list %q: %w", absDir, err)
		}
		for _, e := range entries {
			childRel := e.Name
			if rel != "" {
				childRel = rel + "/" + e.Name
			}
			if err := visit(childRel, e); err != nil {
				if errors.Is(err, errArchiveSkipDir) {
					// Visitor asked us not to descend into this directory.
					continue
				}
				return err
			}
			if e.IsDir {
				queue = append(queue, childRel)
			}
		}
	}
	return nil
}

// joinLocalSafe joins a local base with a slash-separated relative
// segment, rejecting anything that would escape the base (`..`,
// absolute segments). This is needed because `rel` originates from
// server-supplied directory listings: a misbehaving or compromised
// server could return entry names like "../etc/passwd" and a naive
// filepath.Join would write outside localDir.
func joinLocalSafe(base, rel string) (string, error) {
	relSlash := filepath.ToSlash(rel)
	if relSlash == "" || relSlash == "." {
		return base, nil
	}
	if strings.HasPrefix(relSlash, "/") {
		return "", fmt.Errorf("relative segment must not start with /: %q", relSlash)
	}
	for _, seg := range strings.Split(relSlash, "/") {
		if seg == ".." {
			return "", fmt.Errorf("relative segment must not contain ..: %q", relSlash)
		}
	}
	joined := filepath.Join(base, filepath.FromSlash(relSlash))
	cleanBase := filepath.Clean(base)
	if cleanBase != "" && !strings.HasPrefix(joined, cleanBase+string(filepath.Separator)) && joined != cleanBase {
		return "", fmt.Errorf("computed path %q escapes base %q", joined, base)
	}
	return joined, nil
}

// preflightLocalDestinations checks every local destination path with
// os.Lstat. Any path that exists aborts the download before any
// MkdirAll/download.
func preflightLocalDestinations(paths []string) error {
	seen := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		_, err := os.Lstat(p)
		switch {
		case err == nil:
			return fmt.Errorf("local destination %q already exists; DownloadDir refuses to overwrite", p)
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return fmt.Errorf("preflight stat local %q: %w", p, err)
		}
	}
	return nil
}

// remoteInfoIsSymlink reports whether a listing entry is a symlink.
func remoteInfoIsSymlink(info FileInfo) bool {
	return info.HasMode && info.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK)
}

// parallelDownload runs op(ctx, item) for every item with bounded
// parallelism (recursiveDownloadConcurrency). Failures are collected;
// the first error is returned along with a count summary. Sibling
// transfers are NOT cancelled when a single leaf fails — they keep
// going so partial-failure semantics match the CLI recursive copy.
// The context is honored at both the top-of-loop check and the
// semaphore acquire (which can block when the worker pool is
// saturated).
func parallelDownload[T any](ctx context.Context, items []T, op func(context.Context, T) error) error {
	if len(items) == 0 {
		return nil
	}
	sem := make(chan struct{}, recursiveDownloadConcurrency)
	var (
		wg         sync.WaitGroup
		failuresMu sync.Mutex
		failures   []error
	)
	recordCancel := func(err error) {
		failuresMu.Lock()
		failures = append(failures, err)
		failuresMu.Unlock()
	}
launch:
	for _, item := range items {
		select {
		case <-ctx.Done():
			recordCancel(ctx.Err())
			break launch
		default:
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			recordCancel(ctx.Err())
			break launch
		}
		wg.Add(1)
		go func(item T) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := op(ctx, item); err != nil {
				failuresMu.Lock()
				failures = append(failures, err)
				failuresMu.Unlock()
			}
		}(item)
	}
	wg.Wait()
	if len(failures) == 0 {
		return nil
	}
	if len(failures) == 1 {
		return failures[0]
	}
	return fmt.Errorf("%d transfers failed (first: %v)", len(failures), failures[0])
}

// downloadPaths extracts just the local destination paths from a slice
// of downloadEntry. Convenience for preflight calls.
func downloadPaths(entries []downloadEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.localPath
	}
	return out
}

// ── plan types ────────────────────────────────────────────────────────

type dirRel struct {
	rel string // path relative to source root, slash-separated
}

type fileRel struct {
	remotePath string // absolute remote path of the source file
	rel        string // path relative to source root, slash-separated
	size       int64
}

// downloadEntry is the resolved (post-joinLocalSafe) form of a
// fileRel: the absolute local path has been validated as staying
// inside the dst root.
type downloadEntry struct {
	remotePath string
	localPath  string
	size       int64
}
