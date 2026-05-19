package cli

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

	"github.com/mem9-ai/dat9/pkg/client"
)

// cp_recursive.go implements `drive9 fs cp -r <src> <dst>` (P0 task #58
// per the db9-drive9 gap analysis). Lives next to cp.go so the
// single-file ergonomics flags and the tree-copy paths share parsing
// + error vocabulary.
//
// Design (locked in #drive9:72bf030c thread):
//   - Pure client-side: server primitives stay file-level (handleCopy,
//     WriteStreamWithSummary, DownloadToFileWithSummary, Mkdir).
//     Tree walk + concurrency happen on the client.
//   - Destination semantics: dst is treated as the directory that
//     receives the source's CONTENTS. If dst doesn't exist it is
//     created as a directory. Existing file at dst is rejected. No
//     auto-create-dst/basename(src) variant in this PR (per
//     @adversary-1 review constraint msg 72d6eb06).
//   - Symlinks: explicit reject, not silent follow/skip. Per
//     @adversary-1 msg b8603b41.
//   - Preflight: BatchStat every destination path (files AND dirs) up
//     front; if any exists, abort before any transfer. Runtime
//     failures during transfer surface back as errgroup error;
//     siblings continue but the overall return is non-nil.
//   - Path safety: joinRemoteSafe rejects "..", duplicate slashes,
//     ensures every leaf path stays under the destination root.
//   - Bounded concurrency: recursiveCopyConcurrency = 16, mirroring
//     pkg/client/transfer/ existing 16-worker convention.

// copyTreeLocalToRemote uploads a local directory tree to a remote
// destination. Existing files/dirs at the destination cause preflight
// failure; symlinks in the source tree cause walk-time failure.
func copyTreeLocalToRemote(ctx context.Context, c *client.Client, srcLocal, dstRemote string) error {
	// Source must exist and must be a directory.
	srcInfo, err := os.Lstat(srcLocal)
	if err != nil {
		return fmt.Errorf("stat source %q: %w", srcLocal, err)
	}
	if srcInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("recursive copy does not support symlinks yet (source: %s)", srcLocal)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("-r/--recursive requires a directory source; %q is a file (use `drive9 fs cp` without -r)", srcLocal)
	}

	// Walk local tree collecting dirs + files. filepath.Walk uses
	// Lstat so we detect symlinks here and refuse them, rather than
	// silently following or skipping (per design lock #2).
	var (
		dirs  []string // remote paths to create
		files []localFileEntry
	)
	walkErr := filepath.Walk(srcLocal, func(localPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			rel, _ := filepath.Rel(srcLocal, localPath)
			return fmt.Errorf("recursive copy does not support symlinks yet (path: %s)", rel)
		}
		rel, relErr := filepath.Rel(srcLocal, localPath)
		if relErr != nil {
			return fmt.Errorf("rel %q: %w", localPath, relErr)
		}
		// Skip the source root itself; we don't create the root dir
		// at dst, we treat dst as the root (per destination semantic
		// lock #1).
		if rel == "." {
			return nil
		}
		remotePath, joinErr := joinRemoteSafe(dstRemote, rel)
		if joinErr != nil {
			return joinErr
		}
		if info.IsDir() {
			dirs = append(dirs, remotePath)
			return nil
		}
		files = append(files, localFileEntry{
			localPath:  localPath,
			remotePath: remotePath,
			size:       info.Size(),
		})
		return nil
	})
	if walkErr != nil {
		return walkErr
	}

	// Destination root semantics (locked in #drive9:72bf030c thread
	// review msgs 72d6eb06 + 5f25d0a0):
	//   - dst doesn't exist        → create it, copy contents into dst/
	//   - dst exists as directory  → accept, copy contents into dst/
	//   - dst exists as a file     → reject (refuse to overwrite a file
	//                                with a directory tree)
	// Only DESCENDANT paths participate in the conflict-preflight; the
	// dst root itself is handled separately above so the common case
	// ("agent A wants to drop its scratch tree into an already-created
	// agent-B workspace dir") works.
	dstExists, dstIsDir, err := remotePathStatus(ctx, c, dstRemote)
	if err != nil {
		return err
	}
	if dstExists && !dstIsDir {
		return fmt.Errorf("remote destination %q exists and is not a directory", dstRemote)
	}

	// Preflight DESCENDANT destinations only. If dst-exists-as-dir, the
	// dst root is not in the conflict set (we explicitly allow merging
	// the source tree into the existing dst dir). Any descendant
	// conflict still aborts before any transfer, matching @adversary-1
	// #3 preflight rule.
	allDests := make([]string, 0, len(dirs)+len(files))
	allDests = append(allDests, dirs...)
	for _, f := range files {
		allDests = append(allDests, f.remotePath)
	}
	if err := preflightDestinations(ctx, c, allDests); err != nil {
		return err
	}

	// Create intermediate dirs in path-length ASC order (parent before
	// child). Empty dirs are preserved via explicit Mkdir per design
	// lock #5. Skip mkdir on dst root if it already exists; we only
	// need to ensure descendant dirs are present.
	mkdirRoots := dirs
	if !dstExists {
		mkdirRoots = append([]string{dstRemote}, dirs...)
	}
	if err := mkdirRemoteTree(ctx, c, mkdirRoots); err != nil {
		return err
	}

	// Upload files with bounded parallelism. Per-leaf failure surfaces
	// back via the failure collector; sibling transfers keep going so
	// "partial failure" semantics match @adversary-1 #3 runtime rule.
	return parallelTransfer(ctx, files, func(ctx context.Context, e localFileEntry) error {
		return uploadOneFile(ctx, c, e.localPath, e.remotePath, e.size)
	})
}

// copyTreeRemoteToLocal downloads a remote directory tree to a local
// destination. Mirrors copyTreeLocalToRemote: dst is the directory
// that will hold src's CONTENTS.
//
// Destination semantics (locked, identical to local→remote and
// remote→remote, per @adversary-1 secondary review msg c7eb6852):
//   - dst doesn't exist       → create it, copy contents into dst/
//   - dst exists as directory → accept, copy contents into dst/
//   - dst exists as a file    → reject
//
// Descendant preflight: any pre-existing path inside dst/ is a
// conflict and aborts BEFORE any os.MkdirAll or download (no
// `-f`/overwrite in P0). This protects against the
// `os.Create(localPath)` truncation surface that
// DownloadToFileWithSummary opens up.
func copyTreeRemoteToLocal(ctx context.Context, c *client.Client, srcRemote, dstLocal string) error {
	// Source must exist and be a directory.
	srcInfo, err := c.StatCtx(ctx, srcRemote)
	if err != nil {
		return fmt.Errorf("stat remote source %q: %w", srcRemote, err)
	}
	if !srcInfo.IsDir {
		return fmt.Errorf("-r/--recursive requires a directory source; %q is a file (use `drive9 fs cp` without -r)", srcRemote)
	}

	// Walk remote tree via ListCtx BFS, collecting dirs + files. The
	// walk runs BEFORE we touch the local filesystem so a malformed
	// list response or transport error can't leave a partial dst dir.
	var (
		dirs  []remoteDirEntry  // relative paths to mkdir locally
		files []remoteFileEntry // relative paths to download
	)
	walkErr := walkRemoteTreeBFS(ctx, c, srcRemote, func(rel string, info client.FileInfo) error {
		if info.IsDir {
			dirs = append(dirs, remoteDirEntry{rel: rel})
			return nil
		}
		files = append(files, remoteFileEntry{
			remotePath: pathpkg.Join(srcRemote, rel),
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
	// escape dstLocal. Done as a separate pass so we fail fast before
	// any filesystem mutation.
	dstFiles := make([]localDownloadEntry, len(files))
	for i, f := range files {
		localPath, joinErr := joinLocalSafe(dstLocal, f.rel)
		if joinErr != nil {
			return joinErr
		}
		dstFiles[i] = localDownloadEntry{
			remotePath: f.remotePath,
			localPath:  localPath,
			size:       f.size,
		}
	}
	dstDirs := make([]string, 0, len(dirs))
	for _, d := range dirs {
		localDir, joinErr := joinLocalSafe(dstLocal, d.rel)
		if joinErr != nil {
			return joinErr
		}
		dstDirs = append(dstDirs, localDir)
	}

	// Destination root preflight (local side).
	dstInfo, statErr := os.Stat(dstLocal)
	dstExists := statErr == nil
	switch {
	case dstExists && !dstInfo.IsDir():
		return fmt.Errorf("local destination %q exists and is not a directory", dstLocal)
	case dstExists && dstInfo.IsDir():
		// Accept-as-dir; do NOT recreate. Descendant preflight below
		// catches per-leaf conflicts.
	case errors.Is(statErr, os.ErrNotExist):
		// Will create after descendant preflight passes.
	default:
		return fmt.Errorf("stat local destination %q: %w", dstLocal, statErr)
	}

	// Descendant preflight: any pre-existing dir/file under dstLocal
	// is a conflict. Mirrors remote-dst preflight; descendants of an
	// accepted dst-as-dir still must not collide.
	if err := preflightLocalDestinations(append(append([]string{}, dstDirs...), localDownloadPaths(dstFiles)...)); err != nil {
		return err
	}

	// Now that preflight passed, create dst root if needed.
	if !dstExists {
		if mkErr := os.MkdirAll(dstLocal, 0o755); mkErr != nil {
			return fmt.Errorf("create local destination %q: %w", dstLocal, mkErr)
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

	return parallelTransfer(ctx, dstFiles, func(ctx context.Context, e localDownloadEntry) error {
		return downloadOneFile(ctx, c, e.remotePath, e.localPath, e.size)
	})
}

// copyTreeRemoteToRemote performs a remote→remote tree copy. Each leaf
// uses server-side zero-copy via Client.Copy (same backend, file_id
// alias), so we never round-trip file bytes through the client.
// Directories are recreated via MkdirCtx because server-side Copy is
// file-level only.
func copyTreeRemoteToRemote(ctx context.Context, c *client.Client, srcRemote, dstRemote string) error {
	srcInfo, err := c.StatCtx(ctx, srcRemote)
	if err != nil {
		return fmt.Errorf("stat remote source %q: %w", srcRemote, err)
	}
	if !srcInfo.IsDir {
		return fmt.Errorf("-r/--recursive requires a directory source; %q is a file (use `drive9 fs cp` without -r)", srcRemote)
	}

	var (
		dirs  []string             // remote dst dirs to create
		files []remoteCopyPlanItem // src+dst pairs
	)
	walkErr := walkRemoteTreeBFS(ctx, c, srcRemote, func(rel string, info client.FileInfo) error {
		dstPath, joinErr := joinRemoteSafe(dstRemote, rel)
		if joinErr != nil {
			return joinErr
		}
		if info.IsDir {
			dirs = append(dirs, dstPath)
			return nil
		}
		files = append(files, remoteCopyPlanItem{
			srcPath: pathpkg.Join(srcRemote, rel),
			dstPath: dstPath,
		})
		return nil
	})
	if walkErr != nil {
		return walkErr
	}

	// Destination root semantics (same as local→remote path):
	//   - dst doesn't exist        → create it, copy contents into dst/
	//   - dst exists as directory  → accept, copy contents into dst/
	//   - dst exists as a file     → reject
	dstExists, dstIsDir, err := remotePathStatus(ctx, c, dstRemote)
	if err != nil {
		return err
	}
	if dstExists && !dstIsDir {
		return fmt.Errorf("remote destination %q exists and is not a directory", dstRemote)
	}

	// Preflight descendants only; dst root is allowed to pre-exist
	// as a directory.
	allDests := make([]string, 0, len(dirs)+len(files))
	allDests = append(allDests, dirs...)
	for _, f := range files {
		allDests = append(allDests, f.dstPath)
	}
	if err := preflightDestinations(ctx, c, allDests); err != nil {
		return err
	}

	mkdirRoots := dirs
	if !dstExists {
		mkdirRoots = append([]string{dstRemote}, dirs...)
	}
	if err := mkdirRemoteTree(ctx, c, mkdirRoots); err != nil {
		return err
	}

	return parallelTransfer(ctx, files, func(ctx context.Context, item remoteCopyPlanItem) error {
		// Client.CopyCtx is metadata-only server-side zero-copy with
		// context support added in this PR (see pkg/client/client.go),
		// so r→r honors Ctrl+C just like the local↔remote paths.
		return c.CopyCtx(ctx, item.srcPath, item.dstPath)
	})
}

// joinRemoteSafe joins a remote base with a slash-or-platform-relative
// segment, rejecting anything that would escape the base (`..`,
// absolute segments). Duplicate / redundant slashes inside `rel` are
// normalized by path.Clean rather than rejected — that is intentional;
// the contract is "result must be inside base", not "rel must be
// canonical". Returns a forward-slash remote path suitable for the
// drive9 server API.
func joinRemoteSafe(base, rel string) (string, error) {
	relSlash := filepath.ToSlash(rel)
	if relSlash == "" || relSlash == "." {
		return base, nil
	}
	// Reject absolute children — relative is required by definition.
	if strings.HasPrefix(relSlash, "/") {
		return "", fmt.Errorf("relative segment must not start with /: %q", relSlash)
	}
	// Reject `..` anywhere in the relative segments. We can't trust
	// pathpkg.Clean to eliminate `..` (it would resolve them away,
	// potentially escaping base if the rel is `../sibling`).
	for _, seg := range strings.Split(relSlash, "/") {
		if seg == ".." {
			return "", fmt.Errorf("relative segment must not contain ..: %q", relSlash)
		}
	}
	// Normalize duplicate slashes / dots via pathpkg.Clean on the
	// concatenated path; base is already canonical at this point.
	cleanBase := strings.TrimSuffix(base, "/")
	joined := pathpkg.Clean(cleanBase + "/" + relSlash)
	// Final boundary check: result must still be inside the base.
	// `pathpkg.Clean` cannot escape because we already rejected ".."
	// above, but be explicit so future refactors don't drift.
	if cleanBase != "" && !strings.HasPrefix(joined, cleanBase+"/") && joined != cleanBase {
		return "", fmt.Errorf("computed path %q escapes base %q", joined, base)
	}
	return joined, nil
}

// joinLocalSafe is the local-filesystem counterpart of joinRemoteSafe.
// It accepts a slash-separated relative path (as produced by the
// remote BFS) and returns the corresponding absolute local path, but
// only if that path stays under `base`. Absolute rels, "..", and
// post-Clean escapes are all rejected.
//
// The remote→local copy direction needs this because `rel` originates
// from server-supplied directory listings: a misbehaving or compromised
// server could return entry names like "../etc/passwd" and naively
// `filepath.Join(dstLocal, ...)` would write outside dstLocal.
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
	// Build the local path using filepath semantics. Resolve to a
	// canonical form and then verify it still has `base` as a prefix.
	joined := filepath.Join(base, filepath.FromSlash(relSlash))
	// Re-canonicalize base the same way Join would so the prefix
	// comparison is apples-to-apples on platforms that normalize
	// (e.g. trailing-slash on POSIX is dropped by Clean).
	cleanBase := filepath.Clean(base)
	if cleanBase != "" && !strings.HasPrefix(joined, cleanBase+string(filepath.Separator)) && joined != cleanBase {
		return "", fmt.Errorf("computed path %q escapes base %q", joined, base)
	}
	return joined, nil
}

// preflightLocalDestinations checks every local destination path with
// os.Lstat (Lstat not Stat, so a pre-existing symlink also counts as
// a conflict — we don't want to follow a symlink into someone else's
// directory). Any path that exists aborts the copy before any
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
			return fmt.Errorf("local destination %q already exists; recursive copy refuses to overwrite", p)
		case errors.Is(err, os.ErrNotExist):
			continue
		default:
			return fmt.Errorf("preflight stat local %q: %w", p, err)
		}
	}
	return nil
}

// localDownloadPaths extracts just the local destination paths from a
// slice of localDownloadEntry. Convenience for preflight calls.
func localDownloadPaths(entries []localDownloadEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.localPath
	}
	return out
}

// preflightDestinations stats every destination path in batches of
// MaxBatchStatPaths (256) and fails if any exists. Catches both
// file and directory conflicts before any transfer/mkdir.
func preflightDestinations(ctx context.Context, c *client.Client, paths []string) error {
	// Dedupe — multiple dirs may share parent paths via Mkdir's
	// behavior, and the dst root appears once explicitly.
	seen := make(map[string]struct{}, len(paths))
	unique := make([]string, 0, len(paths))
	for _, p := range paths {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		unique = append(unique, p)
	}

	const batchSize = client.MaxBatchStatPaths // 256
	for start := 0; start < len(unique); start += batchSize {
		end := start + batchSize
		if end > len(unique) {
			end = len(unique)
		}
		results, err := c.BatchStatCtx(ctx, unique[start:end])
		if err != nil {
			return fmt.Errorf("preflight batch stat: %w", err)
		}
		for _, r := range results {
			// BatchStatResult.Status mirrors the per-path HTTP code:
			// 200 = exists, 404 = missing. Any 2xx is a conflict for
			// recursive copy; anything else (5xx, etc.) is surfaced
			// as a preflight failure so we don't silently proceed.
			switch {
			case r.Status == 404:
				continue
			case r.Status >= 200 && r.Status < 300:
				return fmt.Errorf("destination %q already exists; recursive copy refuses to overwrite", r.Path)
			default:
				return fmt.Errorf("preflight stat %q failed (status %d): %s", r.Path, r.Status, r.Error)
			}
		}
	}
	return nil
}

// mkdirRemoteTree creates the supplied dirs in path-length ASC order
// so parents land before children. Per design lock #5. Callers
// pre-include the dst root if it needs creating; mkdirRemoteTree
// itself just sorts + dedupes + issues MkdirCtx per entry.
func mkdirRemoteTree(ctx context.Context, c *client.Client, dirs []string) error {
	seen := make(map[string]struct{}, len(dirs))
	unique := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		unique = append(unique, d)
	}
	sort.Slice(unique, func(i, j int) bool {
		return len(unique[i]) < len(unique[j])
	})
	for _, d := range unique {
		if err := c.MkdirCtx(ctx, d, 0o755); err != nil {
			return fmt.Errorf("mkdir %q: %w", d, err)
		}
	}
	return nil
}

// remotePathStatus returns (exists, isDir, error) for a remote path
// via StatCtx. A 404 maps to (false, false, nil) so callers can
// distinguish "missing" from "exists" without parsing the StatusError
// at the call site. Any other transport/auth error is surfaced.
func remotePathStatus(ctx context.Context, c *client.Client, path string) (bool, bool, error) {
	info, err := c.StatCtx(ctx, path)
	if err == nil {
		return true, info.IsDir, nil
	}
	if client.IsNotFound(err) {
		return false, false, nil
	}
	return false, false, fmt.Errorf("stat %q: %w", path, err)
}

// walkRemoteTreeBFS walks a remote directory tree breadth-first via
// Client.ListCtx, invoking visit(rel, info) for every entry (dirs and
// files) under root. `rel` is the slash-separated path relative to
// root; root itself is not visited. Per the @adversary-1 lane 2 facts
// (msg a15bc453) the BFS is fully client-driven because the drive9
// server has no recursive readdir primitive.
func walkRemoteTreeBFS(ctx context.Context, c *client.Client, root string, visit func(rel string, info client.FileInfo) error) error {
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
				return err
			}
			if e.IsDir {
				queue = append(queue, childRel)
			}
		}
	}
	return nil
}

// uploadOneFile streams a single local file to the remote destination.
// Wraps WriteStreamWithSummary so per-leaf transfer reuses the
// existing multipart upload path.
func uploadOneFile(ctx context.Context, c *client.Client, localPath, remotePath string, size int64) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %q: %w", localPath, err)
	}
	defer func() { _ = f.Close() }()
	// Recursive copy doesn't pipe per-leaf progress to the CLI
	// stderr — the bounded parallelism makes the part counters
	// interleave unreadably. Future ergonomics PR can add a tree
	// progress bar.
	_, err = c.WriteStreamWithSummary(ctx, remotePath, f, size, nil)
	return err
}

// downloadOneFile fetches a single remote file to the given local
// path. Wraps DownloadToFileWithSummary for the same reason as
// uploadOneFile.
func downloadOneFile(ctx context.Context, c *client.Client, remotePath, localPath string, size int64) error {
	_, err := c.DownloadToFileWithSummary(ctx, remotePath, localPath, size)
	return err
}

// parallelTransfer runs `op(ctx, item)` for every item in `items` with
// bounded parallelism (recursiveCopyConcurrency). Failures are
// collected; the first error is returned along with a count summary.
// Concurrent calls are cancelled when the context is cancelled, but
// sibling transfers are NOT cancelled when a single leaf fails — this
// matches the @adversary-1 #3 runtime rule (siblings continue after
// per-leaf runtime failure).
//
// Cancellation correctness (per @adversary-1 secondary review B5):
//   - There are TWO blocking points where ctx must be honored: the
//     top-of-loop "have we already been cancelled?" check, AND the
//     semaphore acquire (which can block arbitrarily long if all
//     workers are busy). Both use ctx-aware selects.
//   - `break` inside `select` only exits the select, not the for, so
//     a labeled break exits the whole loop. (Bug from round-1 PR review.)
func parallelTransfer[T any](ctx context.Context, items []T, op func(context.Context, T) error) error {
	if len(items) == 0 {
		return nil
	}
	sem := make(chan struct{}, recursiveCopyConcurrency)
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
		// (1) Already-cancelled check. Cheap; lets us bail out before
		// even waiting for a semaphore slot if ctx is already done.
		select {
		case <-ctx.Done():
			recordCancel(ctx.Err())
			break launch
		default:
		}
		// (2) Semaphore acquire. This can block when the worker pool
		// is saturated; without the ctx case the launcher would still
		// wait for a worker to finish even after ctx is cancelled.
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

// ── plan types ────────────────────────────────────────────────────────

type localFileEntry struct {
	localPath  string
	remotePath string
	size       int64
}

type remoteFileEntry struct {
	remotePath string // absolute remote path of the source file
	rel        string // path relative to source root, slash-separated
	size       int64
}

// localDownloadEntry is the resolved (post-joinLocalSafe) form of a
// remoteFileEntry: the absolute local path has been validated as
// staying inside the dst root.
type localDownloadEntry struct {
	remotePath string
	localPath  string
	size       int64
}

type remoteDirEntry struct {
	rel string
}

type remoteCopyPlanItem struct {
	srcPath string
	dstPath string
}
