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

	// Preflight: batch-stat every destination path (dirs AND files).
	// If any destination exists, abort before any transfer (per #3
	// refined by @adversary-1 msg 72d6eb06).
	allDests := make([]string, 0, len(dirs)+len(files)+1)
	allDests = append(allDests, dstRemote)
	allDests = append(allDests, dirs...)
	for _, f := range files {
		allDests = append(allDests, f.remotePath)
	}
	if err := preflightDestinations(ctx, c, allDests); err != nil {
		return err
	}

	// Create dst root + intermediate dirs in path-length ASC order
	// (parent before child). Empty dirs are preserved via this
	// explicit Mkdir, per design lock #5.
	if err := mkdirRemoteTree(ctx, c, dstRemote, dirs); err != nil {
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
func copyTreeRemoteToLocal(ctx context.Context, c *client.Client, srcRemote, dstLocal string) error {
	// Source must exist and be a directory.
	srcInfo, err := c.StatCtx(ctx, srcRemote)
	if err != nil {
		return fmt.Errorf("stat remote source %q: %w", srcRemote, err)
	}
	if !srcInfo.IsDir {
		return fmt.Errorf("-r/--recursive requires a directory source; %q is a file (use `drive9 fs cp` without -r)", srcRemote)
	}

	// Destination preflight (local side): dst exists as dir → ok;
	// dst doesn't exist → create; dst exists as file → reject.
	dstInfo, err := os.Stat(dstLocal)
	switch {
	case err == nil && !dstInfo.IsDir():
		return fmt.Errorf("local destination %q exists and is not a directory", dstLocal)
	case err == nil && dstInfo.IsDir():
		// Existing local dir is fine; we'll create children inside.
		// (Local recursive copy is less strict than remote because
		// local already-existing dirs are normal Unix behavior.)
	case errors.Is(err, os.ErrNotExist):
		if mkErr := os.MkdirAll(dstLocal, 0o755); mkErr != nil {
			return fmt.Errorf("create local destination %q: %w", dstLocal, mkErr)
		}
	default:
		return fmt.Errorf("stat local destination %q: %w", dstLocal, err)
	}

	// Walk remote tree via ListCtx BFS, collecting dirs + files.
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

	// Create local dirs in parent-before-child order.
	sort.Slice(dirs, func(i, j int) bool {
		return len(dirs[i].rel) < len(dirs[j].rel)
	})
	for _, d := range dirs {
		localDir := filepath.Join(dstLocal, filepath.FromSlash(d.rel))
		if err := os.MkdirAll(localDir, 0o755); err != nil {
			return fmt.Errorf("mkdir local %q: %w", localDir, err)
		}
	}

	return parallelTransfer(ctx, files, func(ctx context.Context, e remoteFileEntry) error {
		localPath := filepath.Join(dstLocal, filepath.FromSlash(e.rel))
		return downloadOneFile(ctx, c, e.remotePath, localPath, e.size)
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

	// Preflight: dst root + every dir + every file must NOT exist.
	allDests := make([]string, 0, len(dirs)+len(files)+1)
	allDests = append(allDests, dstRemote)
	allDests = append(allDests, dirs...)
	for _, f := range files {
		allDests = append(allDests, f.dstPath)
	}
	if err := preflightDestinations(ctx, c, allDests); err != nil {
		return err
	}

	if err := mkdirRemoteTree(ctx, c, dstRemote, dirs); err != nil {
		return err
	}

	return parallelTransfer(ctx, files, func(ctx context.Context, item remoteCopyPlanItem) error {
		// Client.Copy is metadata-only server-side zero-copy; no
		// content round-trip. We don't need to pass ctx because the
		// non-Ctx variant is what's exposed today; if cancellation
		// support is added later this is the seam.
		return c.Copy(item.srcPath, item.dstPath)
	})
}

// joinRemoteSafe joins a remote base with a slash-or-platform-relative
// segment, rejecting anything that would escape the base (`..`,
// absolute segments, duplicate slashes that would let `:/foo` match
// `:/foobar`-style boundary leaks). Returns a forward-slash remote
// path suitable for the drive9 server API.
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

// mkdirRemoteTree creates the destination root and all intermediate
// dirs in path-length ASC order so parents land before children. Per
// design lock #5.
func mkdirRemoteTree(ctx context.Context, c *client.Client, root string, dirs []string) error {
	// Build full list including root, dedupe, sort by path length so
	// parents precede children.
	all := append([]string{root}, dirs...)
	seen := make(map[string]struct{}, len(all))
	unique := make([]string, 0, len(all))
	for _, d := range all {
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
	for _, item := range items {
		// Honor cancellation: stop launching new transfers if ctx
		// has been cancelled.
		select {
		case <-ctx.Done():
			failuresMu.Lock()
			failures = append(failures, ctx.Err())
			failuresMu.Unlock()
			break
		default:
		}
		sem <- struct{}{}
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

type remoteDirEntry struct {
	rel string
}

type remoteCopyPlanItem struct {
	srcPath string
	dstPath string
}
