// Package gitcache provides local, rebuildable caches for Drive9 git
// fast-clone workspaces.
package gitcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const hydrateStatusSuccess = "success"
const hydrateObjectLockStaleAfter = 2 * time.Hour
const hydrateHashObjectBatchSize = 256

// HydrateOptions describes one local clean tree cache hydration.
type HydrateOptions struct {
	LocalRoot   string
	WorkspaceID string
	Commit      string
	RepoURL     string
	GitDir      string
	Token       string
	HTTPClient  *http.Client
	TreeEntries []HydrateTreeEntry
}

// HydrateTreeEntry is the manifest subset needed to hydrate clean Git objects
// from the materialized clean tree cache.
type HydrateTreeEntry struct {
	Path      string
	Kind      string
	Mode      string
	ObjectSHA string
}

// HydrateResult summarizes a hydrate attempt.
type HydrateResult struct {
	Status          string        `json:"status"`
	Provider        string        `json:"provider"`
	TreeStatus      string        `json:"tree_status,omitempty"`
	ObjectStatus    string        `json:"object_status,omitempty"`
	Files           int64         `json:"files"`
	Bytes           int64         `json:"bytes"`
	Objects         int64         `json:"objects"`
	ObjectBytes     int64         `json:"object_bytes"`
	ObjectSkipped   int64         `json:"object_skipped"`
	ObjectMismatch  int64         `json:"object_mismatch"`
	ObjectFallbacks int64         `json:"object_fallbacks"`
	ObjectDuration  time.Duration `json:"object_duration,omitempty"`
	Duration        time.Duration `json:"duration"`
	Error           string        `json:"error,omitempty"`
}

// CacheRoot returns the root for one workspace/commit's local git cache.
func CacheRoot(localRoot, workspaceID, commit string) string {
	return filepath.Join(localRoot, "git-workspaces", safePathSegment(workspaceID), safePathSegment(commit))
}

// TreeRoot returns the materialized clean-tree cache root.
func TreeRoot(localRoot, workspaceID, commit string) string {
	return filepath.Join(CacheRoot(localRoot, workspaceID, commit), "tree")
}

// BlobPath returns the content-addressed local blob cache path.
func BlobPath(localRoot, workspaceID, commit, objectSHA string) (string, error) {
	if err := validateObjectSHA(objectSHA); err != nil {
		return "", err
	}
	return filepath.Join(CacheRoot(localRoot, workspaceID, commit), "blobs", objectSHA[:2], objectSHA), nil
}

// HydrateLogPath returns the background hydrate log path.
func HydrateLogPath(localRoot, workspaceID, commit string) string {
	return filepath.Join(CacheRoot(localRoot, workspaceID, commit), "hydrate.log")
}

// ReadTreeFile reads a materialized clean tree file or symlink. The boolean is
// false when the materialized path is simply absent.
func ReadTreeFile(ctx context.Context, localRoot, workspaceID, commit, rel string, offset, size int64) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	p, err := treeFilePath(localRoot, workspaceID, commit, rel)
	if err != nil {
		return nil, false, err
	}
	info, err := os.Lstat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if info.IsDir() {
		return nil, true, syscall.EISDIR
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		target, err := os.Readlink(p)
		if err != nil {
			return nil, true, err
		}
		return SliceRead([]byte(target), offset, size), true, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = f.Close() }()
	return readFileRange(f, info.Size(), offset, size)
}

// StatTreeFile returns the size of a materialized clean tree file or symlink.
// The boolean is false when the materialized path is absent or not file-like.
func StatTreeFile(ctx context.Context, localRoot, workspaceID, commit, rel string) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	p, err := treeFilePath(localRoot, workspaceID, commit, rel)
	if err != nil {
		return 0, false, err
	}
	info, err := os.Lstat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if info.IsDir() {
		return 0, false, nil
	}
	return info.Size(), true, nil
}

// ReadBlob reads a cached blob.
func ReadBlob(ctx context.Context, localRoot, workspaceID, commit, objectSHA string, offset, size int64) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	p, err := BlobPath(localRoot, workspaceID, commit, objectSHA)
	if err != nil {
		return nil, false, err
	}
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = f.Close() }()
	return readFileRange(f, info.Size(), offset, size)
}

// StatBlob returns the size of a cached blob. The boolean is false when the
// blob is absent or not a regular file.
func StatBlob(ctx context.Context, localRoot, workspaceID, commit, objectSHA string) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	p, err := BlobPath(localRoot, workspaceID, commit, objectSHA)
	if err != nil {
		return 0, false, err
	}
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if !info.Mode().IsRegular() {
		return 0, false, nil
	}
	return info.Size(), true, nil
}

// WriteBlob writes a blob cache entry atomically. Existing entries are kept.
func WriteBlob(ctx context.Context, localRoot, workspaceID, commit, objectSHA string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := BlobPath(localRoot, workspaceID, commit, objectSHA)
	if err != nil {
		return err
	}
	if _, err := os.Stat(p); err == nil {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeFileAtomic(p, data, 0o644)
}

// Hydrate materializes a workspace clean tree into the local cache and, when
// GitDir and TreeEntries are provided, fills the local Git object database from
// that tree cache.
func Hydrate(ctx context.Context, opts HydrateOptions) (HydrateResult, error) {
	start := time.Now()
	result := HydrateResult{Status: "failed"}
	if err := validateHydrateOptions(opts); err != nil {
		result.Duration = time.Since(start)
		result.Error = err.Error()
		_ = writeHydrateMetadata(opts, result)
		return result, err
	}
	treeRoot := TreeRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit)
	if ok, err := treeReady(treeRoot); err != nil {
		result.Duration = time.Since(start)
		result.Error = err.Error()
		_ = writeHydrateMetadata(opts, result)
		return result, err
	} else if ok {
		result.Status = hydrateStatusSuccess
		result.Provider = "cache"
		result.TreeStatus = hydrateStatusSuccess
		if objectResult, err := hydrateGitObjectDB(ctx, opts); err != nil {
			mergeObjectHydrateResult(&result, objectResult)
			result.Status = "failed"
			result.Duration = time.Since(start)
			result.Error = err.Error()
			_ = writeHydrateMetadata(opts, result)
			return result, err
		} else {
			mergeObjectHydrateResult(&result, objectResult)
		}
		result.Duration = time.Since(start)
		_ = writeHydrateMetadata(opts, result)
		return result, nil
	}

	ref, github := ParseGitHubRepoURL(opts.RepoURL)
	var err error
	if github {
		result, err = hydrateFromGitHubTarball(ctx, opts, ref)
	} else {
		result, err = hydrateFromLocalGit(ctx, opts)
	}
	result.Duration = time.Since(start)
	if err != nil && github {
		fallback, fallbackErr := hydrateFromLocalGit(ctx, opts)
		fallback.Duration = time.Since(start)
		if fallbackErr == nil {
			result = fallback
			err = nil
		}
	}
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
	} else {
		result.TreeStatus = hydrateStatusSuccess
		if objectResult, objectErr := hydrateGitObjectDB(ctx, opts); objectErr != nil {
			mergeObjectHydrateResult(&result, objectResult)
			result.Status = "failed"
			result.Error = objectErr.Error()
			err = objectErr
		} else {
			mergeObjectHydrateResult(&result, objectResult)
		}
	}
	_ = writeHydrateMetadata(opts, result)
	return result, err
}

// GitHubRepoRef identifies a GitHub repository.
type GitHubRepoRef struct {
	Owner string
	Repo  string
}

// ParseGitHubRepoURL parses https, ssh, and SCP-style GitHub repo URLs.
func ParseGitHubRepoURL(raw string) (GitHubRepoRef, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return GitHubRepoRef{}, false
	}
	const scpPrefix = "git@github.com:"
	if strings.HasPrefix(raw, scpPrefix) {
		return parseGitHubRepoPath(strings.TrimPrefix(raw, scpPrefix))
	}
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Hostname(), "github.com") {
		return GitHubRepoRef{}, false
	}
	return parseGitHubRepoPath(u.Path)
}

// SanitizeRepoURL removes credential material from a Git remote URL before it is
// persisted in Drive9 metadata or local hydrate manifests.
func SanitizeRepoURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return raw
	}
	if u.User != nil {
		switch strings.ToLower(u.Scheme) {
		case "http", "https":
			u.User = nil
		default:
			if _, hasPassword := u.User.Password(); hasPassword {
				u.User = url.User(u.User.Username())
			}
		}
	}
	if u.RawQuery != "" {
		q := u.Query()
		for _, key := range []string{"access_token", "auth_token", "oauth_token", "password", "private_token", "token"} {
			q.Del(key)
		}
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// SanitizeGitConfigCredentials redacts credential-bearing remote URLs from a
// Git config file while preserving ordinary config formatting.
func SanitizeGitConfigCredentials(data []byte) []byte {
	lines := strings.SplitAfter(string(data), "\n")
	changed := false
	for i, line := range lines {
		lineNoNewline := strings.TrimRight(line, "\r\n")
		newline := line[len(lineNoNewline):]
		eq := strings.Index(lineNoNewline, "=")
		if eq < 0 || strings.TrimSpace(lineNoNewline[:eq]) != "url" {
			continue
		}
		rawURL := strings.TrimSpace(lineNoNewline[eq+1:])
		sanitized := SanitizeRepoURL(rawURL)
		if sanitized == rawURL {
			continue
		}
		lines[i] = lineNoNewline[:eq+1] + " " + sanitized + newline
		changed = true
	}
	if !changed {
		return data
	}
	return []byte(strings.Join(lines, ""))
}

// GitHubCodeloadURL returns the GitHub codeload tarball URL for a commit.
func GitHubCodeloadURL(ref GitHubRepoRef, commit string) string {
	return fmt.Sprintf("https://codeload.github.com/%s/%s/tar.gz/%s",
		url.PathEscape(ref.Owner),
		url.PathEscape(ref.Repo),
		url.PathEscape(commit),
	)
}

func hydrateFromGitHubTarball(ctx context.Context, opts HydrateOptions, ref GitHubRepoRef) (HydrateResult, error) {
	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, GitHubCodeloadURL(ref, opts.Commit), nil)
	if err != nil {
		return HydrateResult{Provider: "github-codeload"}, err
	}
	if token := strings.TrimSpace(opts.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return HydrateResult{Provider: "github-codeload"}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return HydrateResult{Provider: "github-codeload"}, fmt.Errorf("GitHub codeload %s: %s", resp.Status, msg)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return HydrateResult{Provider: "github-codeload"}, err
	}
	defer func() { _ = gz.Close() }()
	tmp, cleanup, err := prepareTempTree(opts)
	if err != nil {
		return HydrateResult{Provider: "github-codeload"}, err
	}
	defer cleanup()
	files, bytes, err := extractCodeloadTar(gz, tmp)
	if err != nil {
		return HydrateResult{Provider: "github-codeload"}, err
	}
	if err := installTree(opts, tmp); err != nil {
		return HydrateResult{Provider: "github-codeload"}, err
	}
	return HydrateResult{Status: hydrateStatusSuccess, Provider: "github-codeload", Files: files, Bytes: bytes}, nil
}

func hydrateFromLocalGit(ctx context.Context, opts HydrateOptions) (HydrateResult, error) {
	if strings.TrimSpace(opts.GitDir) == "" {
		return HydrateResult{Provider: "git-checkout-index"}, fmt.Errorf("git directory is required for local git hydrate fallback")
	}
	tmp, cleanup, err := prepareTempTree(opts)
	if err != nil {
		return HydrateResult{Provider: "git-checkout-index"}, err
	}
	defer cleanup()

	indexPath := filepath.Join(CacheRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit), "hydrate.index")
	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		return HydrateResult{Provider: "git-checkout-index"}, err
	}
	defer func() { _ = os.Remove(indexPath) }()

	if err := runGitWithIndex(ctx, opts.GitDir, indexPath, "read-tree", opts.Commit); err != nil {
		return HydrateResult{Provider: "git-checkout-index"}, err
	}
	prefix := tmp + string(filepath.Separator)
	if err := runGitWithIndex(ctx, opts.GitDir, indexPath, "checkout-index", "-a", "-f", "--prefix="+prefix); err != nil {
		return HydrateResult{Provider: "git-checkout-index"}, err
	}
	files, bytes, err := scanTreeStats(tmp)
	if err != nil {
		return HydrateResult{Provider: "git-checkout-index"}, err
	}
	if err := installTree(opts, tmp); err != nil {
		return HydrateResult{Provider: "git-checkout-index"}, err
	}
	return HydrateResult{Status: hydrateStatusSuccess, Provider: "git-checkout-index", Files: files, Bytes: bytes}, nil
}

type objectHydrateResult struct {
	status    string
	objects   int64
	bytes     int64
	skipped   int64
	mismatch  int64
	fallbacks int64
	duration  time.Duration
}

type objectHydrateMarker struct {
	GitDir      string    `json:"git_dir"`
	Commit      string    `json:"commit"`
	WorkspaceID string    `json:"workspace_id"`
	Entries     int       `json:"entries"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type objectHydrateFile struct {
	abs       string
	rel       string
	expected  string
	sizeBytes int64
}

func mergeObjectHydrateResult(result *HydrateResult, objectResult objectHydrateResult) {
	if result == nil {
		return
	}
	result.ObjectStatus = objectResult.status
	result.Objects = objectResult.objects
	result.ObjectBytes = objectResult.bytes
	result.ObjectSkipped = objectResult.skipped
	result.ObjectMismatch = objectResult.mismatch
	result.ObjectFallbacks = objectResult.fallbacks
	result.ObjectDuration = objectResult.duration
}

func hydrateGitObjectDB(ctx context.Context, opts HydrateOptions) (objectHydrateResult, error) {
	start := time.Now()
	result := objectHydrateResult{status: "skipped"}
	if len(opts.TreeEntries) == 0 {
		result.duration = time.Since(start)
		return result, nil
	}
	gitDir := strings.TrimSpace(opts.GitDir)
	if gitDir == "" {
		result.duration = time.Since(start)
		return result, fmt.Errorf("git directory is required for object hydrate")
	}
	info, err := os.Stat(gitDir)
	if err != nil {
		result.duration = time.Since(start)
		return result, fmt.Errorf("stat git directory for object hydrate: %w", err)
	}
	if !info.IsDir() {
		result.duration = time.Since(start)
		return result, fmt.Errorf("git directory for object hydrate is not a directory: %s", gitDir)
	}
	if objectHydrateReady(opts) {
		result.status = "cache"
		result.duration = time.Since(start)
		return result, nil
	}
	unlock, err := acquireObjectHydrateLock(ctx, opts)
	if err != nil {
		result.duration = time.Since(start)
		return result, err
	}
	defer unlock()
	if objectHydrateReady(opts) {
		result.status = "cache"
		result.duration = time.Since(start)
		return result, nil
	}
	result, err = hydrateGitObjectDBLocked(ctx, opts)
	result.duration = time.Since(start)
	if err != nil {
		result.status = "failed"
		return result, err
	}
	result.status = hydrateStatusSuccess
	if err := writeObjectHydrateMarker(opts); err != nil {
		result.status = "failed"
		return result, err
	}
	return result, nil
}

func hydrateGitObjectDBLocked(ctx context.Context, opts HydrateOptions) (objectHydrateResult, error) {
	var result objectHydrateResult
	var regular []objectHydrateFile
	var firstErr error
	for _, entry := range opts.TreeEntries {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		kind := strings.TrimSpace(entry.Kind)
		switch kind {
		case "dir", "submodule":
			result.skipped++
			continue
		case "file", "symlink":
		default:
			result.skipped++
			continue
		}
		rel, err := CleanRelative(entry.Path)
		if err != nil {
			return result, err
		}
		expected := strings.ToLower(strings.TrimSpace(entry.ObjectSHA))
		if err := validateObjectSHA(expected); err != nil {
			return result, err
		}
		treePath, err := treeFilePath(opts.LocalRoot, opts.WorkspaceID, opts.Commit, rel)
		if err != nil {
			return result, err
		}
		info, err := os.Lstat(treePath)
		if err != nil {
			if os.IsNotExist(err) {
				result.skipped++
				continue
			}
			return result, err
		}
		switch kind {
		case "file":
			if !info.Mode().IsRegular() {
				result.skipped++
				continue
			}
			regular = append(regular, objectHydrateFile{
				abs:       treePath,
				rel:       rel,
				expected:  expected,
				sizeBytes: info.Size(),
			})
		case "symlink":
			if info.Mode()&fs.ModeSymlink == 0 {
				result.skipped++
				continue
			}
			target, err := os.Readlink(treePath)
			if err != nil {
				return result, err
			}
			got, err := hashGitBlobBytes(ctx, opts.GitDir, []byte(target))
			if err != nil {
				return result, err
			}
			result.bytes += int64(len(target))
			if strings.EqualFold(got, expected) {
				result.objects++
				continue
			}
			result.mismatch++
			if err := fallbackFetchGitObject(ctx, opts.GitDir, expected); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("hydrate symlink object %s mismatch got %s and fallback failed: %w", rel, got, err)
				}
			} else {
				result.fallbacks++
			}
		}
	}
	for start := 0; start < len(regular); start += hydrateHashObjectBatchSize {
		end := start + hydrateHashObjectBatchSize
		if end > len(regular) {
			end = len(regular)
		}
		if err := hashRegularGitObjects(ctx, opts.GitDir, regular[start:end], &result, &firstErr); err != nil {
			return result, err
		}
	}
	return result, firstErr
}

func hashRegularGitObjects(ctx context.Context, gitDir string, files []objectHydrateFile, result *objectHydrateResult, firstErr *error) error {
	if len(files) == 0 {
		return nil
	}
	var batch []objectHydrateFile
	flush := func(items []objectHydrateFile) error {
		if len(items) == 0 {
			return nil
		}
		hashes, err := hashGitRegularObjects(ctx, gitDir, items)
		if err != nil {
			return err
		}
		for i, got := range hashes {
			item := items[i]
			result.bytes += item.sizeBytes
			if strings.EqualFold(got, item.expected) {
				result.objects++
				continue
			}
			result.mismatch++
			if err := fallbackFetchGitObject(ctx, gitDir, item.expected); err != nil {
				if *firstErr == nil {
					*firstErr = fmt.Errorf("hydrate file object %s mismatch got %s and fallback failed: %w", item.rel, got, err)
				}
			} else {
				result.fallbacks++
			}
		}
		return nil
	}
	for _, item := range files {
		if strings.Contains(item.abs, "\n") || strings.Contains(item.abs, "\r") {
			if err := flush(batch); err != nil {
				return err
			}
			batch = nil
			got, err := hashGitRegularObject(ctx, gitDir, item.abs)
			if err != nil {
				return err
			}
			result.bytes += item.sizeBytes
			if strings.EqualFold(got, item.expected) {
				result.objects++
				continue
			}
			result.mismatch++
			if err := fallbackFetchGitObject(ctx, gitDir, item.expected); err != nil {
				if *firstErr == nil {
					*firstErr = fmt.Errorf("hydrate file object %s mismatch got %s and fallback failed: %w", item.rel, got, err)
				}
			} else {
				result.fallbacks++
			}
			continue
		}
		batch = append(batch, item)
	}
	return flush(batch)
}

func hashGitRegularObjects(ctx context.Context, gitDir string, files []objectHydrateFile) ([]string, error) {
	var stdin strings.Builder
	for _, item := range files {
		stdin.WriteString(item.abs)
		stdin.WriteByte('\n')
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "hash-object", "-w", "--no-filters", "--stdin-paths")
	cmd.Stdin = strings.NewReader(stdin.String())
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git hash-object --stdin-paths: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("git hash-object --stdin-paths: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) == 1 && lines[0] == "" {
		lines = nil
	}
	if len(lines) != len(files) {
		return nil, fmt.Errorf("git hash-object wrote %d objects, want %d", len(lines), len(files))
	}
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = strings.TrimSpace(line)
	}
	return out, nil
}

func hashGitRegularObject(ctx context.Context, gitDir, path string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "hash-object", "-w", "--no-filters", path)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return "", fmt.Errorf("git hash-object %s: %w: %s", path, err, msg)
		}
		return "", fmt.Errorf("git hash-object %s: %w", path, err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

func hashGitBlobBytes(ctx context.Context, gitDir string, data []byte) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "hash-object", "-w", "--stdin")
	cmd.Stdin = bytes.NewReader(data)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return "", fmt.Errorf("git hash-object --stdin: %w: %s", err, msg)
		}
		return "", fmt.Errorf("git hash-object --stdin: %w", err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

func fallbackFetchGitObject(ctx context.Context, gitDir, objectSHA string) error {
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "blob", objectSHA)
	cmd.Stdout = io.Discard
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("git cat-file blob %s: %w: %s", objectSHA, err, msg)
		}
		return fmt.Errorf("git cat-file blob %s: %w", objectSHA, err)
	}
	return nil
}

func acquireObjectHydrateLock(ctx context.Context, opts HydrateOptions) (func(), error) {
	root := CacheRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(root, "hydrate.objects.lock")
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = fmt.Fprintf(f, "pid=%d time=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
			_ = f.Close()
			return func() { _ = os.Remove(lockPath) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if info, statErr := os.Stat(lockPath); statErr == nil && time.Since(info.ModTime()) > hydrateObjectLockStaleAfter {
			_ = os.Remove(lockPath)
			continue
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func objectHydrateReady(opts HydrateOptions) bool {
	gitDir := strings.TrimSpace(opts.GitDir)
	if gitDir == "" || len(opts.TreeEntries) == 0 {
		return false
	}
	if info, err := os.Stat(filepath.Join(gitDir, "objects")); err != nil || !info.IsDir() {
		return false
	}
	data, err := os.ReadFile(filepath.Join(CacheRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit), "objects.ready.json"))
	if err != nil {
		return false
	}
	var marker objectHydrateMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return false
	}
	return marker.GitDir == gitDir &&
		marker.Commit == opts.Commit &&
		marker.WorkspaceID == opts.WorkspaceID &&
		marker.Entries == len(opts.TreeEntries)
}

func writeObjectHydrateMarker(opts HydrateOptions) error {
	root := CacheRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	payload := objectHydrateMarker{
		GitDir:      strings.TrimSpace(opts.GitDir),
		Commit:      opts.Commit,
		WorkspaceID: opts.WorkspaceID,
		Entries:     len(opts.TreeEntries),
		UpdatedAt:   time.Now().UTC(),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(root, "objects.ready.json"), append(data, '\n'), 0o644)
}

func runGitWithIndex(ctx context.Context, gitDir, indexPath string, args ...string) error {
	full := append([]string{"--git-dir", gitDir}, args...)
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.Env = append(os.Environ(), "GIT_INDEX_FILE="+indexPath)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("git %s: %w: %s", strings.Join(full, " "), err, msg)
		}
		return fmt.Errorf("git %s: %w", strings.Join(full, " "), err)
	}
	return nil
}

func prepareTempTree(opts HydrateOptions) (string, func(), error) {
	root := CacheRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", func() {}, err
	}
	tmp, err := os.MkdirTemp(root, ".tree-tmp-*")
	if err != nil {
		return "", func() {}, err
	}
	return tmp, func() { _ = os.RemoveAll(tmp) }, nil
}

func installTree(opts HydrateOptions, tmp string) error {
	dst := TreeRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit)
	if ok, err := treeReady(dst); err != nil {
		return err
	} else if ok {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	return nil
}

func extractCodeloadTar(r io.Reader, dst string) (int64, int64, error) {
	tr := tar.NewReader(r)
	var files, bytes int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return files, bytes, nil
		}
		if err != nil {
			return files, bytes, err
		}
		rel, ok, err := stripCodeloadRoot(hdr.Name)
		if err != nil {
			return files, bytes, err
		}
		if !ok {
			continue
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			target, err := hydrateTarget(dst, rel, true)
			if err != nil {
				return files, bytes, err
			}
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&0o777); err != nil {
				return files, bytes, err
			}
		case tar.TypeReg:
			target, err := hydrateTarget(dst, rel, true)
			if err != nil {
				return files, bytes, err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return files, bytes, err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return files, bytes, err
			}
			n, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return files, bytes, copyErr
			}
			if closeErr != nil {
				return files, bytes, closeErr
			}
			files++
			bytes += n
		case tar.TypeSymlink:
			if !safeSymlinkTarget(hdr.Linkname) {
				continue
			}
			target, err := hydrateTarget(dst, rel, false)
			if err != nil {
				return files, bytes, err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return files, bytes, err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return files, bytes, err
			}
			files++
			bytes += int64(len(hdr.Linkname))
		}
	}
}

func hydrateTarget(dst, rel string, includeTarget bool) (string, error) {
	clean, err := CleanRelative(rel)
	if err != nil {
		return "", err
	}
	root, err := filepath.Abs(dst)
	if err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	target := filepath.Join(root, filepath.FromSlash(clean))
	if target != root && !strings.HasPrefix(target, root+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe hydrate path %q", rel)
	}
	if err := rejectHydrateSymlinkTraversal(root, target, includeTarget); err != nil {
		return "", err
	}
	return target, nil
}

func rejectHydrateSymlinkTraversal(root, target string, includeTarget bool) error {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	parts := strings.Split(rel, string(filepath.Separator))
	limit := len(parts)
	if !includeTarget && limit > 0 {
		limit--
	}
	cur := root
	for i := 0; i < limit; i++ {
		cur = filepath.Join(cur, parts[i])
		info, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe hydrate path %q traverses symlink %q", target, cur)
		}
	}
	return nil
}

func stripCodeloadRoot(name string) (string, bool, error) {
	name = strings.TrimPrefix(filepath.ToSlash(name), "/")
	if name == "" {
		return "", false, nil
	}
	parts := strings.Split(name, "/")
	if len(parts) <= 1 {
		return "", false, nil
	}
	rel := strings.Join(parts[1:], "/")
	if rel == "" {
		return "", false, nil
	}
	clean, err := CleanRelative(rel)
	if err != nil {
		return "", false, err
	}
	return clean, true, nil
}

// CleanRelative validates and cleans a repository-relative path.
func CleanRelative(rel string) (string, error) {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	if rel == "" || strings.HasPrefix(rel, "/") || strings.ContainsRune(rel, '\x00') {
		return "", fmt.Errorf("invalid relative path %q", rel)
	}
	clean := path.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("invalid relative path %q", rel)
	}
	for _, part := range strings.Split(clean, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid relative path %q", rel)
		}
	}
	return clean, nil
}

// SliceRead returns a defensive slice copy for an offset/range.
func SliceRead(data []byte, offset, size int64) []byte {
	if offset < 0 {
		return nil
	}
	if offset >= int64(len(data)) {
		return nil
	}
	end := int64(len(data))
	if size >= 0 {
		end = offset + size
	}
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	if end < offset {
		return nil
	}
	out := make([]byte, end-offset)
	copy(out, data[offset:end])
	return out
}

func readFileRange(f *os.File, fileSize, offset, size int64) ([]byte, bool, error) {
	if offset < 0 {
		return nil, true, nil
	}
	if offset >= fileSize {
		return nil, true, nil
	}
	end := fileSize
	if size >= 0 {
		end = offset + size
	}
	if end > fileSize {
		end = fileSize
	}
	if end < offset {
		return nil, true, nil
	}
	buf := make([]byte, end-offset)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, true, err
	}
	return buf[:n], true, nil
}

func treeFilePath(localRoot, workspaceID, commit, rel string) (string, error) {
	clean, err := CleanRelative(rel)
	if err != nil {
		return "", err
	}
	return filepath.Join(TreeRoot(localRoot, workspaceID, commit), filepath.FromSlash(clean)), nil
}

func writeHydrateMetadata(opts HydrateOptions, result HydrateResult) error {
	root := CacheRoot(opts.LocalRoot, opts.WorkspaceID, opts.Commit)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	payload := struct {
		Status          string        `json:"status"`
		Provider        string        `json:"provider"`
		TreeStatus      string        `json:"tree_status,omitempty"`
		ObjectStatus    string        `json:"object_status,omitempty"`
		RepoURL         string        `json:"repo_url"`
		Commit          string        `json:"commit"`
		WorkspaceID     string        `json:"workspace_id"`
		Files           int64         `json:"files"`
		Bytes           int64         `json:"bytes"`
		Objects         int64         `json:"objects"`
		ObjectBytes     int64         `json:"object_bytes"`
		ObjectSkipped   int64         `json:"object_skipped"`
		ObjectMismatch  int64         `json:"object_mismatch"`
		ObjectFallbacks int64         `json:"object_fallbacks"`
		ObjectDuration  time.Duration `json:"object_duration,omitempty"`
		Duration        time.Duration `json:"duration"`
		Error           string        `json:"error,omitempty"`
		UpdatedAt       time.Time     `json:"updated_at"`
	}{
		Status:          result.Status,
		Provider:        result.Provider,
		TreeStatus:      result.TreeStatus,
		ObjectStatus:    result.ObjectStatus,
		RepoURL:         SanitizeRepoURL(opts.RepoURL),
		Commit:          opts.Commit,
		WorkspaceID:     opts.WorkspaceID,
		Files:           result.Files,
		Bytes:           result.Bytes,
		Objects:         result.Objects,
		ObjectBytes:     result.ObjectBytes,
		ObjectSkipped:   result.ObjectSkipped,
		ObjectMismatch:  result.ObjectMismatch,
		ObjectFallbacks: result.ObjectFallbacks,
		ObjectDuration:  result.ObjectDuration,
		Duration:        result.Duration,
		Error:           result.Error,
		UpdatedAt:       time.Now().UTC(),
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(root, "hydrate.json"), append(data, '\n'), 0o644)
}

func treeReady(root string) (bool, error) {
	info, err := os.Stat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

func scanTreeStats(root string) (int64, int64, error) {
	var files, bytes int64
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files++
		if info.Mode()&fs.ModeSymlink != 0 {
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			bytes += int64(len(target))
			return nil
		}
		bytes += info.Size()
		return nil
	})
	return files, bytes, err
}

func validateHydrateOptions(opts HydrateOptions) error {
	if strings.TrimSpace(opts.LocalRoot) == "" {
		return fmt.Errorf("local root is required")
	}
	if strings.TrimSpace(opts.WorkspaceID) == "" {
		return fmt.Errorf("workspace id is required")
	}
	if strings.TrimSpace(opts.Commit) == "" {
		return fmt.Errorf("commit is required")
	}
	if strings.TrimSpace(opts.RepoURL) == "" {
		return fmt.Errorf("repo url is required")
	}
	return nil
}

func validateObjectSHA(sha string) error {
	if len(sha) < 4 {
		return fmt.Errorf("invalid git object sha %q", sha)
	}
	for _, r := range sha {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return fmt.Errorf("invalid git object sha %q", sha)
		}
	}
	return nil
}

func parseGitHubRepoPath(rawPath string) (GitHubRepoRef, bool) {
	p := strings.Trim(strings.TrimSpace(rawPath), "/")
	p = strings.TrimSuffix(p, ".git")
	parts := strings.Split(p, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return GitHubRepoRef{}, false
	}
	return GitHubRepoRef{Owner: parts[0], Repo: parts[1]}, true
}

func safePathSegment(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "." || raw == ".." {
		return "empty"
	}
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		sum := sha256.Sum256([]byte(raw))
		return hex.EncodeToString(sum[:])
	}
	return raw
}

func safeSymlinkTarget(target string) bool {
	if target == "" || strings.HasPrefix(target, "/") || strings.ContainsRune(target, '\x00') {
		return false
	}
	clean := path.Clean(filepath.ToSlash(target))
	return clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
