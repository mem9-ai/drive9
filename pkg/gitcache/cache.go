// Package gitcache provides local, rebuildable caches for Drive9 git
// fast-clone workspaces.
package gitcache

import (
	"archive/tar"
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

// HydrateOptions describes one local clean tree cache hydration.
type HydrateOptions struct {
	LocalRoot   string
	WorkspaceID string
	Commit      string
	RepoURL     string
	GitDir      string
	Token       string
	HTTPClient  *http.Client
}

// HydrateResult summarizes a hydrate attempt.
type HydrateResult struct {
	Status   string        `json:"status"`
	Provider string        `json:"provider"`
	Files    int64         `json:"files"`
	Bytes    int64         `json:"bytes"`
	Duration time.Duration `json:"duration"`
	Error    string        `json:"error,omitempty"`
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
func ReadTreeFile(localRoot, workspaceID, commit, rel string, offset, size int64) ([]byte, bool, error) {
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
		target, err := os.Readlink(p)
		if err != nil {
			return nil, true, err
		}
		return SliceRead([]byte(target), offset, size), true, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = f.Close() }()
	return readFileRange(f, info.Size(), offset, size)
}

// ReadBlob reads a cached blob.
func ReadBlob(localRoot, workspaceID, commit, objectSHA string, offset, size int64) ([]byte, bool, error) {
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
	f, err := os.Open(p)
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = f.Close() }()
	return readFileRange(f, info.Size(), offset, size)
}

// WriteBlob writes a blob cache entry atomically. Existing entries are kept.
func WriteBlob(localRoot, workspaceID, commit, objectSHA string, data []byte) error {
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
	return writeFileAtomic(p, data, 0o644)
}

// Hydrate materializes a workspace clean tree into the local cache.
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
	cleanup = func() {}
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
	cleanup = func() {}
	return HydrateResult{Status: hydrateStatusSuccess, Provider: "git-checkout-index", Files: files, Bytes: bytes}, nil
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
		if err != nil || !ok {
			return files, bytes, err
		}
		target := filepath.Join(dst, filepath.FromSlash(rel))
		if !strings.HasPrefix(target, dst+string(filepath.Separator)) && target != dst {
			return files, bytes, fmt.Errorf("unsafe hydrate path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&0o777); err != nil {
				return files, bytes, err
			}
		case tar.TypeReg, tar.TypeRegA:
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
		Status      string        `json:"status"`
		Provider    string        `json:"provider"`
		RepoURL     string        `json:"repo_url"`
		Commit      string        `json:"commit"`
		WorkspaceID string        `json:"workspace_id"`
		Files       int64         `json:"files"`
		Bytes       int64         `json:"bytes"`
		Duration    time.Duration `json:"duration"`
		Error       string        `json:"error,omitempty"`
		UpdatedAt   time.Time     `json:"updated_at"`
	}{
		Status:      result.Status,
		Provider:    result.Provider,
		RepoURL:     opts.RepoURL,
		Commit:      opts.Commit,
		WorkspaceID: opts.WorkspaceID,
		Files:       result.Files,
		Bytes:       result.Bytes,
		Duration:    result.Duration,
		Error:       result.Error,
		UpdatedAt:   time.Now().UTC(),
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
