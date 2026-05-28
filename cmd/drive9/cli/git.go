package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/gitcache"
	"github.com/mem9-ai/dat9/pkg/mountpath"
	"github.com/mem9-ai/dat9/pkg/mountstate"
	"github.com/mem9-ai/dat9/pkg/pathutil"
)

const (
	gitWorkspaceAPITimeout        = 2 * time.Minute
	githubTreeAPITimeout          = 30 * time.Second
	gitHydrateTimeout             = 30 * time.Minute
	githubAPIBaseURL              = "https://api.github.com"
	gitStateStorageTarGzNoObjects = "tar.gz-no-objects"
)

// Git handles git-aware drive9 workflows.
func Git(args []string) error {
	if len(args) == 0 {
		gitUsage()
		return fmt.Errorf("usage: drive9 git <command> [arguments]")
	}
	switch args[0] {
	case "clone":
		return gitClone(args[1:])
	case "hydrate":
		return gitHydrate(args[1:])
	case "-h", "-help", "--help", "help":
		gitUsage()
		return nil
	default:
		gitUsage()
		return fmt.Errorf("drive9 git: unknown command %q", args[0])
	}
}

func gitUsage() {
	fmt.Fprintf(os.Stderr, `usage: drive9 git <command> [arguments]

commands:
  clone --fast [--blobless] [--hydrate=background|sync|off] <repo-url> <mounted-path>
                       create a local .git and register the HEAD tree
  hydrate <mounted-path>
                       materialize a blobless clean tree into local cache

global:
  -h, --help, help     show this help
`)
}

func gitClone(args []string) error {
	fs := flag.NewFlagSet("git clone", flag.ContinueOnError)
	fast := fs.Bool("fast", false, "use drive9 git fast clone")
	blobless := fs.Bool("blobless", false, "use a blobless partial local .git; clean blobs lazy-fetch from the remote")
	hydrate := fs.String("hydrate", "auto", "blobless clean tree hydrate strategy: auto, background, sync, or off")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 git clone --fast [--blobless] [--hydrate=background|sync|off] <repo-url> <mounted-path>\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*fast {
		return fmt.Errorf("drive9 git clone currently requires --fast")
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return fmt.Errorf("drive9 git clone --fast requires <repo-url> and <mounted-path>")
	}
	hydrateMode, err := resolveGitHydrateMode(*hydrate, *blobless)
	if err != nil {
		return err
	}
	repoURL := fs.Arg(0)
	target := fs.Arg(1)

	resolved, err := resolveMountedGitTarget(target)
	if err != nil {
		return err
	}

	cloneArgs := gitFastCloneArgs(repoURL, target, *blobless)
	if err := runGitStreaming(cloneArgs...); err != nil {
		return fmt.Errorf("git %s: %w", strings.Join(cloneArgs, " "), err)
	}
	head, err := gitOutput(target, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	branch, branchErr := gitOutput(target, "symbolic-ref", "--short", "-q", "HEAD")
	if branchErr != nil {
		branch = ""
	}
	nodes, err := gitListTree(target, head)
	if err != nil {
		return err
	}
	treeSHA, treeErr := gitOutput(target, "rev-parse", head+"^{tree}")
	if treeErr != nil {
		return fmt.Errorf("git rev-parse HEAD^{tree}: %w", treeErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), githubTreeAPITimeout)
	enriched, enrichErr := enrichGitTreeSizesFromGitHub(ctx, repoURL, treeSHA, nodes)
	cancel()
	if enrichErr != nil {
		fmt.Fprintf(os.Stderr, "drive9: warning: could not enrich GitHub tree sizes: %v\n", enrichErr)
		if unknown := unknownGitTreeFileSizeCount(nodes); unknown > 0 {
			fmt.Fprintf(os.Stderr, "drive9: warning: %d git file sizes remain unknown; git status may need to read those blobs lazily\n", unknown)
		}
	} else {
		nodes = enriched
	}
	if err := initializeFastCloneIndex(target, head); err != nil {
		return err
	}

	mode := "fast"
	if *blobless {
		mode = "fast-blobless"
	}
	c := NewFromEnv()
	ctx, cancel = context.WithTimeout(context.Background(), gitWorkspaceAPITimeout)
	ws, err := c.UpsertGitWorkspace(ctx, client.GitWorkspaceRequest{
		RootPath:   resolved.RemotePath,
		RepoURL:    repoURL,
		RemoteName: "origin",
		BranchName: branch,
		BaseCommit: head,
		HeadCommit: head,
		Mode:       mode,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("register git workspace: %w", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), gitWorkspaceAPITimeout)
	if err := c.ReplaceGitTree(ctx, ws.WorkspaceID, client.GitTreeReplaceRequest{
		CommitSHA: head,
		Nodes:     nodes,
	}); err != nil {
		cancel()
		return fmt.Errorf("register git tree manifest: %w", err)
	}
	cancel()
	gitDir := filepath.Join(target, ".git")
	if resolved.LocalGitDir != "" {
		if info, statErr := os.Stat(resolved.LocalGitDir); statErr == nil && info.IsDir() {
			gitDir = resolved.LocalGitDir
		} else if statErr != nil && !os.IsNotExist(statErr) {
			return fmt.Errorf("stat local .git checkpoint path: %w", statErr)
		}
	}
	gitState, err := archiveGitStateDir(gitDir)
	if err != nil {
		return fmt.Errorf("checkpoint .git: %w", err)
	}
	sum := sha256.Sum256(gitState)
	ctx, cancel = context.WithTimeout(context.Background(), gitWorkspaceAPITimeout)
	if _, err := c.UpsertGitState(ctx, ws.WorkspaceID, client.GitStateRequest{
		CheckpointCommit: head,
		StorageType:      gitStateStorageTarGzNoObjects,
		ChecksumSHA256:   hex.EncodeToString(sum[:]),
		SizeBytes:        int64(len(gitState)),
		Content:          gitState,
	}); err != nil {
		cancel()
		return fmt.Errorf("upload .git checkpoint: %w", err)
	}
	cancel()

	fmt.Fprintf(os.Stderr, "drive9: registered git workspace %s at :%s (%d tree entries)\n", ws.WorkspaceID, resolved.RemotePath, len(nodes))
	if *blobless {
		switch hydrateMode {
		case gitHydrateModeSync:
			ctx, cancel := context.WithTimeout(context.Background(), gitHydrateTimeout)
			result, err := hydrateMountedGitTarget(ctx, repoURL, resolved, ws)
			cancel()
			if err != nil {
				return fmt.Errorf("hydrate clean tree: %w", err)
			}
			fmt.Fprintf(os.Stderr, "drive9: hydrated clean tree provider=%s files=%d bytes=%d duration=%s\n",
				result.Provider, result.Files, result.Bytes, result.Duration.Truncate(time.Millisecond))
		case gitHydrateModeBackground:
			if err := startGitHydrateBackground(target, resolved, ws); err != nil {
				fmt.Fprintf(os.Stderr, "drive9: warning: could not start background hydrate: %v\n", err)
			}
		}
	}
	return nil
}

type gitHydrateMode string

const (
	gitHydrateModeOff        gitHydrateMode = "off"
	gitHydrateModeBackground gitHydrateMode = "background"
	gitHydrateModeSync       gitHydrateMode = "sync"
)

func resolveGitHydrateMode(raw string, blobless bool) (gitHydrateMode, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "auto" {
		if blobless {
			return gitHydrateModeBackground, nil
		}
		return gitHydrateModeOff, nil
	}
	switch gitHydrateMode(raw) {
	case gitHydrateModeOff:
		return gitHydrateModeOff, nil
	case gitHydrateModeBackground, gitHydrateModeSync:
		if !blobless {
			return "", fmt.Errorf("--hydrate=%s requires --blobless", raw)
		}
		return gitHydrateMode(raw), nil
	default:
		return "", fmt.Errorf("invalid --hydrate %q (valid: auto, background, sync, off)", raw)
	}
}

func gitHydrate(args []string) error {
	fs := flag.NewFlagSet("git hydrate", flag.ContinueOnError)
	timeout := fs.Duration("timeout", gitHydrateTimeout, "maximum hydrate duration")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 git hydrate [--timeout=30m] <mounted-path>\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("drive9 git hydrate requires <mounted-path>")
	}
	target := fs.Arg(0)
	resolved, err := resolveMountedGitTarget(target)
	if err != nil {
		return err
	}
	c := NewFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), gitWorkspaceAPITimeout)
	ws, err := c.GetGitWorkspaceByRoot(ctx, resolved.RemotePath)
	cancel()
	if err != nil {
		return fmt.Errorf("lookup git workspace :%s: %w", resolved.RemotePath, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), *timeout)
	result, err := hydrateMountedGitTarget(ctx, ws.RepoURL, resolved, ws)
	cancel()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "drive9: hydrated clean tree provider=%s files=%d bytes=%d duration=%s\n",
		result.Provider, result.Files, result.Bytes, result.Duration.Truncate(time.Millisecond))
	return nil
}

func hydrateMountedGitTarget(ctx context.Context, repoURL string, resolved mountedGitTarget, ws *client.GitWorkspace) (gitcache.HydrateResult, error) {
	if ws == nil {
		return gitcache.HydrateResult{}, fmt.Errorf("workspace is required")
	}
	repoURL = strings.TrimSpace(repoURL)
	if repoURL == "" {
		repoURL = ws.RepoURL
	}
	return gitcache.Hydrate(ctx, gitcache.HydrateOptions{
		LocalRoot:   resolved.LocalRoot,
		WorkspaceID: ws.WorkspaceID,
		Commit:      ws.HeadCommit,
		RepoURL:     repoURL,
		GitDir:      resolved.LocalGitDir,
		Token:       githubTokenForRepoURL(repoURL),
	})
}

func startGitHydrateBackground(target string, resolved mountedGitTarget, ws *client.GitWorkspace) error {
	if strings.TrimSpace(resolved.LocalRoot) == "" {
		return fmt.Errorf("mount metadata does not include local_root")
	}
	if ws == nil {
		return fmt.Errorf("workspace is required")
	}
	logPath := gitcache.HydrateLogPath(resolved.LocalRoot, ws.WorkspaceID, ws.HeadCommit)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(os.Args[0], "git", "hydrate", "--timeout="+gitHydrateTimeout.String(), target)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return err
	}
	if err := cmd.Process.Release(); err != nil {
		_ = logFile.Close()
		return err
	}
	_ = logFile.Close()
	fmt.Fprintf(os.Stderr, "drive9: started background clean tree hydrate pid=%d log=%s\n", cmd.Process.Pid, logPath)
	return nil
}

func gitFastCloneArgs(repoURL, target string, blobless bool) []string {
	args := []string{"clone"}
	if blobless {
		args = append(args, "--filter=blob:none")
	}
	args = append(args, "--no-checkout", repoURL, target)
	return args
}

func initializeFastCloneIndex(repoDir, commitSHA string) error {
	if err := gitRun(repoDir, "read-tree", "--reset", commitSHA); err != nil {
		return fmt.Errorf("initialize git index: %w", err)
	}
	return nil
}

type mountedGitTarget struct {
	MountPoint  string
	RemoteRoot  string
	RemotePath  string
	Profile     string
	LocalRoot   string
	LocalGitDir string
}

func resolveMountedGitTarget(target string) (mountedGitTarget, error) {
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return mountedGitTarget{}, fmt.Errorf("resolve target path: %w", err)
	}
	candidate := filepath.Clean(absTarget)
	for {
		state, _, err := mountstate.ReadProcessState(candidate)
		if err == nil {
			if strings.TrimSpace(state.RemoteRoot) == "" {
				return mountedGitTarget{}, fmt.Errorf("drive9 mount metadata for %q does not include remote_root; remount with a newer drive9 CLI", candidate)
			}
			absMount, rel, ok, err := relToMountedTarget(absTarget, state.MountPoint)
			if err != nil {
				return mountedGitTarget{}, err
			}
			if !ok {
				absMount, rel, ok, err = relToMountedTarget(absTarget, candidate)
				if err != nil {
					return mountedGitTarget{}, err
				}
			}
			if !ok {
				return mountedGitTarget{}, fmt.Errorf("target %q is outside drive9 mount %q", target, candidate)
			}
			localPath := "/"
			if rel != "." {
				localPath = filepath.ToSlash(rel)
			}
			remotePath := mountpath.ToRemote(state.RemoteRoot, localPath)
			remotePath, err = pathutil.CanonicalizeDir(remotePath)
			if err != nil {
				return mountedGitTarget{}, fmt.Errorf("canonicalize remote workspace path: %w", err)
			}
			localGitDir, err := localGitDirForMountedTarget(state.LocalRoot, rel)
			if err != nil {
				return mountedGitTarget{}, err
			}
			return mountedGitTarget{
				MountPoint:  absMount,
				RemoteRoot:  state.RemoteRoot,
				RemotePath:  remotePath,
				Profile:     state.Profile,
				LocalRoot:   state.LocalRoot,
				LocalGitDir: localGitDir,
			}, nil
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			break
		}
		candidate = parent
	}
	return mountedGitTarget{}, fmt.Errorf("target %q is not inside a drive9 mount with readable mount metadata", target)
}

func localGitDirForMountedTarget(localRoot, rel string) (string, error) {
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return "", nil
	}
	if !filepath.IsAbs(localRoot) {
		return "", fmt.Errorf("drive9 mount metadata local_root must be absolute, got %q", localRoot)
	}
	localPath := filepath.Join(localRoot, "overlay")
	if rel != "" && rel != "." {
		localPath = filepath.Join(localPath, rel)
	}
	return filepath.Join(localPath, ".git"), nil
}

func relToMountedTarget(absTarget, mountPoint string) (absMount string, rel string, ok bool, err error) {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return "", "", false, nil
	}
	absMount, err = filepath.Abs(mountPoint)
	if err != nil {
		return "", "", false, fmt.Errorf("resolve mount point: %w", err)
	}
	rel, err = filepath.Rel(absMount, absTarget)
	if err != nil {
		return "", "", false, fmt.Errorf("map target to mount: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return absMount, "", false, nil
	}
	return absMount, rel, true, nil
}

func runGitStreaming(args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func gitOutput(repoDir string, args ...string) (string, error) {
	full := append([]string{"-C", repoDir}, args...)
	cmd := exec.Command("git", full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return "", fmt.Errorf("%w: %s", err, msg)
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func gitRun(repoDir string, args ...string) error {
	full := append([]string{"-C", repoDir}, args...)
	cmd := exec.Command("git", full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("%w: %s", err, msg)
		}
		return err
	}
	return nil
}

func gitListTree(repoDir, commitSHA string) ([]client.GitTreeNode, error) {
	full := []string{"-C", repoDir, "ls-tree", "-r", "-t", "-z", commitSHA}
	cmd := exec.Command("git", full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git ls-tree: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("git ls-tree: %w", err)
	}
	nodes, err := parseGitLsTree(out)
	if err != nil {
		return nil, fmt.Errorf("parse git ls-tree: %w", err)
	}
	return nodes, nil
}

func parseGitLsTree(out []byte) ([]client.GitTreeNode, error) {
	records := bytes.Split(out, []byte{0})
	nodes := make([]client.GitTreeNode, 0, len(records))
	for _, rec := range records {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("record missing path separator")
		}
		meta := strings.Fields(string(rec[:tab]))
		if len(meta) < 3 {
			return nil, fmt.Errorf("record metadata has %d fields", len(meta))
		}
		mode, gitType, objectSHA := meta[0], meta[1], meta[2]
		path := string(rec[tab+1:])
		parent, name, err := splitGitManifestPath(path)
		if err != nil {
			return nil, err
		}
		kind, err := gitTreeKind(mode, gitType)
		if err != nil {
			return nil, err
		}
		size := int64(-1)
		if len(meta) >= 4 && meta[3] != "-" {
			sizeRaw := meta[3]
			size, err = strconv.ParseInt(sizeRaw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid size %q for %q: %w", sizeRaw, path, err)
			}
		}
		nodes = append(nodes, client.GitTreeNode{
			Path:       path,
			ParentPath: parent,
			Name:       name,
			Kind:       kind,
			Mode:       mode,
			ObjectSHA:  objectSHA,
			SizeBytes:  size,
		})
	}
	return nodes, nil
}

type githubRepoRef struct {
	Owner string
	Repo  string
}

type githubTreeResponse struct {
	Tree      []githubTreeEntry `json:"tree"`
	Truncated bool              `json:"truncated"`
}

type githubTreeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
	Size *int64 `json:"size"`
}

func enrichGitTreeSizesFromGitHub(ctx context.Context, repoURL, treeSHA string, nodes []client.GitTreeNode) ([]client.GitTreeNode, error) {
	ref, ok := parseGitHubRepoURL(repoURL)
	if !ok {
		return nodes, nil
	}
	sizes, err := fetchGitHubTreeSizes(ctx, http.DefaultClient, githubAPIBaseURL, ref, treeSHA, githubTokenForRepoURL(repoURL))
	if err != nil {
		return nodes, err
	}
	return applyGitHubTreeSizes(nodes, sizes), nil
}

func applyGitHubTreeSizes(nodes []client.GitTreeNode, sizes map[string]int64) []client.GitTreeNode {
	out := make([]client.GitTreeNode, len(nodes))
	copy(out, nodes)
	for i := range out {
		if out[i].Kind == "dir" || out[i].Kind == "submodule" {
			continue
		}
		if size, ok := sizes[out[i].Path]; ok {
			out[i].SizeBytes = size
		}
	}
	return out
}

func fetchGitHubTreeSizes(ctx context.Context, httpClient *http.Client, baseURL string, ref githubRepoRef, treeSHA string, token string) (map[string]int64, error) {
	body, err := fetchGitHubTree(ctx, httpClient, baseURL, ref, treeSHA, token, true)
	if err != nil {
		return nil, err
	}
	if !body.Truncated {
		return githubTreeSizesFromEntries("", body.Tree), nil
	}
	return fetchGitHubTreeSizesWalk(ctx, httpClient, baseURL, ref, treeSHA, token)
}

func fetchGitHubTree(ctx context.Context, httpClient *http.Client, baseURL string, ref githubRepoRef, treeSHA string, token string, recursive bool) (githubTreeResponse, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	baseURL = strings.TrimRight(baseURL, "/")
	endpoint := fmt.Sprintf("%s/repos/%s/%s/git/trees/%s",
		baseURL,
		url.PathEscape(ref.Owner),
		url.PathEscape(ref.Repo),
		url.PathEscape(treeSHA),
	)
	if recursive {
		endpoint += "?recursive=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return githubTreeResponse{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token = strings.TrimSpace(token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return githubTreeResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return githubTreeResponse{}, fmt.Errorf("GitHub tree API %s: %s", resp.Status, msg)
	}
	var body githubTreeResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return githubTreeResponse{}, err
	}
	return body, nil
}

func fetchGitHubTreeSizesWalk(ctx context.Context, httpClient *http.Client, baseURL string, ref githubRepoRef, treeSHA string, token string) (map[string]int64, error) {
	sizes := make(map[string]int64)
	pending := []struct {
		sha    string
		prefix string
	}{{sha: treeSHA}}
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		body, err := fetchGitHubTree(ctx, httpClient, baseURL, ref, item.sha, token, false)
		if err != nil {
			return nil, err
		}
		if body.Truncated {
			return nil, fmt.Errorf("GitHub tree API non-recursive response was truncated for tree %s", item.sha)
		}
		for _, entry := range body.Tree {
			fullPath := entry.Path
			if item.prefix != "" {
				fullPath = item.prefix + "/" + entry.Path
			}
			switch entry.Type {
			case "blob":
				if fullPath != "" && entry.Size != nil {
					sizes[fullPath] = *entry.Size
				}
			case "tree":
				if entry.SHA != "" && fullPath != "" {
					pending = append(pending, struct {
						sha    string
						prefix string
					}{sha: entry.SHA, prefix: fullPath})
				}
			}
		}
	}
	return sizes, nil
}

func githubTreeSizesFromEntries(prefix string, entries []githubTreeEntry) map[string]int64 {
	sizes := make(map[string]int64)
	for _, entry := range entries {
		fullPath := entry.Path
		if prefix != "" {
			fullPath = prefix + "/" + entry.Path
		}
		if entry.Type != "blob" || fullPath == "" || entry.Size == nil {
			continue
		}
		sizes[fullPath] = *entry.Size
	}
	return sizes
}

func parseGitHubRepoURL(raw string) (githubRepoRef, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return githubRepoRef{}, false
	}
	const scpPrefix = "git@github.com:"
	if strings.HasPrefix(raw, scpPrefix) {
		return parseGitHubRepoPath(strings.TrimPrefix(raw, scpPrefix))
	}
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Hostname(), "github.com") {
		return githubRepoRef{}, false
	}
	return parseGitHubRepoPath(u.Path)
}

func parseGitHubRepoPath(rawPath string) (githubRepoRef, bool) {
	p := strings.Trim(strings.TrimSpace(rawPath), "/")
	p = strings.TrimSuffix(p, ".git")
	parts := strings.Split(p, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return githubRepoRef{}, false
	}
	return githubRepoRef{Owner: parts[0], Repo: parts[1]}, true
}

func githubTokenForRepoURL(repoURL string) string {
	for _, key := range []string{"GITHUB_TOKEN", "GH_TOKEN"} {
		if token := strings.TrimSpace(os.Getenv(key)); token != "" {
			return token
		}
	}
	u, err := url.Parse(repoURL)
	if err != nil || !strings.EqualFold(u.Hostname(), "github.com") || u.User == nil {
		return ""
	}
	if password, ok := u.User.Password(); ok && password != "" {
		return password
	}
	username := u.User.Username()
	if username != "" && username != "git" && username != "x-access-token" {
		return username
	}
	return ""
}

func unknownGitTreeFileSizeCount(nodes []client.GitTreeNode) int {
	var count int
	for _, n := range nodes {
		if (n.Kind == "file" || n.Kind == "symlink") && n.SizeBytes < 0 {
			count++
		}
	}
	return count
}

func gitTreeKind(mode, gitType string) (string, error) {
	switch gitType {
	case "tree":
		return "dir", nil
	case "commit":
		return "submodule", nil
	case "blob":
		if mode == "120000" {
			return "symlink", nil
		}
		return "file", nil
	default:
		return "", fmt.Errorf("unsupported git tree type %q", gitType)
	}
}

func splitGitManifestPath(p string) (parent, name string, err error) {
	if p == "" {
		return "", "", fmt.Errorf("git tree path is empty")
	}
	if strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") {
		return "", "", fmt.Errorf("git tree path must be relative without trailing slash: %q", p)
	}
	if strings.ContainsRune(p, '\x00') || strings.ContainsRune(p, '\\') {
		return "", "", fmt.Errorf("git tree path contains unsupported character: %q", p)
	}
	parts := strings.Split(p, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", "", fmt.Errorf("git tree path contains invalid segment %q", part)
		}
	}
	name = parts[len(parts)-1]
	if len(parts) > 1 {
		parent = strings.Join(parts[:len(parts)-1], "/")
	}
	return parent, name, nil
}

func archiveGitStateDir(gitDir string) ([]byte, error) {
	return archiveGitDir(gitDir, shouldSkipGitObjectStatePath)
}

func archiveGitDir(gitDir string, skip func(string, fs.DirEntry) bool) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	err := filepath.WalkDir(gitDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == gitDir {
			return nil
		}
		rel, err := filepath.Rel(gitDir, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if skip != nil && skip(name, d) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(p)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = name
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func shouldSkipGitObjectStatePath(rel string, _ fs.DirEntry) bool {
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == "objects" {
			return true
		}
	}
	return false
}
