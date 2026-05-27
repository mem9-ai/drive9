package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountpath"
	"github.com/mem9-ai/dat9/pkg/mountstate"
	"github.com/mem9-ai/dat9/pkg/pathutil"
)

const gitWorkspaceAPITimeout = 2 * time.Minute

// Git handles git-aware drive9 workflows.
func Git(args []string) error {
	if len(args) == 0 {
		gitUsage()
		return fmt.Errorf("usage: drive9 git <command> [arguments]")
	}
	switch args[0] {
	case "clone":
		return gitClone(args[1:])
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
  clone --fast <repo-url> <mounted-path>
                       create a blobless local .git and register the HEAD tree

global:
  -h, --help, help     show this help
`)
}

func gitClone(args []string) error {
	fs := flag.NewFlagSet("git clone", flag.ContinueOnError)
	fast := fs.Bool("fast", false, "use drive9 git fast clone")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 git clone --fast <repo-url> <mounted-path>\n\nflags:\n")
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
	repoURL := fs.Arg(0)
	target := fs.Arg(1)

	resolved, err := resolveMountedGitTarget(target)
	if err != nil {
		return err
	}

	if err := runGitStreaming("clone", "--filter=blob:none", "--no-checkout", repoURL, target); err != nil {
		return fmt.Errorf("git clone --filter=blob:none --no-checkout: %w", err)
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

	c := NewFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), gitWorkspaceAPITimeout)
	defer cancel()
	ws, err := c.UpsertGitWorkspace(ctx, client.GitWorkspaceRequest{
		RootPath:   resolved.RemotePath,
		RepoURL:    repoURL,
		RemoteName: "origin",
		BranchName: branch,
		BaseCommit: head,
		HeadCommit: head,
		Mode:       "fast",
	})
	if err != nil {
		return fmt.Errorf("register git workspace: %w", err)
	}
	if err := c.ReplaceGitTree(ctx, ws.WorkspaceID, client.GitTreeReplaceRequest{
		CommitSHA: head,
		Nodes:     nodes,
	}); err != nil {
		return fmt.Errorf("register git tree manifest: %w", err)
	}
	gitState, err := archiveGitDir(filepath.Join(target, ".git"))
	if err != nil {
		return fmt.Errorf("checkpoint .git: %w", err)
	}
	sum := sha256.Sum256(gitState)
	if _, err := c.UpsertGitState(ctx, ws.WorkspaceID, client.GitStateRequest{
		CheckpointCommit: head,
		StorageType:      "tar.gz",
		ChecksumSHA256:   hex.EncodeToString(sum[:]),
		SizeBytes:        int64(len(gitState)),
		Content:          gitState,
	}); err != nil {
		return fmt.Errorf("upload .git checkpoint: %w", err)
	}

	fmt.Fprintf(os.Stderr, "drive9: registered git workspace %s at :%s (%d tree entries)\n", ws.WorkspaceID, resolved.RemotePath, len(nodes))
	return nil
}

type mountedGitTarget struct {
	MountPoint string
	RemoteRoot string
	RemotePath string
	Profile    string
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
			return mountedGitTarget{
				MountPoint: absMount,
				RemoteRoot: state.RemoteRoot,
				RemotePath: remotePath,
				Profile:    state.Profile,
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

func gitListTree(repoDir, commitSHA string) ([]client.GitTreeNode, error) {
	full := []string{"-C", repoDir, "ls-tree", "-r", "-t", "-l", "-z", commitSHA}
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
		if len(meta) < 4 {
			return nil, fmt.Errorf("record metadata has %d fields", len(meta))
		}
		mode, gitType, objectSHA, sizeRaw := meta[0], meta[1], meta[2], meta[3]
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
		if sizeRaw != "-" {
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

func archiveGitDir(gitDir string) ([]byte, error) {
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
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(gitDir, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
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
		defer func() { _ = f.Close() }()
		_, err = io.Copy(tw, f)
		return err
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
