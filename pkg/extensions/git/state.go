// Local git-state checkpoint machinery: schedule/checkpoint/restore of a
// workspace's .git directory — object packing with escalating fallbacks,
// the object-without-state archive, and the atomic restore (remote clone,
// stored packs, archive, then the HEAD-tree hydration for blobless
// workspaces). All persistence goes through the /.drive9/git-workspaces
// store view (packs/<id> + git-state.tar.gz); the per-node overlay
// pruning of the workspace view is approximated by the workspace
// HeadCommit cutoff, which bounds the object walk the same way for a
// restored workspace.
package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/gitcache"
	"github.com/mem9-ai/drive9/pkg/gitws"
)

const (
	gitLocalObjectMaxPackByte = 256 << 20

	gitStateStorageTarGzNoObjects = gitws.GitStateStorageTarGzNoObjects
)

func gitCommandOutput(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, msg)
		}
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}

func gitCommandOutputNoLazy(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	setGitNoLazyEnv(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, msg)
		}
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}

func setGitNoLazyEnv(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0")
}

func isMissingGitObjectError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "missing") ||
		strings.Contains(msg, "Not a valid object name") ||
		strings.Contains(msg, "could not get object info") ||
		strings.Contains(msg, "bad object")
}

func isGitObjectID(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func mergeObjectSets(sets ...map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for _, set := range sets {
		for oid := range set {
			out[oid] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// packGitObjects packs the given object IDs with git pack-objects.
func packGitObjects(ctx context.Context, gitDir string, objects map[string]struct{}) ([]byte, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	ids := mapKeys(objects)
	sort.Strings(ids)
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "pack-objects", "--stdout")
	setGitNoLazyEnv(cmd)
	cmd.Stdin = strings.NewReader(strings.Join(ids, "\n") + "\n")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git pack-objects: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("git pack-objects: %w", err)
	}
	return stdout.Bytes(), nil
}

func gitObjectTypeNoLazy(ctx context.Context, gitDir, oid string) (string, bool, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "cat-file", "-t", oid)
	if err != nil {
		if isMissingGitObjectError(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.ToLower(strings.TrimSpace(string(out))), true, nil
}

func gitCommitTreeAndParentsNoLazy(ctx context.Context, gitDir, oid string) (string, []string, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "show", "-s", "--format=%T%n%P", oid)
	if err != nil {
		return "", nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 {
		return "", nil, nil
	}
	tree := strings.ToLower(strings.TrimSpace(lines[0]))
	var parents []string
	if len(lines) > 1 {
		for _, p := range strings.Fields(lines[1]) {
			parents = append(parents, strings.ToLower(p))
		}
	}
	return tree, parents, nil
}

func gitTagTargetNoLazy(ctx context.Context, gitDir, oid string) (string, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "rev-parse", oid+"^{commit}")
	if err != nil {
		return "", nil
	}
	return strings.ToLower(strings.TrimSpace(string(out))), nil
}

// collectLocalRefTips gathers HEAD + local branch/stash tips.
func collectLocalRefTips(ctx context.Context, gitDir string) ([]string, error) {
	seen := make(map[string]struct{})
	add := func(raw string) {
		fields := strings.Fields(raw)
		if len(fields) == 0 {
			return
		}
		oid := strings.ToLower(strings.TrimSpace(fields[0]))
		if !isGitObjectID(oid) {
			return
		}
		seen[oid] = struct{}{}
	}
	if head, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "rev-parse", "--verify", "HEAD"); err == nil {
		add(string(head))
	} else if !isMissingGitObjectError(err) {
		return nil, err
	}
	if out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "for-each-ref", "--format=%(objectname)", "refs/heads", "refs/stash"); err != nil {
		if !isMissingGitObjectError(err) {
			return nil, err
		}
	} else {
		for _, line := range strings.Split(string(out), "\n") {
			add(line)
		}
	}
	tips := mapKeys(seen)
	sort.Strings(tips)
	return tips, nil
}

// collectLocalCommitObjects walks commits/trees from tip down to the
// workspace's base (HeadCommit) cutoff, accumulating object IDs.
func collectLocalCommitObjects(ctx context.Context, gitDir string, baseCommit, tip string, objects map[string]struct{}, seenCommits, seenTrees map[string]struct{}) (bool, error) {
	tip = strings.ToLower(strings.TrimSpace(tip))
	if !isGitObjectID(tip) || tip == baseCommit {
		return false, nil
	}
	stack := []string{tip}
	for len(stack) > 0 {
		oid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if oid == "" || oid == baseCommit {
			continue
		}
		if _, ok := seenCommits[oid]; ok {
			continue
		}
		typ, ok, err := gitObjectTypeNoLazy(ctx, gitDir, oid)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		switch typ {
		case "commit":
			seenCommits[oid] = struct{}{}
			objects[oid] = struct{}{}
			tree, parents, err := gitCommitTreeAndParentsNoLazy(ctx, gitDir, oid)
			if err != nil {
				return false, err
			}
			if tree != "" {
				if oversize, err := collectTreeObjectsNoLazy(ctx, gitDir, tree, "", objects, seenTrees); err != nil || oversize {
					return oversize, err
				}
			}
			for _, parent := range parents {
				parent = strings.ToLower(strings.TrimSpace(parent))
				if parent != "" && parent != baseCommit {
					stack = append(stack, parent)
				}
			}
		case "tag":
			target, err := gitTagTargetNoLazy(ctx, gitDir, oid)
			if err != nil {
				return false, err
			}
			if target != "" && target != baseCommit {
				stack = append(stack, target)
			}
		}
	}
	return false, nil
}

func collectTreeObjectsNoLazy(ctx context.Context, gitDir, treeOID, relDir string, objects map[string]struct{}, seenTrees map[string]struct{}) (bool, error) {
	treeOID = strings.ToLower(strings.TrimSpace(treeOID))
	if !isGitObjectID(treeOID) {
		return false, nil
	}
	if _, ok := seenTrees[treeOID]; ok {
		return false, nil
	}
	seenTrees[treeOID] = struct{}{}
	objects[treeOID] = struct{}{}

	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "ls-tree", "-z", treeOID)
	if err != nil {
		return false, err
	}
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		sp := bytes.IndexByte(rec, ' ')
		if sp < 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[sp:tab]))
		if len(meta) < 2 {
			continue
		}
		kind, oid := meta[0], strings.ToLower(meta[1])
		name := string(rec[tab+1:])
		switch kind {
		case "tree":
			sub := name
			if relDir != "" {
				sub = relDir + "/" + name
			}
			if oversize, err := collectTreeObjectsNoLazy(ctx, gitDir, oid, sub, objects, seenTrees); err != nil || oversize {
				return oversize, err
			}
		case "blob":
			objects[oid] = struct{}{}
		}
	}
	return false, nil
}

type gitObjectInfo struct {
	typ  string
	size int64
}

func gitObjectInfoBatch(ctx context.Context, gitDir string, ids []string) (map[string]gitObjectInfo, error) {
	out := make(map[string]gitObjectInfo, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)", "--buffer")
	setGitNoLazyEnv(cmd)
	cmd.Stdin = strings.NewReader(strings.Join(ids, "\n") + "\n")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git cat-file --batch-check: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("git cat-file --batch-check: %w", err)
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		var size int64
		_, _ = fmt.Sscanf(fields[2], "%d", &size)
		out[strings.ToLower(fields[0])] = gitObjectInfo{typ: fields[1], size: size}
	}
	return out, nil
}

// buildLocalGitObjectPack packs ref objects + staged blobs with escalating
// fallbacks against the pack budget.
func (g *Git) buildLocalGitObjectPack(ctx context.Context, gitDir, baseCommit string) ([]byte, error) {
	refObjects, oversize, err := collectLocalRefObjectSet(ctx, gitDir, baseCommit)
	if err != nil {
		return nil, err
	}
	if oversize {
		refObjects = nil
	}
	stagedObjects, err := collectStagedObjectSet(ctx, gitDir)
	if err != nil {
		return nil, err
	}
	pack, err := packGitObjects(ctx, gitDir, mergeObjectSets(refObjects, stagedObjects))
	if err != nil {
		return nil, err
	}
	if int64(len(pack)) > gitLocalObjectMaxPackByte {
		if pack, err = packGitObjects(ctx, gitDir, refObjects); err != nil {
			return nil, err
		}
	}
	if int64(len(pack)) > gitLocalObjectMaxPackByte {
		if pack, err = packGitObjects(ctx, gitDir, stagedObjects); err != nil {
			return nil, err
		}
		if int64(len(pack)) > gitLocalObjectMaxPackByte {
			pack = nil
		}
	}
	return pack, nil
}

func collectLocalRefObjectSet(ctx context.Context, gitDir, baseCommit string) (map[string]struct{}, bool, error) {
	tips, err := collectLocalRefTips(ctx, gitDir)
	if err != nil {
		return nil, false, err
	}
	if len(tips) == 0 {
		return nil, false, nil
	}
	objects := make(map[string]struct{})
	seenCommits := make(map[string]struct{})
	seenTrees := make(map[string]struct{})
	for _, tip := range tips {
		oversize, err := collectLocalCommitObjects(ctx, gitDir, baseCommit, tip, objects, seenCommits, seenTrees)
		if err != nil {
			return nil, false, err
		}
		if oversize {
			return nil, true, nil
		}
	}
	return objects, false, nil
}

func collectStagedObjectSet(ctx context.Context, gitDir string) (map[string]struct{}, error) {
	out, err := gitCommandOutput(ctx, "--git-dir", gitDir, "ls-files", "-s", "-z")
	if err != nil {
		return nil, err
	}
	var staged []string
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[:tab]))
		if len(meta) < 3 || meta[2] != "0" {
			continue
		}
		staged = append(staged, meta[1])
	}
	if len(staged) == 0 {
		return nil, nil
	}
	info, err := gitObjectInfoBatch(ctx, gitDir, staged)
	if err != nil {
		return nil, err
	}
	objects := make(map[string]struct{})
	for _, oid := range staged {
		if object, ok := info[strings.ToLower(oid)]; ok && object.typ != "" && object.typ != "missing" {
			objects[strings.ToLower(oid)] = struct{}{}
		}
	}
	return objects, nil
}

func shouldSkipGitObjectStatePath(rel string, _ fs.DirEntry) bool {
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == "objects" {
			return true
		}
	}
	return false
}

// archiveLocalGitStateDir tars+gzips the .git tree without objects/.
func archiveLocalGitStateDir(gitDir string) ([]byte, error) {
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
		if shouldSkipGitObjectStatePath(name, d) {
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
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = name
		var data []byte
		if info.Mode().IsRegular() {
			var rerr error
			if data, rerr = os.ReadFile(p); rerr != nil {
				return rerr
			}
			// A checkpoint archive lands in the shared /.drive9 store:
			// git config credentials must never ride along.
			if isGitConfigStatePath(name) {
				data = gitcache.SanitizeGitConfigCredentials(data)
				hdr.Size = int64(len(data))
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if len(data) > 0 {
			if _, err := tw.Write(data); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// extractGitArchive unpacks a checkpoint archive into dst.
func extractGitArchive(content []byte, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	gz, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(filepath.FromSlash(hdr.Name))
		if name == "." || strings.HasPrefix(name, "..") {
			continue
		}
		target := filepath.Join(dst, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeSymlink:
			// A checkpoint archive is untrusted input: a link pointing
			// outside the extracted tree must never materialize.
			if !validateGitArchiveLinkname(hdr.Linkname) {
				return fmt.Errorf("git archive: symlink %q escapes the extraction root", hdr.Linkname)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			data, err := io.ReadAll(tr)
			if err != nil {
				return err
			}
			if err := os.WriteFile(target, data, fs.FileMode(hdr.Mode)&0o777|0o600); err != nil {
				return err
			}
		}
	}
}

// validateGitArchiveLinkname rejects archive symlinks whose target is
// absolute or climbs above the extraction root.
func validateGitArchiveLinkname(linkname string) bool {
	if linkname == "" || filepath.IsAbs(linkname) {
		return false
	}
	rel := filepath.Clean(filepath.FromSlash(linkname))
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func gitDirLooksUsable(ctx context.Context, gitDir string) bool {
	info, err := os.Stat(gitDir)
	if err != nil || !info.IsDir() {
		return false
	}
	head, err := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if err != nil || strings.TrimSpace(string(head)) == "" {
		return false
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(checkCtx, "git", "--git-dir", gitDir, "cat-file", "-e", "HEAD^{commit}")
	cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1")
	return cmd.Run() == nil
}

// localOverlayGitDir maps a view root onto the localize overlay backing
// path of its .git directory: <LocalRoot>/overlay/<rel>/.git (the same
// mapping the CLI derives, localGitDirForMountedTarget).
func (g *Git) localOverlayGitDir(root string) string {
	local := strings.TrimSuffix(strings.TrimSpace(root), "/")
	if local == "" {
		local = "/"
	}
	if !strings.HasPrefix(local, "/") {
		local = "/" + local
	}
	rel := strings.TrimPrefix(local, "/")
	parts := []string{strings.TrimSpace(g.localRoot), "overlay"}
	if rel != "" {
		parts = append(parts, filepath.FromSlash(rel))
	}
	return filepath.Join(append(parts, ".git")...)
}

// commonGitDirFor resolves the common (main) workspace's git dir that a
// linked worktree anchors against.
func (g *Git) commonGitDirFor(w *workspace) (string, error) {
	commonID := w.commonWorkspaceID()
	if commonID == "" {
		return "", fmt.Errorf("git workspace %s (linked) has no common_workspace_id", w.id)
	}
	common := g.workspaceByID(commonID)
	if common == nil {
		return "", fmt.Errorf("git workspace %s: common workspace %s not loaded", w.id, commonID)
	}
	return g.localOverlayGitDir(common.rootPath()), nil
}

// gitDirForWorkspace maps a workspace onto the local .git path its state
// machine checkpoints and restores: a main workspace owns the overlay
// .git directory; a linked worktree owns the common gitdir's
// worktrees/<name> subdirectory (its work-tree .git is a pointer file).
func (g *Git) gitDirForWorkspace(w *workspace) string {
	if !w.linked() {
		return g.localOverlayGitDir(w.rootPath())
	}
	if w.worktree() == "" {
		return g.localOverlayGitDir(w.rootPath())
	}
	common, err := g.commonGitDirFor(w)
	if err != nil {
		return g.localOverlayGitDir(w.rootPath())
	}
	return filepath.Join(common, "worktrees", w.worktree())
}

// scheduleGitStateCheckpoint debounces a checkpoint for the workspace
// owning path.
func (g *Git) scheduleGitStateCheckpoint(path string) {
	if g == nil || g.checkpoints == nil || g.localRoot == "" {
		return
	}
	w, rel, ok := g.workspaceFor(path)
	if !ok {
		return
	}
	gitDir := g.gitDirForWorkspace(w)
	relSlash := filepath.ToSlash(rel)
	if relSlash != ".git" && !strings.HasPrefix(relSlash, ".git/") {
		return
	}
	g.checkpoints.Schedule(gitDir, func() {
		ctx, cancel := context.WithTimeout(context.Background(), gitCheckpointTimeout)
		defer cancel()
		if err := g.checkpointWorkspace(ctx, w, gitDir); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: git state checkpoint failed for workspace %s: %v\n", w.id, err)
		}
	})
}

// checkpointWorkspace packs local objects, stores the pack, then
// archives+stores the state without objects.
func (g *Git) checkpointWorkspace(ctx context.Context, w *workspace, gitDir string) error {
	if g.storeView() == nil {
		return nil
	}
	if _, err := os.Stat(gitDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	g.ckptMu.Lock()
	defer g.ckptMu.Unlock()
	baseCommit := strings.ToLower(strings.TrimSpace(w.head()))
	pack, err := g.buildLocalGitObjectPack(ctx, gitDir, baseCommit)
	if err != nil {
		return err
	}
	if len(pack) > 0 {
		if _, err := g.putObjectPack(ctx, w.id, pack); err != nil {
			return err
		}
	}
	content, err := archiveLocalGitStateDir(gitDir)
	if err != nil {
		return err
	}
	return g.putGitState(ctx, w.id, w.head(), content)
}

// checkpointAllGitWorkspaces flushes every workspace's state (the shutdown
// boundary).
func (g *Git) checkpointAllGitWorkspaces() {
	if g == nil || g.localRoot == "" {
		return
	}
	g.mu.RLock()
	workspaces := make([]*workspace, 0, len(g.workspaces))
	workspaces = append(workspaces, g.workspaces...)
	g.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), gitCheckpointTimeout)
	defer cancel()
	for _, w := range workspaces {
		if err := g.checkpointWorkspace(ctx, w, g.gitDirForWorkspace(w)); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: git state final checkpoint failed for workspace %s: %v\n", w.id, err)
		}
	}
}

// restoreWorkspaceState restores objects from the remote clone, then the
// stored packs, then the state archive, swapped in atomically. Linked
// worktrees restore into the common gitdir's worktrees/<name> and rewrite
// their .git pointer file.
func (g *Git) restoreWorkspaceState(ctx context.Context, w *workspace) error {
	if g.storeView() == nil {
		return nil
	}
	state, ok, err := g.getGitState(ctx, w.id)
	if err != nil || !ok {
		return err
	}
	if w.linked() && w.worktree() != "" {
		return g.restoreLinkedWorktreeState(ctx, w, state)
	}
	gitDir := g.localOverlayGitDir(w.rootPath())
	parent := filepath.Dir(gitDir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmpRoot, err := os.MkdirTemp(parent, ".drive9-git-state-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpRoot) }()
	tmpGitDir := filepath.Join(tmpRoot, ".git")
	if state.storageType == gitStateStorageTarGzNoObjects {
		if err := g.restoreGitObjectsFromRemote(ctx, w, tmpGitDir); err != nil {
			return err
		}
		if err := g.restoreGitObjectPacks(ctx, w.id, tmpGitDir); err != nil {
			return err
		}
	}
	if err := extractGitArchive(state.content, tmpGitDir); err != nil {
		return err
	}
	if !gitDirLooksUsable(ctx, tmpGitDir) {
		return fmt.Errorf("restored git state is missing a usable HEAD")
	}
	if err := os.RemoveAll(gitDir); err != nil {
		return err
	}
	return os.Rename(tmpGitDir, gitDir)
}

// restoreLinkedWorktreeState re-materializes one linked worktree: the
// stored archive lands under the common gitdir's worktrees/<name>, and
// the work-tree .git pointer file is rewritten to it.
func (g *Git) restoreLinkedWorktreeState(ctx context.Context, w *workspace, state storedGitState) error {
	common, err := g.commonGitDirFor(w)
	if err != nil {
		return err
	}
	target := filepath.Join(common, "worktrees", w.worktree())
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(target), ".drive9-git-wt-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	if state.storageType == gitStateStorageTarGzNoObjects {
		if err := g.restoreGitObjectPacks(ctx, w.id, tmp); err != nil {
			return err
		}
	}
	if err := extractGitArchive(state.content, tmp); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(tmp, "HEAD")); err != nil {
		return fmt.Errorf("restored linked worktree state is missing HEAD")
	}
	if err := os.RemoveAll(target); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	pointer := g.localOverlayGitDir(w.rootPath())
	if err := os.MkdirAll(filepath.Dir(pointer), 0o755); err != nil {
		return err
	}
	_ = os.Remove(pointer)
	return os.WriteFile(pointer, []byte("gitdir: "+target+"\n"), 0o644)
}

// restoreGitObjectsFromRemote clones --no-checkout from the workspace's
// repo URL into the temp gitdir.
func (g *Git) restoreGitObjectsFromRemote(ctx context.Context, w *workspace, gitDir string) error {
	repoURL := strings.TrimSpace(w.repo())
	if repoURL == "" {
		return fmt.Errorf("git workspace %s has no repo URL for object restore", w.id)
	}
	worktreeRoot := filepath.Dir(gitDir)
	parent := filepath.Dir(worktreeRoot)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmpWorktree, err := os.MkdirTemp(parent, ".drive9-git-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpWorktree) }()
	args := []string{"clone", "--no-checkout"}
	if w.blobless() {
		args = append(args, "--filter=blob:none")
	}
	args = append(args, repoURL, tmpWorktree)
	cmd := exec.CommandContext(ctx, "git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("restore git objects from remote: %w: %s", err, msg)
		}
		return fmt.Errorf("restore git objects from remote: %w", err)
	}
	tmpGit := filepath.Join(tmpWorktree, ".git")
	if err := os.MkdirAll(worktreeRoot, 0o755); err != nil {
		return err
	}
	if err := os.RemoveAll(gitDir); err != nil {
		return err
	}
	return os.Rename(tmpGit, gitDir)
}

// restoreGitObjectPacks fetches and unpacks every stored object pack
// (checksum + size verified).
func (g *Git) restoreGitObjectPacks(ctx context.Context, id, gitDir string) error {
	packs, err := g.listObjectPacks(ctx, id)
	if err != nil {
		return err
	}
	for _, pack := range packs {
		if len(pack.content) == 0 {
			continue
		}
		if pack.sizeBytes != 0 && pack.sizeBytes != int64(len(pack.content)) {
			return fmt.Errorf("git object pack %s size mismatch", pack.packID)
		}
		sum := sha256.Sum256(pack.content)
		checksum := hex.EncodeToString(sum[:])
		if pack.checksumSHA256 != "" && !strings.EqualFold(pack.checksumSHA256, checksum) {
			return fmt.Errorf("git object pack %s checksum mismatch", pack.packID)
		}
		cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "unpack-objects", "-q")
		cmd.Stdin = bytes.NewReader(pack.content)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg != "" {
				return fmt.Errorf("unpack git object pack %s: %w: %s", pack.packID, err, msg)
			}
			return fmt.Errorf("unpack git object pack %s: %w", pack.packID, err)
		}
	}
	return nil
}

// MaterializeHeadTree prefetches the HEAD tree's blobs for a restored
// blobless workspace: git clones with --filter=blob:none leave blobs
// remote; checkout and indexing need them local, so walk the tree and
// hydrate every blob before the mount serves.
func (g *Git) MaterializeHeadTree(ctx context.Context, gitDir string) error {
	tree, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "rev-parse", "--verify", "HEAD^{tree}")
	if err != nil {
		if isMissingGitObjectError(err) {
			return nil
		}
		return err
	}
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "ls-tree", "-r", "-z", strings.TrimSpace(string(tree)))
	if err != nil {
		return err
	}
	var blobs []string
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[:tab]))
		// ls-tree records are `<mode> <type> <oid>`: the type is the
		// second field and the object id the third — testing meta[0]
		// (the mode, always 100644/100755/120000 here) matched nothing
		// and hydration silently skipped every blob.
		if len(meta) >= 3 && meta[1] == "blob" {
			blobs = append(blobs, strings.ToLower(meta[2]))
		}
	}
	if len(blobs) == 0 {
		return nil
	}
	// git cat-file --batch against a partial clone fetches each missing
	// blob lazily (GIT_NO_LAZY_FETCH deliberately unset here).
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "--batch", "--buffer")
	cmd.Stdin = strings.NewReader(strings.Join(blobs, "\n") + "\n")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("materialize head tree: %w: %s", err, msg)
		}
		return fmt.Errorf("materialize head tree: %w", err)
	}
	return nil
}

// isGitConfigStatePath reports a git config file inside a state archive.
func isGitConfigStatePath(rel string) bool {
	rel = filepath.ToSlash(rel)
	return rel == "config" || strings.HasSuffix(rel, "/config")
}
