// Local git-state checkpoint machinery: schedule/checkpoint/restore of a
// workspace's .git directory — object packing with escalating fallbacks,
// the object-without-state archive, and the atomic restore (remote clone,
// stored packs, archive, then the HEAD-tree hydration for blobless
// workspaces). The per-node overlay pruning of the workspace view is
// approximated by the workspace HeadCommit cutoff, which bounds the object
// walk the same way for a restored workspace.
package git

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	gitLocalObjectMaxPackByte = 256 << 20

	gitStateStorageTarGzNoObjects = "tar.gz-no-objects"
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
// fallbacks against the pack budget. complete reports whether BOTH sets made
// it into the returned pack: a checkpoint that archives the index/refs while
// some of their objects were dropped publishes an unrestorable state (git
// diff --cached then reports missing objects).
func (g *Git) buildLocalGitObjectPack(ctx context.Context, gitDir, baseCommit string) (pack []byte, complete bool, err error) {
	refObjects, refsOversize, err := collectLocalRefObjectSet(ctx, gitDir, baseCommit)
	if err != nil {
		return nil, false, err
	}
	if refsOversize {
		refObjects = nil
	}
	stagedObjects, err := collectStagedObjectSet(ctx, gitDir)
	if err != nil {
		return nil, false, err
	}
	fits := func(p []byte) bool { return int64(len(p)) <= gitLocalObjectMaxPackByte }
	if !refsOversize {
		if pack, err = packGitObjects(ctx, gitDir, mergeObjectSets(refObjects, stagedObjects)); err != nil {
			return nil, false, err
		}
		if fits(pack) {
			return pack, true, nil
		}
	}
	refsCovered := false
	if !refsOversize {
		if pack, err = packGitObjects(ctx, gitDir, refObjects); err != nil {
			return nil, false, err
		}
		refsCovered = fits(pack)
	}
	stagedCovered := len(stagedObjects) == 0
	if !stagedCovered {
		stagedPack, serr := packGitObjects(ctx, gitDir, stagedObjects)
		if serr != nil {
			return nil, false, serr
		}
		stagedCovered = fits(stagedPack)
		if refsCovered {
			// Keep the refs pack; completeness already failed on the
			// staged set above.
			return pack, false, nil
		}
		pack = stagedPack
	}
	if !fits(pack) {
		pack = nil
	}
	return pack, refsCovered && stagedCovered, nil
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
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
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

// gitDirFor maps a workspace root onto the local overlay's .git directory.
// The overlay roots view paths under <localRoot>/overlay (localize's
// NewOverlay), so the restored state and git's own file traffic must land
// there — the CLI's LocalGitDir derivation expects the same layout.
func (g *Git) gitDirFor(root string) string {
	local := strings.TrimSuffix(strings.TrimSpace(root), "/")
	if local == "" {
		local = "/"
	}
	if !strings.HasPrefix(local, "/") {
		local = "/" + local
	}
	if local == "/" {
		return filepath.Join(g.localRoot, "overlay", ".git")
	}
	return filepath.Join(g.localRoot, "overlay", local[1:], ".git")
}

// checkpointGitDir resolves the directory a checkpoint must archive. A
// linked worktree's <root>/.git is a POINTER FILE: archiving gitDirFor(root)
// walked a single file, skipped the root node and produced an empty archive
// that then overwrote the server's checkpoint. The real admin dir lives
// under the common repository.
func (g *Git) checkpointGitDir(w *workspace) (string, bool) {
	if w == nil {
		return "", false
	}
	if !w.linked() {
		return g.gitDirFor(w.rootPath()), true
	}
	common := g.workspaceByID(w.commonWorkspaceID())
	if common == nil || common.id == w.id {
		return "", false
	}
	target, err := linkedWorktreeGitDir(g.gitDirFor(common.rootPath()), w.gitDirRelPath())
	if err != nil {
		return "", false
	}
	return target, true
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
	gitDir, ok := g.checkpointGitDir(w)
	if !ok {
		return
	}
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

// checkpointWorkspace packs local objects, uploads the pack, then
// archives+uploads the state without objects.
func (g *Git) checkpointWorkspace(ctx context.Context, w *workspace, gitDir string) error {
	if g.cl == nil {
		return nil
	}
	if info, err := os.Stat(gitDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	} else if !info.IsDir() {
		// A pointer file (a linked worktree rooted at an unresolved path)
		// would archive nothing and overwrite a good checkpoint.
		return fmt.Errorf("git: checkpoint %s: %s is not a git directory", w.id, gitDir)
	}
	g.ckptMu.Lock()
	defer g.ckptMu.Unlock()
	baseCommit := strings.ToLower(strings.TrimSpace(w.head()))
	pack, complete, err := g.buildLocalGitObjectPack(ctx, gitDir, baseCommit)
	if err != nil {
		return err
	}
	if !complete {
		// Refuse rather than publish an archive whose index/refs reference
		// objects outside the pack budget: the previous checkpoint stays
		// and restoration remains possible.
		return fmt.Errorf("git: checkpoint %s: local object set exceeds the pack budget; refusing to publish an unrestorable state", w.id)
	}
	if len(pack) > 0 {
		if _, err := g.cl.PutGitObjectPack(ctx, w.id, client.GitObjectPackRequest{Content: pack}); err != nil {
			return err
		}
	}
	content, err := archiveLocalGitStateDir(gitDir)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(content)
	_, err = g.cl.UpsertGitState(ctx, w.id, client.GitStateRequest{
		CheckpointCommit: w.head(),
		StorageType:      gitStateStorageTarGzNoObjects,
		ChecksumSHA256:   hex.EncodeToString(sum[:]),
		SizeBytes:        int64(len(content)),
		Content:          content,
	})
	return err
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
		gitDir, ok := g.checkpointGitDir(w)
		if !ok {
			continue
		}
		if err := g.checkpointWorkspace(ctx, w, gitDir); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: git state final checkpoint failed for workspace %s: %v\n", w.id, err)
		}
	}
}

// restoreWorkspaceState restores objects from the remote clone, then the
// stored packs, then the state archive, swapped in atomically.
// errGitStateNotReady reports that no server-side checkpoint exists yet —
// the discovery-time restore racing the CLI's upload. Callers retry later
// instead of consuming their restore attempt.
var errGitStateNotReady = errors.New("git state checkpoint not available yet")

func (g *Git) restoreWorkspaceState(ctx context.Context, w *workspace) error {
	if g.cl == nil {
		return nil
	}
	gitDir := g.gitDirFor(w.rootPath())
	// A linked worktree's state is its gitdir under the COMMON repository
	// plus a .git pointer file at the root. Unpacking the archive as a
	// standalone .git dropped the commondir relationship (the CLI still
	// registers linked workspaces and uploads a relative commondir).
	linkedCommonGitDir := ""
	if w.linked() {
		common := g.workspaceByID(w.commonWorkspaceID())
		if common == nil || common.id == w.id {
			// The common workspace has not been discovered yet: retry on
			// the next load instead of restoring a broken standalone repo.
			return errGitStateNotReady
		}
		if err := g.ensureWorkspaceState(ctx, common); err != nil {
			return err
		}
		linkedCommonGitDir = g.gitDirFor(common.rootPath())
		target, lerr := linkedWorktreeGitDir(linkedCommonGitDir, w.gitDirRelPath())
		if lerr != nil {
			return lerr
		}
		gitDir = target
	}
	// v1 ensureGitStateRestored: a usable local .git owns the truth — a
	// live `drive9 git clone --fast` on THIS mount built it, and the
	// server's checkpoint lags the live writes. Swapping here would wipe
	// post-clone .git changes (the identity config, exercise commits)
	// back to the clone-time archive. Only a missing or unusable .git
	// (a fresh-LocalRoot remount) downloads and swaps the state.
	if gitDirLooksUsable(ctx, gitDir) {
		if linkedCommonGitDir == "" {
			return nil
		}
		if !g.wasFreshRestored(gitDir) {
			// A LIVE local checkout built on this mount owns the truth; only
			// the .git pointer file may be missing.
			return writeWorktreeGitPointer(g, w.rootPath(), gitDir)
		}
		// The admin dir came from the COMMON repository's archive moments
		// ago: it has a usable HEAD but not the linked workspace's own
		// checkpoint or staged-object pack — fall through and restore them.
	}
	state, err := g.cl.GetGitState(ctx, w.id)
	if err != nil {
		if client.IsNotFound(err) {
			return errGitStateNotReady
		}
		return err
	}
	if state == nil {
		return errGitStateNotReady
	}
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
	if linkedCommonGitDir != "" {
		// A linked worktree gitdir sits at <common>/.git/worktrees/<name>
		// and resolves its shared state through the RELATIVE commondir
		// "../..": the staging dir must keep the same depth, or the
		// validation below would follow the pointer into the wrong tree.
		tmpGitDir = tmpRoot
	}
	if state.StorageType == gitStateStorageTarGzNoObjects {
		if linkedCommonGitDir == "" {
			// Standalone: objects come from the remote clone.
			if err := g.restoreGitObjectsFromRemote(ctx, w, tmpGitDir); err != nil {
				return err
			}
		}
		// A linked worktree shares the common repository's object store:
		// the pack lands THERE, never in the worktree gitdir (which git
		// resolves through commondir).
		packDir := tmpGitDir
		if linkedCommonGitDir != "" {
			packDir = linkedCommonGitDir
		}
		if err := g.restoreGitObjectPacks(ctx, w, packDir); err != nil {
			return err
		}
	}
	if err := extractGitArchive(state.Content, tmpGitDir); err != nil {
		return err
	}
	if linkedCommonGitDir != "" {
		// The worktree gitdir must resolve its shared state through the
		// common .git; an archive that predates commondir gets the
		// canonical relative pointer for <common>/.git/worktrees/<name>.
		if err := ensureWorktreeCommondir(tmpGitDir); err != nil {
			return err
		}
	}
	if !gitDirLooksUsable(ctx, tmpGitDir) {
		return fmt.Errorf("restored git state is missing a usable HEAD")
	}
	if err := os.RemoveAll(gitDir); err != nil {
		return err
	}
	if err := os.Rename(tmpGitDir, gitDir); err != nil {
		return err
	}
	if linkedCommonGitDir != "" {
		return writeWorktreeGitPointer(g, w.rootPath(), gitDir)
	}
	g.markFreshRestored(gitDir)
	return nil
}

// markFreshRestored records the worktree admin dirs a restore just
// materialized from the common repository's archive.
func (g *Git) markFreshRestored(gitDir string) {
	entries, err := os.ReadDir(filepath.Join(gitDir, "worktrees"))
	if err != nil {
		return
	}
	g.mu.Lock()
	if g.freshRestored == nil {
		g.freshRestored = map[string]bool{}
	}
	for _, e := range entries {
		if e.IsDir() {
			g.freshRestored[filepath.Clean(filepath.Join(gitDir, "worktrees", e.Name()))] = true
		}
	}
	g.mu.Unlock()
}

// wasFreshRestored reports whether a restore in this process created the
// worktree admin dir (as opposed to a live local checkout).
func (g *Git) wasFreshRestored(dir string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.freshRestored[filepath.Clean(dir)]
}

// linkedWorktreeGitDir resolves a linked worktree's gitdir from the common
// repository's .git and the registered relative gitdir (".git/worktrees/w1").
func linkedWorktreeGitDir(commonGitDir, gitDirRel string) (string, error) {
	rel := filepath.Clean(strings.TrimSpace(filepath.FromSlash(gitDirRel)))
	rel = strings.TrimPrefix(rel, ".git"+string(filepath.Separator))
	if rel == "" || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("git: linked workspace has no usable gitdir (%q)", gitDirRel)
	}
	target := filepath.Join(commonGitDir, rel)
	// The resolved path must stay inside the common .git.
	if !strings.HasPrefix(target, filepath.Clean(commonGitDir)+string(filepath.Separator)) {
		return "", fmt.Errorf("git: linked workspace gitdir %q escapes the common repository", gitDirRel)
	}
	return target, nil
}

// ensureWorktreeCommondir writes the canonical relative commondir for a
// gitdir that sits at <common>/.git/worktrees/<name>.
func ensureWorktreeCommondir(gitDir string) error {
	path := filepath.Join(gitDir, "commondir")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(path, []byte("../..\n"), 0o644)
}

// writeWorktreeGitPointer turns the linked root's .git into the pointer FILE
// git expects (a linked worktree never owns a .git directory).
func writeWorktreeGitPointer(g *Git, root, linkedGitDir string) error {
	if g.localRoot == "" {
		return nil
	}
	viewRoot := strings.TrimSuffix(root, "/")
	pointer := filepath.Join(g.localRoot, "overlay", strings.TrimPrefix(viewRoot, "/"), ".git")
	if err := os.MkdirAll(filepath.Dir(pointer), 0o755); err != nil {
		return err
	}
	if info, err := os.Stat(pointer); err == nil && info.IsDir() {
		if err := os.RemoveAll(pointer); err != nil {
			return err
		}
	}
	return os.WriteFile(pointer, []byte("gitdir: "+linkedGitDir+"\n"), 0o644)
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
func (g *Git) restoreGitObjectPacks(ctx context.Context, w *workspace, gitDir string) error {
	packs, err := g.cl.ListGitObjectPacks(ctx, w.id)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, meta := range packs {
		pack, err := g.cl.GetGitObjectPack(ctx, w.id, meta.PackID)
		if err != nil {
			return err
		}
		if len(pack.Content) == 0 {
			continue
		}
		if pack.SizeBytes != 0 && pack.SizeBytes != int64(len(pack.Content)) {
			return fmt.Errorf("git object pack %s size mismatch", pack.PackID)
		}
		sum := sha256.Sum256(pack.Content)
		checksum := hex.EncodeToString(sum[:])
		if pack.ChecksumSHA256 != "" && !strings.EqualFold(pack.ChecksumSHA256, checksum) {
			return fmt.Errorf("git object pack %s checksum mismatch", pack.PackID)
		}
		cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "unpack-objects", "-q")
		cmd.Stdin = bytes.NewReader(pack.Content)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg != "" {
				return fmt.Errorf("unpack git object pack %s: %w: %s", pack.PackID, err, msg)
			}
			return fmt.Errorf("unpack git object pack %s: %w", pack.PackID, err)
		}
	}
	return nil
}

// MaterializeHeadTree prefetches the HEAD tree's blobs for a restored
// blobless workspace: git clones with --filter=blob:none leave blobs
// remote; checkout and indexing need them local, so walk the tree and
// hydrate every blob before the mount serves.
func (g *Git) MaterializeHeadTree(ctx context.Context, gitDir string) error {
	if g.cl == nil {
		return nil
	}
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
