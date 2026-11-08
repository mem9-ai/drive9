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
	"github.com/mem9-ai/drive9/pkg/gitcache"
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

// shouldSkipGitObjectStatePath reports paths a checkpoint archive never needs to
// carry. objects/ travels as packs, and hooks/ are executables that only the
// mounting host could run: uploading them would ship code to the server and the
// restore path strips them anyway.
func shouldSkipGitObjectStatePath(rel string, _ fs.DirEntry) bool {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) == 0 {
		return false
	}
	// objects/ and hooks/ are top-level .git directories. Matching the name at
	// any depth used to drop legitimate content such as refs/heads/hooks.
	return parts[0] == "objects" || parts[0] == "hooks"
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
	// The checkpoint archive is tenant-writable, but it is restored as a live
	// .git directory on the mount host. Anything in it that git would execute —
	// hooks, a core.fsmonitor/pager/sshCommand, a filter driver, an
	// include.path — would run as the user running this process on the next
	// git command, so the executable surface is stripped before the directory
	// is put into service.
	if err := sanitizeGitCheckpointDir(tmpGitDir); err != nil {
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
	// The URL came from a tenant request. Re-validate here (the API already
	// did) so no other caller path can clone an ext:: transport or a local
	// repository, and pin the transport list git itself accepts.
	if err := gitcache.ValidateRepoURL(repoURL); err != nil {
		return fmt.Errorf("git workspace %s repo URL rejected: %w", w.id, err)
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
	cmd.Env = append(gitSafeEnv(os.Environ()), "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL="+gitcache.GitAllowedProtocols)
	if gitcache.GitAllowLocalRepos() {
		cmd.Env[len(cmd.Env)-1] = "GIT_ALLOW_PROTOCOL=" + gitcache.GitLocalAllowedProtocols
	}
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

// gitCheckpointSafeConfigKeys are the only config keys a restored checkpoint
// may contribute to git's behaviour. Everything else is dropped: the archive is
// tenant-writable, and a .git/config key can make git execute arbitrary
// commands (core.fsmonitor, core.pager, core.sshCommand, core.askPass,
// core.editor, core.alternateRefsCommand, filter.*.clean/smudge,
// diff.*.textconv, merge.*.driver, include.path, url.*.insteadOf, ...).
var gitCheckpointSafeConfigKeys = map[string]bool{
	"core.repositoryformatversion": true,
	"core.filemode":                true,
	"core.bare":                    true,
	"core.logallrefupdates":        true,
	"core.ignorecase":              true,
	"core.precomposeunicode":       true,
	"core.symlinks":                true,
	// core.worktree is deliberately absent: it redirects the repository's
	// worktree to an arbitrary host path, which tenant-controlled checkpoint
	// state must never be able to do.
	"extensions.objectformat": true,
	// extensions.worktreeConfig is deliberately absent as well: it moves core.*
	// keys into a per-worktree config file, so keeping it let a checkpoint
	// decide which files git reads next — including
	// <gitdir>/worktrees/<name>/config.worktree, which the sanitizer only
	// started rewriting below. A restore leaves the extension off; the
	// workspace can re-enable it once its config files are known clean.
	"gc.autodetach": true,
}

// gitCheckpointSafeConfigSections are the config sections a checkpoint may
// keep. Hooks and command-bearing sections are excluded by construction; a
// section not listed here is dropped wholesale.
var gitCheckpointSafeConfigSections = map[string]bool{
	"remote": true,
	"branch": true,
	"tag":    true,
	"user":   true,
}

// gitCheckpointUnsafeRemoteKeys are remote-section keys that make git run a
// program rather than talk to a URL.
var gitCheckpointUnsafeRemoteKeys = map[string]bool{
	"vcs":         true, // remote.<name>.vcs invokes git-remote-<vcs>
	"uploadpack":  true,
	"receivepack": true,
	"proxy":       true,
}

// gitConfigValueState carries git's parser state across continued value lines.
// Quote state is not per line: git keeps its `quote` flag while consuming a
// backslash-newline, so a value that opens a quote on one physical line and
// closes it on the next must be scanned with that quote still open.
type gitConfigValueState struct {
	inQuotes bool
}

// continues reports whether a physical line ends inside a value that a trailing
// backslash continues, and advances the carried quote state.
//
// Git ends a value at an unquoted '#' or ';' (the rest of the line is a comment)
// and only a single trailing backslash continues the value; "\\" is an escaped
// literal. Treating any trailing backslash as a continuation let a comment end
// the value while the sanitizer still swallowed — and emitted verbatim — the
// following physical line, which is how a "[core] fsmonitor = ..." line could
// slip past the per-key policy. Resetting the quote flag per line reintroduced
// the same hole through a continuation: in `name = "example\` followed by
// `" # \`, git closes the quote and starts a comment, while a fresh scan treats
// the quote as opening a new string and reports the comment's backslash as a
// continuation. fresh starts a new value and clears that carried state.
func (s *gitConfigValueState) continues(line string, fresh bool) bool {
	if fresh {
		s.inQuotes = false
	}
	escaped := false
	trailingBackslashes := 0
	for i := 0; i < len(line); i++ {
		c := line[i]
		if escaped {
			escaped = false
			trailingBackslashes = 0
			continue
		}
		switch c {
		case '\\':
			escaped = true
			trailingBackslashes++
		case '"':
			s.inQuotes = !s.inQuotes
			trailingBackslashes = 0
		case '#', ';':
			if !s.inQuotes {
				// The value ends here; anything after the marker is a comment,
				// including a backslash.
				s.inQuotes = false
				return false
			}
			trailingBackslashes = 0
		case ' ', '\t', '\r':
			if !s.inQuotes {
				trailingBackslashes = 0
			}
		default:
			trailingBackslashes = 0
		}
	}
	if trailingBackslashes%2 == 1 {
		return true
	}
	// The value ends on this line, so it carries no state into the next one.
	s.inQuotes = false
	return false
}

// splitGitConfigKeyValue splits one config line into its lower-cased key and its
// trimmed value (empty when the line has no "=").
func splitGitConfigKeyValue(line string) (string, string) {
	key := line
	value := ""
	if i := strings.IndexByte(line, '='); i >= 0 {
		key = line[:i]
		value = strings.TrimSpace(line[i+1:])
	}
	return strings.ToLower(strings.TrimSpace(key)), value
}

// gitCheckpointSanitizer walks the restored .git tree once and sanitises every
// directory git reads configuration or hooks from.
type gitCheckpointSanitizer struct {
	// root is the extracted tree. A commondir pointer that resolves outside it is
	// dropped rather than followed.
	root string
	// walked keeps the traversal linear when a commondir pointer leads back into a
	// subtree that was already descended.
	walked map[string]struct{}
	// sanitized records which directories have had their config and hooks handled.
	// It is separate from walked: a directory can be walked first as a submodule
	// path component (and only sanitised if it is a repository) and later be named
	// by another repository's commondir, at which point it is an admin directory
	// and has to be sanitised whatever the walk already did with it.
	sanitized map[string]struct{}
}

// sanitizeGitCheckpointDir removes the executable surface of a restored .git
// directory: the hooks of every repository it holds and every config key or
// section that can make git run a program. It is applied to the freshly extracted
// tree, before the directory is renamed into place.
func sanitizeGitCheckpointDir(gitDir string) error {
	if gitDir == "" {
		return nil
	}
	w := &gitCheckpointSanitizer{root: filepath.Clean(gitDir), walked: map[string]struct{}{}, sanitized: map[string]struct{}{}}
	return w.repoDir(gitDir)
}

// repoDir sanitises one repository admin directory: the config files git reads
// from it, its hooks, the common directory its commondir names, the admin
// directory of each linked worktree and every submodule repository below it.
//
// It is only called for a directory that is an admin directory — by position for
// the restore root and for worktrees/<name>, by isGitCheckpointRepoDir below
// modules/ — which is why it can rewrite "config" unconditionally. Rewriting every
// file named config in the tree would not be equivalent: refs/heads/config is a
// loose ref, refs/heads/config/topic is a directory, and logs/…/config is a reflog
// entry, and none of them is configuration.
func (w *gitCheckpointSanitizer) repoDir(dir string) error {
	dir = filepath.Clean(dir)
	if err := w.sanitizeAdminDir(dir); err != nil {
		return err
	}
	if w.markWalked(dir) {
		return nil
	}
	worktrees, err := checkpointAdminDirs(filepath.Join(dir, "worktrees"))
	if err != nil {
		return err
	}
	for _, worktree := range worktrees {
		if err := w.repoDir(worktree); err != nil {
			return err
		}
	}
	modules, err := checkpointAdminDirs(filepath.Join(dir, "modules"))
	if err != nil {
		return err
	}
	for _, module := range modules {
		if err := w.submodulePath(module); err != nil {
			return err
		}
	}
	return nil
}

// submodulePath sanitises one directory below a modules/ registry. A submodule's
// admin path is <registry>/<name>, and <name> is its path in the superproject, so a
// directory here can be a repository admin directory, a path component of a deeper
// name, or both (submodules "libs" and "libs/foo" can coexist).
//
// A directory that is not a repository keeps every child: inside a repository below
// modules/ an entry named refs/ or objects/ looks like storage, but it can equally
// be the next component of a submodule name, and a checkpoint that names a submodule
// after git's own directories must not be able to hide it from the walk.
func (w *gitCheckpointSanitizer) submodulePath(dir string) error {
	dir = filepath.Clean(dir)
	if isGitCheckpointRepoDir(dir) {
		if err := w.sanitizeAdminDir(dir); err != nil {
			return err
		}
	}
	if w.markWalked(dir) {
		return nil
	}
	// Every child is kept, whatever it is called. Inside a repository below
	// modules/ an entry named refs/ or objects/ looks like that repository's own
	// storage, but git places a submodule "ns/objects/dep" at exactly
	// modules/ns/objects/dep, and a checkpoint that names a submodule after git's
	// own directories must not be able to hide it from the walk.
	children, err := checkpointAdminDirs(dir)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := w.submodulePath(child); err != nil {
			return err
		}
	}
	return nil
}

// sanitizeAdminDir rewrites dir's config files, scrubs its hooks and sanitises the
// common directory its commondir names. It is idempotent, and separate from the
// traversal: being walked as a path component says nothing about whether the
// directory's administrative files were handled.
func (w *gitCheckpointSanitizer) sanitizeAdminDir(dir string) error {
	if _, ok := w.sanitized[dir]; ok {
		return nil
	}
	w.sanitized[dir] = struct{}{}
	if err := sanitizeGitCheckpointRepoFiles(dir); err != nil {
		return err
	}
	return w.commonDir(dir)
}

// markWalked records that dir has been descended, so a commondir pointer that
// leads back into the tree cannot walk a subtree twice or cycle forever.
func (w *gitCheckpointSanitizer) markWalked(dir string) bool {
	if _, ok := w.walked[dir]; ok {
		return true
	}
	w.walked[dir] = struct{}{}
	return false
}

// commonDir sanitises the directory a worktree's commondir pointer names. Git reads
// the shared config and default hooks from there, so a checkpoint could keep them out
// of the walk by putting them in a directory that has no HEAD of its own.
//
// The pointer has to be resolved the way git resolves it, or the sanitizer rewrites a
// different directory than git reads: git strips only the trailing line terminator (a
// leading space is part of the name) and resolves the result with realpath, so
// symlinks inside the path are followed. Both disagreements were live bypasses.
func (w *gitCheckpointSanitizer) commonDir(dir string) error {
	raw, err := os.ReadFile(filepath.Join(dir, "commondir"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint commondir: %w", err)
	}
	target, resolved := w.resolveCommonDir(dir, string(raw))
	switch {
	case resolved && target != nil:
		return w.repoDir(*target)
	case resolved:
		// The pointer names a directory outside the restored tree: following it
		// would let a checkpoint point git at a host directory's config and hooks.
		// The restore writes the canonical relative pointer for a linked worktree,
		// so the file is dropped instead of trusted.
		if err := os.Remove(filepath.Join(dir, "commondir")); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove checkpoint commondir: %w", err)
		}
	default:
		// The value does not resolve to a directory. Git then refuses the whole
		// directory ("fatal: not a git repository"), so nothing can be read or run
		// through it, and the file is left alone: a submodule may legitimately have
		// related branches named HEAD and commondir, whose loose refs live under
		// refs/heads/<name>/commondir and must keep their bytes.
		return nil
	}
	return nil
}

// resolveCommonDir reproduces git's commondir resolution: strip the trailing CR/LF
// only, join with the gitdir, then resolve the real path (symlinks included). The
// second result reports whether the value resolved at all; a nil target means it
// resolved outside the extracted tree.
func (w *gitCheckpointSanitizer) resolveCommonDir(dir, value string) (*string, bool) {
	value = strings.TrimRight(value, "\r\n")
	if value == "" {
		return nil, false
	}
	path := filepath.FromSlash(value)
	if !filepath.IsAbs(path) {
		// Concatenate rather than filepath.Join: Join cleans the path lexically, so
		// "objects/link/../shared" would become "objects/shared" before the symlink
		// in it is ever resolved, which is exactly the disagreement with git.
		path = dir + string(filepath.Separator) + path
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, false
	}
	root, err := filepath.EvalSymlinks(w.root)
	if err != nil {
		root = w.root
	}
	if resolved == root || strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		return &resolved, true
	}
	return nil, true
}

// sanitizeGitCheckpointRepoFiles rewrites the config files of one repository admin
// directory and removes its hooks.
func sanitizeGitCheckpointRepoFiles(dir string) error {
	// extensions.worktreeConfig moves core.* keys into config.worktree, so a
	// checkpoint could hide a fsmonitor/pager there instead of in config.
	for _, name := range [...]string{"config", "config.worktree"} {
		if err := sanitizeGitCheckpointConfig(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	if err := scrubGitCheckpointHooks(dir); err != nil {
		return err
	}
	return nil
}

// scrubGitCheckpointHooks removes the programs git would run from dir/hooks. A
// directory can satisfy the repository predicate while being a branch namespace —
// branches "topic/HEAD" and "topic/commondir" do — and that namespace's hooks/ entry
// is a directory of loose refs, not hooks, so entries holding a loose ref (an object
// id or a symref line) keep their bytes. Directories are left alone: git only runs
// the regular files it finds directly in hooks/, never a subdirectory.
func scrubGitCheckpointHooks(dir string) error {
	hooksDir := filepath.Join(dir, "hooks")
	entries, err := os.ReadDir(hooksDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint hooks: %w", err)
	}
	remaining := 0
	for _, entry := range entries {
		path := filepath.Join(hooksDir, entry.Name())
		info, statErr := os.Lstat(path)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return fmt.Errorf("stat checkpoint hook %s: %w", path, statErr)
		}
		if info.IsDir() {
			remaining++
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 {
			// A link would be executed through its target; git resolves hooks by name
			// in this directory, so the link itself goes.
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove checkpoint hook link %s: %w", path, err)
			}
			continue
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read checkpoint hook %s: %w", path, readErr)
		}
		if isGitCheckpointRefContent(data) {
			remaining++
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove checkpoint hook %s: %w", path, err)
		}
	}
	if remaining == 0 {
		if err := os.Remove(hooksDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove checkpoint hooks: %w", err)
		}
	}
	return nil
}

// isGitCheckpointRefContent reports whether a file holds a loose ref (an object id)
// or a symref line rather than a program.
func isGitCheckpointRefContent(data []byte) bool {
	text := strings.TrimRight(string(data), "\r\n")
	if strings.HasPrefix(text, "ref: ") {
		return true
	}
	switch len(text) {
	case 40, 64:
	default:
		return false
	}
	for _, r := range text {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

// checkpointAdminDirs lists the child directories of dir that can be repository
// admin directories. A symbolic link is dropped instead of followed: git resolves
// worktrees/<name> and modules/<name> by name, so a link would point git — and the
// rewrites below it — outside the restored tree. A missing dir yields nothing.
func checkpointAdminDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint directory %s: %w", dir, err)
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		// Lstat rather than the directory-entry type: a filesystem may report an
		// unknown type (Type() == 0), and a link must never be mistaken for the
		// directory it points at.
		info, statErr := os.Lstat(path)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return nil, fmt.Errorf("stat checkpoint entry %s: %w", path, statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("remove checkpoint link %s: %w", path, err)
			}
			continue
		}
		if info.IsDir() {
			out = append(out, path)
		}
	}
	return out, nil
}

// isGitCheckpointRepoDir reports whether dir is laid out the way git accepts a
// repository before reading anything from it: a HEAD file, an objects entry and a
// refs (or reftable) entry — git validates the last two with access(X_OK), so they
// may be regular files as well as directories (an executable refs file is accepted
// by git 2.50.1, which then reads the repository's config), or a commondir file for
// a linked worktree. Existence is a superset of git's check, and sanitising more
// than git accepts is safe; a directory that fails this test is not one git reads
// configuration or hooks from.
func isGitCheckpointRepoDir(dir string) bool {
	head, err := os.Stat(filepath.Join(dir, "HEAD"))
	if err != nil || !head.Mode().IsRegular() {
		return false
	}
	if info, err := os.Stat(filepath.Join(dir, "commondir")); err == nil && info.Mode().IsRegular() {
		return true
	}
	if _, err := os.Stat(filepath.Join(dir, "objects")); err != nil {
		return false
	}
	for _, name := range [...]string{"refs", "reftable"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// sanitizeGitCheckpointConfig rewrites a git config file keeping only the
// sections and keys that cannot execute a program.
func sanitizeGitCheckpointConfig(path string) error {
	if info, err := os.Lstat(path); err == nil {
		// A checkpoint can ship the file as a symlink that resolves inside the
		// extraction root; rewriting through it would edit whatever it points at
		// instead of the config git is about to read. No restore needs a linked
		// config, so it is dropped rather than followed.
		if info.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove checkpoint config symlink: %w", err)
			}
			return nil
		}
		// A directory named config (refs/heads/config when a branch is called
		// config/topic) is not a config file, and reading it would fail with
		// EISDIR and abort the restore.
		if !info.Mode().IsRegular() {
			return nil
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint config: %w", err)
	}
	// Git accepts a UTF-8 byte-order mark at the start of a config file and parses
	// the rest, so the mark is stripped before the format check and the rewrite.
	// Leaving it would let a BOM-prefixed config skip sanitising entirely.
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	// Git requires section headers, so anything that is not a config file at all
	// (a loose ref, a reflog entry, a payload a checkpoint happened to name
	// config) is left exactly as it is instead of being rewritten into an empty
	// file. A file git would accept as config always starts with a section or a
	// comment, so this cannot skip a config git would read.
	if !looksLikeGitConfig(data) {
		return nil
	}
	var out strings.Builder
	sectionName := ""
	sectionKept := false
	// continued: the previous emitted line ended with a backslash, so this
	// physical line is part of a value that was kept.
	// dropped: the previous line was dropped and ended with a backslash, so
	// every physical line of that logical value is dropped too. Without this a
	// continuation line of a rejected value ("uploadpack = x \" followed by
	// "[core] fsmonitor = ...") would be written verbatim and become live
	// configuration under the previous section.
	continued := false
	dropped := false
	// scan carries git's quote state across the physical lines of one value.
	var scan gitConfigValueState
	dropLine := func(trimmed string) {
		dropped = scan.continues(trimmed, true)
		continued = false
	}
	for _, line := range strings.SplitAfter(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.TrimRight(line, "\r\n"))
		if dropped {
			dropped = scan.continues(trimmed, false)
			continue
		}
		if continued {
			// A backslash continuation belongs to the previous value; keep or
			// drop it together with that value so the file stays parseable.
			continued = scan.continues(trimmed, false)
			if sectionKept {
				out.WriteString(line)
			}
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			end := strings.IndexByte(trimmed, ']')
			if end <= 0 {
				// Malformed header: stop keeping anything under it so the
				// parser and the emitted text cannot disagree.
				sectionName, sectionKept, continued = "", false, false
				scan.inQuotes = false
				continue
			}
			header := trimmed[:end+1]
			raw := strings.TrimSpace(trimmed[1:end])
			sectionName = strings.ToLower(raw)
			if i := strings.IndexAny(sectionName, " \t\""); i >= 0 {
				sectionName = sectionName[:i]
			}
			sectionKept = gitCheckpointSafeConfigSections[sectionName] || sectionName == "core" ||
				sectionName == "extensions" || sectionName == "gc"
			continued = false
			dropped = false
			scan.inQuotes = false
			// The header itself is inert, so a kept section always emits it —
			// otherwise the parser state (say, "user") would diverge from the
			// last emitted section (say, "core") and the following lines would
			// land in the wrong section. Trailing content is kept only when it
			// is a comment; a key on the header line is dropped because writing
			// it back would smuggle it past the per-section policy below.
			if sectionKept {
				trailing := strings.TrimSpace(trimmed[end+1:])
				if trailing == "" {
					out.WriteString(line)
				} else if strings.HasPrefix(trailing, "#") || strings.HasPrefix(trailing, ";") {
					out.WriteString(header + " " + trailing + "\n")
				} else {
					out.WriteString(header + "\n")
				}
			}
			continue
		}
		if !sectionKept {
			dropLine(trimmed)
			continue
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
			out.WriteString(line)
			continue
		}
		// Sections kept wholesale (remote/branch/tag/user/init) carry only
		// values; core/extensions/gc are key allowlists because those keys can
		// name a program to run.
		if sectionName == "remote" {
			// A remote section is kept for its URL and fetch refspecs, but some
			// of its keys name a program git runs: remote.<name>.vcs selects a
			// git-remote-<vcs> helper, and uploadpack/receivepack/proxy name
			// commands. Those are dropped, as is any URL that asks for a
			// remote-helper transport.
			key, value := splitGitConfigKeyValue(trimmed)
			if gitCheckpointUnsafeRemoteKeys[key] {
				dropLine(trimmed)
				continue
			}
			if (key == "url" || key == "pushurl") && gitcache.HasRemoteHelperScheme(value) {
				dropLine(trimmed)
				continue
			}
			continued = scan.continues(trimmed, true)
			out.WriteString(line)
			continue
		}
		if gitCheckpointSafeConfigSections[sectionName] {
			continued = scan.continues(trimmed, true)
			out.WriteString(line)
			continue
		}
		key := trimmed
		if i := strings.IndexByte(key, '='); i >= 0 {
			key = key[:i]
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if !gitCheckpointSafeConfigKeys[sectionName+"."+key] {
			dropLine(trimmed)
			continue
		}
		continued = scan.continues(trimmed, true)
		out.WriteString(line)
	}
	if err := os.WriteFile(path, []byte(out.String()), 0o600); err != nil {
		return fmt.Errorf("rewrite checkpoint config: %w", err)
	}
	return nil
}

// looksLikeGitConfig reports whether data can be a git config file: git requires
// section headers, so the first significant byte is a section opener or a comment
// marker. Loose refs (a hex object id) and reflog entries (<sha> <sha> <ident>)
// never match.
func looksLikeGitConfig(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '[', '#', ';':
			return true
		default:
			return false
		}
	}
	return false
}

// gitSafeEnv neutralises ambient git configuration that could make a tenant
// clone execute code: system/global config, hooks, and the credential helpers
// an attacker-controlled URL could steer.
func gitSafeEnv(base []string) []string {
	out := make([]string, 0, len(base)+6)
	for _, kv := range base {
		switch {
		case strings.HasPrefix(kv, "GIT_CONFIG_COUNT="),
			strings.HasPrefix(kv, "GIT_ALLOW_PROTOCOL="),
			strings.HasPrefix(kv, "GIT_TERMINAL_PROMPT="),
			strings.HasPrefix(kv, "GIT_ASKPASS="),
			strings.HasPrefix(kv, "SSH_ASKPASS="):
			continue
		}
		out = append(out, kv)
	}
	// core.hooksPath=/dev/null still leaves no hook to run, and disabling the
	// fsmonitor/askpass hooks keeps a crafted repo config inert.
	return append(out,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_COUNT=2",
		"GIT_CONFIG_KEY_0=core.hooksPath",
		"GIT_CONFIG_VALUE_0=/dev/null",
		"GIT_CONFIG_KEY_1=core.fsmonitor",
		"GIT_CONFIG_VALUE_1=false",
	)
}
