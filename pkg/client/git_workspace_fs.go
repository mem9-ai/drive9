// FS-native git workspace store: the client-side writer/reader of
// /.drive9/git-workspaces/**. Every record the git feature persists —
// the workspace manifest, the lifecycle state, the tracked-path index,
// the dirty mirror, the git object packs, and the .git state archive —
// lives here as plain drive9 files written through the regular fs API.
// The server's /v1/git-workspaces* endpoints are legacy (pre-FS-native
// workspaces) and are no longer consulted by the CLI or the mount.
package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/gitws"
)

// GitWorkspaceFSRoot is the FS-native git workspace store root.
const GitWorkspaceFSRoot = gitws.Root

// GitWorkspaceFSNotFound reports a store miss (no manifest).
type GitWorkspaceFSNotFound struct {
	ID string
}

func (e *GitWorkspaceFSNotFound) Error() string {
	return fmt.Sprintf("git workspace %s not found in %s", e.ID, GitWorkspaceFSRoot)
}

// IsGitWorkspaceFSNotFound distinguishes store misses from other errors.
func IsGitWorkspaceFSNotFound(err error) bool {
	_, ok := err.(*GitWorkspaceFSNotFound)
	return ok
}

func newGitWorkspaceID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate git workspace id: %w", err)
	}
	return "gitws-" + hex.EncodeToString(b[:]), nil
}

// readGitWorkspaceDoc reads one bounded JSON document from the store.
// A missing file returns IsNotFound.
func (c *Client) readGitWorkspaceDoc(ctx context.Context, p string) ([]byte, error) {
	st, err := c.StatCtx(ctx, p)
	if IsNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, fmt.Errorf("stat %s: no result", p)
	}
	if st.Size > gitws.MaxDocBytes {
		return nil, fmt.Errorf("git workspace document %s exceeds %d bytes", p, gitws.MaxDocBytes)
	}
	rc, err := c.ReadStream(ctx, p)
	if IsNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, gitws.MaxDocBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", p, err)
	}
	if int64(len(data)) > gitws.MaxDocBytes {
		return nil, fmt.Errorf("git workspace document %s exceeds %d bytes", p, gitws.MaxDocBytes)
	}
	return data, nil
}

// ensureGitWorkspaceDir mkdirs one store directory (idempotent).
func (c *Client) ensureGitWorkspaceDir(ctx context.Context, dir string) error {
	if dir == "" || dir == "/" {
		return nil
	}
	cur := ""
	for _, seg := range strings.Split(strings.Trim(dir, "/"), "/") {
		if seg == "" {
			continue
		}
		cur = cur + "/" + seg
		if st, err := c.StatCtx(ctx, cur); err == nil {
			if st != nil && !st.IsDir {
				return fmt.Errorf("git workspace store path %s is not a directory", cur)
			}
			continue
		} else if !IsNotFound(err) {
			return err
		}
		if err := c.MkdirCtx(ctx, cur, 0o755); err != nil {
			// Tolerate a lost create race.
			if st, serr := c.StatCtx(ctx, cur); serr == nil && st != nil && st.IsDir {
				return nil
			}
			return fmt.Errorf("mkdir %s: %w", cur, err)
		}
	}
	return nil
}

// writeGitWorkspaceFile writes one whole file into the store (parents
// created as needed, unconditional revision).
func (c *Client) writeGitWorkspaceFile(ctx context.Context, p string, data []byte, mode uint32) error {
	if err := c.ensureGitWorkspaceDir(ctx, path.Dir(p)); err != nil {
		return err
	}
	if _, err := c.WriteCtxConditionalWithRevision(ctx, p, data, -1); err != nil {
		return fmt.Errorf("write %s: %w", p, err)
	}
	if mode != 0 && mode != 0o644 {
		if err := c.ChmodCtx(ctx, p, mode&0o777); err != nil {
			return fmt.Errorf("chmod %s: %w", p, err)
		}
	}
	return nil
}

// deleteGitWorkspaceTree removes a store subtree best-effort (post-order
// walk; every step tolerates absence).
func (c *Client) deleteGitWorkspaceTree(ctx context.Context, dir string) {
	entries, err := c.ListCtx(ctx, dir)
	if err != nil {
		return
	}
	base := strings.TrimSuffix(dir, "/")
	for _, e := range entries {
		child := base + "/" + e.Name
		if e.IsDir {
			c.deleteGitWorkspaceTree(ctx, child)
			continue
		}
		_ = c.DeleteFileCtx(ctx, child)
	}
	_ = c.DeleteDirCtx(ctx, base)
}

func manifestToWorkspace(id string, m *gitws.Manifest, state string) *GitWorkspace {
	kind := strings.TrimSpace(m.WorkspaceKind)
	if kind == "" {
		kind = "main"
	}
	ws := &GitWorkspace{
		WorkspaceID:       strings.TrimSpace(m.WorkspaceID),
		RootPath:          m.RootPath,
		RepoURL:           m.RepoURL,
		RemoteName:        m.RemoteName,
		BranchName:        m.BranchName,
		BaseCommit:        m.BaseCommit,
		HeadCommit:        m.HeadCommit,
		Mode:              m.Mode,
		WorkspaceKind:     kind,
		CommonWorkspaceID: m.CommonWorkspaceID,
		WorktreeName:      m.WorktreeName,
		GitDirRel:         m.GitDirRel,
		Status:            state,
	}
	if ws.WorkspaceID == "" {
		ws.WorkspaceID = id
	}
	if ws.RemoteName == "" {
		ws.RemoteName = "origin"
	}
	for _, stamp := range []struct {
		src string
		dst *time.Time
	}{
		{m.CreatedAt, &ws.CreatedAt},
		{m.UpdatedAt, &ws.UpdatedAt},
	} {
		if stamp.src != "" {
			if t, err := time.Parse(time.RFC3339, stamp.src); err == nil {
				*stamp.dst = t
			}
		}
	}
	return ws
}

// GitWorkspaceFSGet loads one workspace record (manifest + state).
func (c *Client) GitWorkspaceFSGet(ctx context.Context, id string) (*GitWorkspace, error) {
	if err := gitws.ValidateID(id); err != nil {
		return nil, err
	}
	data, err := c.readGitWorkspaceDoc(ctx, gitws.ManifestPath(id))
	if IsNotFound(err) {
		return nil, &GitWorkspaceFSNotFound{ID: id}
	}
	if err != nil {
		return nil, err
	}
	m, err := gitws.ParseManifest(data)
	if err != nil {
		return nil, err
	}
	state := gitws.StateDormant
	if body, err := c.readGitWorkspaceDoc(ctx, gitws.StatePath(id)); err == nil {
		state = gitws.ParseState(body)
	} else if !IsNotFound(err) {
		return nil, err
	}
	if state == gitws.StateDeleted {
		return nil, &GitWorkspaceFSNotFound{ID: id}
	}
	return manifestToWorkspace(id, m, state), nil
}

// GitWorkspaceFSList enumerates the store through the registry index;
// entries without a manifest (legacy or swept) are skipped.
func (c *Client) GitWorkspaceFSList(ctx context.Context) ([]GitWorkspace, error) {
	idx, _, err := c.ReadGitWorkspaceIndex(ctx)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}
	out := make([]GitWorkspace, 0, len(idx.Workspaces))
	for _, e := range idx.Workspaces {
		if strings.TrimSpace(e.WorkspaceID) == "" {
			continue
		}
		ws, err := c.GitWorkspaceFSGet(ctx, e.WorkspaceID)
		if err != nil {
			if IsGitWorkspaceFSNotFound(err) || IsNotFound(err) {
				continue
			}
			return nil, err
		}
		out = append(out, *ws)
	}
	return out, nil
}

// GitWorkspaceFSGetByRoot finds the workspace registered at a root path.
func (c *Client) GitWorkspaceFSGetByRoot(ctx context.Context, rootPath string) (*GitWorkspace, error) {
	rootPath = strings.TrimSuffix(strings.TrimSpace(rootPath), "/")
	if rootPath == "" {
		rootPath = "/"
	}
	list, err := c.GitWorkspaceFSList(ctx)
	if err != nil {
		return nil, err
	}
	for i := range list {
		if strings.TrimSuffix(list[i].RootPath, "/") == rootPath {
			return &list[i], nil
		}
	}
	return nil, &GitWorkspaceFSNotFound{ID: rootPath}
}

// GitWorkspaceFSUpsert registers (or refreshes) a workspace record: a
// client-generated id when absent, the manifest, an initial dormant
// state file, and the registry index entry. An existing manifest's
// creation stamp and unknown keys survive the rewrite.
func (c *Client) GitWorkspaceFSUpsert(ctx context.Context, req GitWorkspaceRequest) (*GitWorkspace, error) {
	id := strings.TrimSpace(req.WorkspaceID)
	if id == "" {
		var err error
		if id, err = newGitWorkspaceID(); err != nil {
			return nil, err
		}
	} else if err := gitws.ValidateID(id); err != nil {
		return nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	m := &gitws.Manifest{
		WorkspaceID:       id,
		RootPath:          strings.TrimSpace(req.RootPath),
		WorkspaceKind:     strings.TrimSpace(req.WorkspaceKind),
		RepoURL:           req.RepoURL,
		RemoteName:        req.RemoteName,
		BranchName:        req.BranchName,
		BaseCommit:        req.BaseCommit,
		HeadCommit:        req.HeadCommit,
		Mode:              req.Mode,
		CommonWorkspaceID: req.CommonWorkspaceID,
		WorktreeName:      req.WorktreeName,
		GitDirRel:         req.GitDirRel,
		UpdatedAt:         now,
	}
	if existing, err := c.readGitWorkspaceDoc(ctx, gitws.ManifestPath(id)); err == nil {
		prev, perr := gitws.ParseManifest(existing)
		if perr != nil {
			return nil, perr
		}
		m.CreatedAt = prev.CreatedAt
		m.PreserveUnknownFrom(prev)
	} else if !IsNotFound(err) {
		return nil, err
	} else {
		m.CreatedAt = now
	}
	if m.WorkspaceKind == "" {
		m.WorkspaceKind = "main"
	}
	if m.RemoteName == "" {
		m.RemoteName = "origin"
	}
	body, err := gitws.EncodeManifest(m)
	if err != nil {
		return nil, err
	}
	if err := c.writeGitWorkspaceFile(ctx, gitws.ManifestPath(id), body, 0o644); err != nil {
		return nil, err
	}
	if _, err := c.StatCtx(ctx, gitws.StatePath(id)); IsNotFound(err) {
		if err := c.writeGitWorkspaceFile(ctx, gitws.StatePath(id), []byte(gitws.StateDormant+"\n"), 0o644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	ws := manifestToWorkspace(id, m, gitws.StateDormant)
	kind := ws.WorkspaceKind
	if err := c.UpsertGitWorkspaceIndexEntry(ctx, GitWorkspaceIndexEntry{
		WorkspaceID:   id,
		RootPath:      ws.RootPath,
		WorkspaceKind: kind,
	}); err != nil {
		return nil, fmt.Errorf("publish git workspace index entry: %w", err)
	}
	return ws, nil
}

// GitWorkspaceFSPutTreeIndex records the tracked-path oracle for a commit.
func (c *Client) GitWorkspaceFSPutTreeIndex(ctx context.Context, id, commit string, nodes []GitTreeNode) error {
	if err := gitws.ValidateID(id); err != nil {
		return err
	}
	entries := make([]gitws.TreeEntry, 0, len(nodes))
	for _, n := range nodes {
		rel, err := gitws.CleanRel(n.Path)
		if err != nil {
			return err
		}
		entries = append(entries, gitws.TreeEntry{
			Path:      rel,
			Kind:      n.Kind,
			Mode:      n.Mode,
			ObjectSHA: n.ObjectSHA,
			Size:      n.SizeBytes,
		})
	}
	body, err := gitws.EncodeTreeIndex(&gitws.TreeIndex{Commit: commit, Entries: entries})
	if err != nil {
		return err
	}
	return c.writeGitWorkspaceFile(ctx, gitws.TreeIndexPath(id), body, 0o644)
}

// GitWorkspaceFSGetTreeIndex loads the tracked-path oracle (hydrate input).
func (c *Client) GitWorkspaceFSGetTreeIndex(ctx context.Context, id string) ([]GitTreeNode, error) {
	if err := gitws.ValidateID(id); err != nil {
		return nil, err
	}
	data, err := c.readGitWorkspaceDoc(ctx, gitws.TreeIndexPath(id))
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	idx, err := gitws.ParseTreeIndex(data)
	if err != nil {
		return nil, err
	}
	nodes := make([]GitTreeNode, 0, len(idx.Entries))
	for _, e := range idx.Entries {
		rel := e.Path
		name := path.Base(rel)
		parent := ""
		if r := path.Dir(rel); r != "." {
			parent = r
		}
		nodes = append(nodes, GitTreeNode{
			WorkspaceID: id,
			CommitSHA:   idx.Commit,
			Path:        rel,
			ParentPath:  parent,
			Name:        name,
			Kind:        e.Kind,
			Mode:        e.Mode,
			ObjectSHA:   e.ObjectSHA,
			SizeBytes:   e.Size,
		})
	}
	return nodes, nil
}

// GitWorkspaceFSPutGitState stores the .git state archive and its
// sidecar metadata (checksum + size + checkpoint commit).
func (c *Client) GitWorkspaceFSPutGitState(ctx context.Context, id, checkpointCommit string, content []byte) error {
	if err := gitws.ValidateID(id); err != nil {
		return err
	}
	sum := sha256.Sum256(content)
	meta, err := json.Marshal(gitws.GitStateMeta{
		CheckpointCommit: checkpointCommit,
		StorageType:      gitws.GitStateStorageTarGzNoObjects,
		ChecksumSHA256:   hex.EncodeToString(sum[:]),
		SizeBytes:        int64(len(content)),
		CreatedAt:        time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	if err := c.writeGitWorkspaceFile(ctx, gitws.GitStatePath(id), content, 0o644); err != nil {
		return err
	}
	return c.writeGitWorkspaceFile(ctx, gitws.GitStateMetaPath(id), meta, 0o644)
}

// GitWorkspaceFSGetGitState loads the .git state archive. Missing state
// returns (nil, nil); a checksum mismatch is an error.
func (c *Client) GitWorkspaceFSGetGitState(ctx context.Context, id string) (*GitState, error) {
	if err := gitws.ValidateID(id); err != nil {
		return nil, err
	}
	metaBody, err := c.readGitWorkspaceDoc(ctx, gitws.GitStateMetaPath(id))
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta gitws.GitStateMeta
	if err := json.Unmarshal(metaBody, &meta); err != nil {
		return nil, fmt.Errorf("parse git state meta: %w", err)
	}
	rc, err := c.ReadStream(ctx, gitws.GitStatePath(id))
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read git state archive: %w", err)
	}
	if meta.SizeBytes != 0 && meta.SizeBytes != int64(len(content)) {
		return nil, fmt.Errorf("git state archive size mismatch: meta %d, got %d", meta.SizeBytes, len(content))
	}
	if meta.ChecksumSHA256 != "" {
		sum := sha256.Sum256(content)
		if !strings.EqualFold(meta.ChecksumSHA256, hex.EncodeToString(sum[:])) {
			return nil, fmt.Errorf("git state archive checksum mismatch")
		}
	}
	return &GitState{
		WorkspaceID:      id,
		CheckpointCommit: meta.CheckpointCommit,
		StorageType:      meta.StorageType,
		StorageRef:       gitws.GitStatePath(id),
		ChecksumSHA256:   meta.ChecksumSHA256,
		SizeBytes:        int64(len(content)),
		Content:          content,
		UpdatedAt:        meta.CreatedAt,
	}, nil
}

// GitWorkspaceFSPackID derives the content-addressed pack id.
func GitWorkspaceFSPackID(content []byte) string {
	return gitws.PackIDFor(content)
}

// GitWorkspaceFSPutObjectPack stores one git object pack (content
// addressed, so retried checkpoints dedupe).
func (c *Client) GitWorkspaceFSPutObjectPack(ctx context.Context, id string, content []byte) (string, error) {
	if err := gitws.ValidateID(id); err != nil {
		return "", err
	}
	packID := GitWorkspaceFSPackID(content)
	meta, err := json.Marshal(gitws.PackMeta{
		ChecksumSHA256: gitws.ChecksumHex(content),
		SizeBytes:      int64(len(content)),
		CreatedAt:      gitws.NowUTC(),
	})
	if err != nil {
		return "", err
	}
	if err := c.writeGitWorkspaceFile(ctx, gitws.PackPath(id, packID), content, 0o644); err != nil {
		return "", err
	}
	if err := c.writeGitWorkspaceFile(ctx, gitws.PackMetaPath(id, packID), meta, 0o644); err != nil {
		return "", err
	}
	return packID, nil
}

// GitWorkspaceFSListObjectPacks enumerates the stored packs (metadata
// only). A missing packs directory is an empty list.
func (c *Client) GitWorkspaceFSListObjectPacks(ctx context.Context, id string) ([]GitObjectPack, error) {
	if err := gitws.ValidateID(id); err != nil {
		return nil, err
	}
	entries, err := c.ListCtx(ctx, gitws.PacksDir(id))
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var packs []GitObjectPack
	for _, e := range entries {
		if e.IsDir || !strings.HasSuffix(e.Name, ".json") {
			continue
		}
		packID := strings.TrimSuffix(e.Name, ".json")
		metaBody, err := c.readGitWorkspaceDoc(ctx, gitws.PackMetaPath(id, packID))
		if IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var meta gitws.PackMeta
		if err := json.Unmarshal(metaBody, &meta); err != nil {
			return nil, fmt.Errorf("parse pack meta %s: %w", packID, err)
		}
		packs = append(packs, GitObjectPack{
			WorkspaceID:    id,
			PackID:         packID,
			ChecksumSHA256: meta.ChecksumSHA256,
			SizeBytes:      meta.SizeBytes,
			CreatedAt:      meta.CreatedAt,
		})
	}
	return packs, nil
}

// GitWorkspaceFSGetObjectPack loads one stored pack (checksum verified).
func (c *Client) GitWorkspaceFSGetObjectPack(ctx context.Context, id, packID string) (*GitObjectPack, error) {
	if err := gitws.ValidateID(id); err != nil {
		return nil, err
	}
	packID = strings.TrimSpace(packID)
	if packID == "" || strings.ContainsAny(packID, "/\\") {
		return nil, fmt.Errorf("invalid git object pack id %q", packID)
	}
	metaBody, err := c.readGitWorkspaceDoc(ctx, gitws.PackMetaPath(id, packID))
	if IsNotFound(err) {
		return nil, &GitWorkspaceFSNotFound{ID: packID}
	}
	if err != nil {
		return nil, err
	}
	var meta gitws.PackMeta
	if err := json.Unmarshal(metaBody, &meta); err != nil {
		return nil, fmt.Errorf("parse pack meta %s: %w", packID, err)
	}
	rc, err := c.ReadStream(ctx, gitws.PackPath(id, packID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read git object pack %s: %w", packID, err)
	}
	if meta.SizeBytes != 0 && meta.SizeBytes != int64(len(content)) {
		return nil, fmt.Errorf("git object pack %s size mismatch", packID)
	}
	if meta.ChecksumSHA256 != "" && !strings.EqualFold(meta.ChecksumSHA256, gitws.ChecksumHex(content)) {
		return nil, fmt.Errorf("git object pack %s checksum mismatch", packID)
	}
	return &GitObjectPack{
		WorkspaceID:    id,
		PackID:         packID,
		ChecksumSHA256: meta.ChecksumSHA256,
		SizeBytes:      int64(len(content)),
		Content:        content,
		CreatedAt:      meta.CreatedAt,
	}, nil
}

// GitWorkspaceFSDelete deregisters a workspace: the index entry first
// (the visibility kill-switch), then the store subtree best-effort.
func (c *Client) GitWorkspaceFSDelete(ctx context.Context, id string) error {
	if err := gitws.ValidateID(id); err != nil {
		return err
	}
	if err := c.RemoveGitWorkspaceIndexEntry(ctx, id); err != nil {
		return fmt.Errorf("remove git workspace index entry: %w", err)
	}
	c.deleteGitWorkspaceTree(ctx, gitws.WorkspaceDir(id))
	return nil
}

// GitWorkspaceFSPutTreeFile streams one clean-tree content file into the
// store (parents ensured; used by the hydrate upload).
func (c *Client) GitWorkspaceFSPutTreeFile(ctx context.Context, id, commit, rel string, r io.Reader, size int64) error {
	if err := gitws.ValidateID(id); err != nil {
		return err
	}
	rel, err := gitws.CleanRel(rel)
	if err != nil || rel == "." {
		return fmt.Errorf("invalid git workspace tree path %q", rel)
	}
	p := gitws.TreePath(id, commit, rel)
	if err := c.ensureGitWorkspaceDir(ctx, path.Dir(p)); err != nil {
		return err
	}
	if err := c.WriteStream(ctx, p, r, size, nil); err != nil {
		return fmt.Errorf("write %s: %w", p, err)
	}
	return nil
}

// GitWorkspaceFSPutTreeSymlink re-creates one clean-tree symlink in the
// store (idempotent).
func (c *Client) GitWorkspaceFSPutTreeSymlink(ctx context.Context, id, commit, rel, target string) error {
	if err := gitws.ValidateID(id); err != nil {
		return err
	}
	rel, err := gitws.CleanRel(rel)
	if err != nil || rel == "." {
		return fmt.Errorf("invalid git workspace tree path %q", rel)
	}
	p := gitws.TreePath(id, commit, rel)
	if _, serr := c.StatCtx(ctx, p); serr == nil {
		return nil
	}
	if err := c.ensureGitWorkspaceDir(ctx, path.Dir(p)); err != nil {
		return err
	}
	if err := c.SymlinkCtx(ctx, target, p); err != nil && !errors.Is(err, ErrConflict) {
		return fmt.Errorf("symlink %s: %w", p, err)
	}
	return nil
}
