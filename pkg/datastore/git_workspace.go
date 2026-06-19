package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type GitWorkspaceMode string
type GitWorkspaceKind string
type GitWorkspaceStatus string
type GitTreeNodeKind string
type GitOverlayOp string
type GitOverlayKind string

const (
	GitWorkspaceModeFast         GitWorkspaceMode   = "fast"
	GitWorkspaceModeFastBlobless GitWorkspaceMode   = "fast-blobless"
	GitWorkspaceKindMain         GitWorkspaceKind   = "main"
	GitWorkspaceKindLinked       GitWorkspaceKind   = "linked"
	GitWorkspaceStatusLive       GitWorkspaceStatus = "active"
	GitWorkspaceStatusDeleted    GitWorkspaceStatus = "deleted"

	GitTreeNodeKindFile      GitTreeNodeKind = "file"
	GitTreeNodeKindDirectory GitTreeNodeKind = "dir"
	GitTreeNodeKindSymlink   GitTreeNodeKind = "symlink"
	GitTreeNodeKindSubmodule GitTreeNodeKind = "submodule"

	GitOverlayKindFile      GitOverlayKind = "file"
	GitOverlayKindDirectory GitOverlayKind = "dir"
	GitOverlayKindSymlink   GitOverlayKind = "symlink"
	GitOverlayKindSubmodule GitOverlayKind = "submodule"

	GitOverlayOpUpsert   GitOverlayOp = "upsert"
	GitOverlayOpWhiteout GitOverlayOp = "whiteout"
	GitOverlayOpChmod    GitOverlayOp = "chmod"
	GitOverlayOpSymlink  GitOverlayOp = "symlink"
)

// GitWorkspace is the authoritative drive9 record for a git-backed worktree.
// Clean files are represented by git object metadata rather than file_nodes.
type GitWorkspace struct {
	WorkspaceID  string
	RootPath     string
	RepoURL      string
	RemoteName   string
	BranchName   string
	BaseCommit   string
	HeadCommit   string
	Mode         GitWorkspaceMode
	Kind         GitWorkspaceKind
	CommonID     string
	WorktreeName string
	GitDirRel    string
	Status       GitWorkspaceStatus
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// GitTreeNode describes one entry from a commit tree relative to a workspace
// root. Path and ParentPath never start with a slash.
type GitTreeNode struct {
	WorkspaceID string
	CommitSHA   string
	Path        string
	ParentPath  string
	Name        string
	Kind        GitTreeNodeKind
	Mode        string
	ObjectSHA   string
	SizeBytes   int64
	CreatedAt   time.Time
}

// GitState points at a durable checkpoint of the local .git directory.
type GitState struct {
	WorkspaceID      string
	CheckpointCommit string
	StorageType      string
	StorageRef       string
	StorageRefHash   string
	ChecksumSHA256   string
	SizeBytes        int64
	ContentBlob      []byte
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// GitObjectPack stores a local-only git pack needed to restore staged/local
// objects that are not available from the remote promisor.
type GitObjectPack struct {
	WorkspaceID    string
	PackID         string
	ChecksumSHA256 string
	SizeBytes      int64
	ContentBlob    []byte
	CreatedAt      time.Time
}

// GitOverlayEntry records a durable dirty/new/delete overlay entry on top of a
// clean git tree. Payload bytes are stored through the ordinary content plane
// and referenced here.
type GitOverlayEntry struct {
	WorkspaceID    string
	Path           string
	Op             GitOverlayOp
	Kind           GitOverlayKind
	Mode           string
	StorageType    string
	StorageRef     string
	StorageRefHash string
	ChecksumSHA256 string
	SizeBytes      int64
	BaseObjectSHA  string
	ContentBlob    []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (s *Store) UpsertGitWorkspace(ctx context.Context, ws GitWorkspace) error {
	if strings.TrimSpace(ws.WorkspaceID) == "" {
		return fmt.Errorf("git workspace id is required")
	}
	if strings.TrimSpace(ws.RootPath) == "" {
		return fmt.Errorf("git workspace root path is required")
	}
	root, err := pathutil.CanonicalizeDir(ws.RootPath)
	if err != nil {
		return fmt.Errorf("invalid git workspace root path: %w", err)
	}
	ws.RootPath = root
	if strings.TrimSpace(ws.RepoURL) == "" {
		return fmt.Errorf("git workspace repo url is required")
	}
	if ws.RemoteName == "" {
		ws.RemoteName = "origin"
	}
	if ws.Mode == "" {
		ws.Mode = GitWorkspaceModeFast
	}
	if ws.Kind == "" {
		ws.Kind = GitWorkspaceKindMain
	}
	if ws.Status == "" {
		ws.Status = GitWorkspaceStatusLive
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO git_workspaces (
	workspace_id, root_path, repo_url, remote_name, branch_name,
	base_commit, head_commit, mode, workspace_kind, common_workspace_id,
	worktree_name, gitdir_rel, status, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))
ON DUPLICATE KEY UPDATE
	repo_url = VALUES(repo_url),
	remote_name = VALUES(remote_name),
	branch_name = VALUES(branch_name),
	base_commit = VALUES(base_commit),
	head_commit = VALUES(head_commit),
	mode = VALUES(mode),
	workspace_kind = VALUES(workspace_kind),
	common_workspace_id = VALUES(common_workspace_id),
	worktree_name = VALUES(worktree_name),
	gitdir_rel = VALUES(gitdir_rel),
	status = VALUES(status),
	updated_at = UTC_TIMESTAMP(3)`,
		ws.WorkspaceID, ws.RootPath, ws.RepoURL, ws.RemoteName, ws.BranchName,
		ws.BaseCommit, ws.HeadCommit, string(ws.Mode), string(ws.Kind), ws.CommonID,
		ws.WorktreeName, ws.GitDirRel, string(ws.Status))
	if err != nil {
		return fmt.Errorf("upsert git workspace %s: %w", ws.WorkspaceID, err)
	}
	return nil
}

func (s *Store) GetGitWorkspace(ctx context.Context, workspaceID string) (*GitWorkspace, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, root_path, repo_url, remote_name, branch_name,
	base_commit, head_commit, mode, workspace_kind, common_workspace_id,
	worktree_name, gitdir_rel, status, created_at, updated_at
FROM git_workspaces
WHERE workspace_id = ?`, workspaceID)
	return scanGitWorkspace(row)
}

func (s *Store) GetGitWorkspaceByRoot(ctx context.Context, rootPath string) (*GitWorkspace, error) {
	root, err := pathutil.CanonicalizeDir(rootPath)
	if err != nil {
		return nil, fmt.Errorf("invalid git workspace root path: %w", err)
	}
	row := s.db.QueryRowContext(ctx, `
	SELECT workspace_id, root_path, repo_url, remote_name, branch_name,
		base_commit, head_commit, mode, workspace_kind, common_workspace_id,
		worktree_name, gitdir_rel, status, created_at, updated_at
	FROM git_workspaces
	WHERE root_path = ?`, root)
	return scanGitWorkspace(row)
}

func (s *Store) ListGitWorkspaces(ctx context.Context) ([]GitWorkspace, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, root_path, repo_url, remote_name, branch_name,
	base_commit, head_commit, mode, workspace_kind, common_workspace_id,
	worktree_name, gitdir_rel, status, created_at, updated_at
FROM git_workspaces
WHERE status = ?
	ORDER BY root_path`, string(GitWorkspaceStatusLive))
	if err != nil {
		return nil, fmt.Errorf("list git workspaces: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GitWorkspace
	for rows.Next() {
		ws, err := scanGitWorkspace(rows)
		if err != nil {
			return nil, fmt.Errorf("scan git workspace: %w", err)
		}
		out = append(out, *ws)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list git workspaces: %w", err)
	}
	return out, nil
}

func (s *Store) DeleteGitWorkspace(ctx context.Context, workspaceID string) error {
	if strings.TrimSpace(workspaceID) == "" {
		return fmt.Errorf("git workspace id is required")
	}
	res, err := s.db.ExecContext(ctx, `
	UPDATE git_workspaces
	SET status = ?, updated_at = UTC_TIMESTAMP(3)
	WHERE workspace_id = ?`, string(GitWorkspaceStatusDeleted), workspaceID)
	if err != nil {
		return fmt.Errorf("delete git workspace %s: %w", workspaceID, err)
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return ErrNotFound
	}
	return nil
}

func cleanGitRelativePathForStore(raw string) (path string, parent string, name string, err error) {
	if raw == "" {
		return "", "", "", fmt.Errorf("path is required")
	}
	if len(raw) > 1024 {
		return "", "", "", fmt.Errorf("path exceeds 1024 bytes")
	}
	if strings.HasPrefix(raw, "/") {
		return "", "", "", fmt.Errorf("path must be relative")
	}
	if strings.ContainsRune(raw, '\x00') {
		return "", "", "", fmt.Errorf("path contains NUL")
	}
	if strings.ContainsRune(raw, '\\') {
		return "", "", "", fmt.Errorf("path contains backslash")
	}
	if strings.HasSuffix(raw, "/") {
		return "", "", "", fmt.Errorf("path must not end with slash")
	}
	parts := strings.Split(raw, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", "", "", fmt.Errorf("path contains invalid segment %q", part)
		}
		if len(part) > 255 {
			return "", "", "", fmt.Errorf("path segment exceeds 255 bytes")
		}
	}
	name = parts[len(parts)-1]
	if len(parts) > 1 {
		parent = strings.Join(parts[:len(parts)-1], "/")
	}
	return raw, parent, name, nil
}

type gitWorkspaceScanner interface {
	Scan(dest ...any) error
}

func scanGitWorkspace(row gitWorkspaceScanner) (*GitWorkspace, error) {
	var ws GitWorkspace
	var mode, kind, status string
	if err := row.Scan(
		&ws.WorkspaceID, &ws.RootPath, &ws.RepoURL, &ws.RemoteName, &ws.BranchName,
		&ws.BaseCommit, &ws.HeadCommit, &mode, &kind, &ws.CommonID,
		&ws.WorktreeName, &ws.GitDirRel, &status, &ws.CreatedAt, &ws.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	ws.Mode = GitWorkspaceMode(mode)
	ws.Kind = GitWorkspaceKind(kind)
	ws.Status = GitWorkspaceStatus(status)
	return &ws, nil
}

func (s *Store) ReplaceGitTreeNodes(ctx context.Context, workspaceID, commitSHA string, nodes []GitTreeNode) error {
	if strings.TrimSpace(workspaceID) == "" {
		return fmt.Errorf("git workspace id is required")
	}
	if strings.TrimSpace(commitSHA) == "" {
		return fmt.Errorf("git commit sha is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace git tree: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `
DELETE FROM git_workspace_tree_nodes
WHERE workspace_id = ? AND commit_sha = ?`, workspaceID, commitSHA); err != nil {
		return fmt.Errorf("delete git tree nodes: %w", err)
	}
	if len(nodes) > 0 {
		stmt, prepErr := tx.PrepareContext(ctx, `
INSERT INTO git_workspace_tree_nodes (
	workspace_id, commit_sha, path, path_hash, parent_path, parent_path_hash,
	name, kind, mode, object_sha, size_bytes, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3))`)
		if prepErr != nil {
			return fmt.Errorf("prepare insert git tree node: %w", prepErr)
		}
		defer func() { _ = stmt.Close() }()
		for i := range nodes {
			n := nodes[i]
			if n.WorkspaceID == "" {
				n.WorkspaceID = workspaceID
			} else if n.WorkspaceID != workspaceID {
				return fmt.Errorf("insert git tree node %s: workspace_id mismatch", n.Path)
			}
			if n.CommitSHA == "" {
				n.CommitSHA = commitSHA
			} else if n.CommitSHA != commitSHA {
				return fmt.Errorf("insert git tree node %s: commit_sha mismatch", n.Path)
			}
			relPath, parent, name, err := cleanGitRelativePathForStore(n.Path)
			if err != nil {
				return fmt.Errorf("insert git tree node %s: %w", n.Path, err)
			}
			n.Path = relPath
			if n.ParentPath == "" {
				n.ParentPath = parent
			} else if n.ParentPath != parent {
				return fmt.Errorf("insert git tree node %s: parent_path mismatch", n.Path)
			}
			if n.Name == "" {
				n.Name = name
			} else if n.Name != name {
				return fmt.Errorf("insert git tree node %s: name mismatch", n.Path)
			}
			if _, err = stmt.ExecContext(ctx,
				n.WorkspaceID, n.CommitSHA, n.Path, gitPathHash(n.Path), n.ParentPath, gitPathHash(n.ParentPath),
				n.Name, string(n.Kind), n.Mode, n.ObjectSHA, n.SizeBytes,
			); err != nil {
				return fmt.Errorf("insert git tree node %s: %w", n.Path, err)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit replace git tree: %w", err)
	}
	committed = true
	return nil
}

func (s *Store) ListGitTreeNodes(ctx context.Context, workspaceID, commitSHA string) ([]GitTreeNode, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, commit_sha, path, parent_path, name, kind, mode,
	object_sha, size_bytes, created_at
FROM git_workspace_tree_nodes
WHERE workspace_id = ? AND commit_sha = ?
ORDER BY path`, workspaceID, commitSHA)
	if err != nil {
		return nil, fmt.Errorf("list git tree nodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GitTreeNode
	for rows.Next() {
		var n GitTreeNode
		var kind string
		if err := rows.Scan(
			&n.WorkspaceID, &n.CommitSHA, &n.Path, &n.ParentPath, &n.Name, &kind, &n.Mode,
			&n.ObjectSHA, &n.SizeBytes, &n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan git tree node: %w", err)
		}
		n.Kind = GitTreeNodeKind(kind)
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list git tree nodes: %w", err)
	}
	return out, nil
}

func (s *Store) UpsertGitState(ctx context.Context, state GitState) error {
	if strings.TrimSpace(state.WorkspaceID) == "" {
		return fmt.Errorf("git workspace id is required")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO git_workspace_git_state (
	workspace_id, checkpoint_commit, storage_type, storage_ref, storage_ref_hash,
	checksum_sha256, size_bytes, content_blob, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))
ON DUPLICATE KEY UPDATE
	checkpoint_commit = VALUES(checkpoint_commit),
	storage_type = VALUES(storage_type),
	storage_ref = VALUES(storage_ref),
	storage_ref_hash = VALUES(storage_ref_hash),
	checksum_sha256 = VALUES(checksum_sha256),
	size_bytes = VALUES(size_bytes),
	content_blob = VALUES(content_blob),
	updated_at = UTC_TIMESTAMP(3)`,
		state.WorkspaceID, state.CheckpointCommit, state.StorageType, state.StorageRef,
		state.StorageRefHash, state.ChecksumSHA256, state.SizeBytes, state.ContentBlob)
	if err != nil {
		return fmt.Errorf("upsert git state %s: %w", state.WorkspaceID, err)
	}
	return nil
}

func (s *Store) GetGitState(ctx context.Context, workspaceID string) (*GitState, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, checkpoint_commit, storage_type, storage_ref, storage_ref_hash,
	checksum_sha256, size_bytes, content_blob, created_at, updated_at
FROM git_workspace_git_state
WHERE workspace_id = ?`, workspaceID)
	var state GitState
	if err := row.Scan(
		&state.WorkspaceID, &state.CheckpointCommit, &state.StorageType, &state.StorageRef,
		&state.StorageRefHash, &state.ChecksumSHA256, &state.SizeBytes, &state.ContentBlob, &state.CreatedAt, &state.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &state, nil
}

func (s *Store) UpsertGitObjectPack(ctx context.Context, pack GitObjectPack) error {
	if strings.TrimSpace(pack.WorkspaceID) == "" {
		return fmt.Errorf("git workspace id is required")
	}
	if strings.TrimSpace(pack.PackID) == "" {
		return fmt.Errorf("git object pack id is required")
	}
	if pack.SizeBytes == 0 && len(pack.ContentBlob) > 0 {
		pack.SizeBytes = int64(len(pack.ContentBlob))
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO git_workspace_object_packs (
	workspace_id, pack_id, checksum_sha256, size_bytes, content_blob, created_at
) VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(3))
ON DUPLICATE KEY UPDATE
	checksum_sha256 = VALUES(checksum_sha256),
	size_bytes = VALUES(size_bytes),
	content_blob = VALUES(content_blob)`,
		pack.WorkspaceID, pack.PackID, pack.ChecksumSHA256, pack.SizeBytes, pack.ContentBlob)
	if err != nil {
		return fmt.Errorf("upsert git object pack %s: %w", pack.PackID, err)
	}
	return nil
}

func (s *Store) ListGitObjectPacks(ctx context.Context, workspaceID string) ([]GitObjectPack, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, pack_id, checksum_sha256, size_bytes, created_at
FROM git_workspace_object_packs
WHERE workspace_id = ?
ORDER BY created_at, pack_id`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list git object packs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GitObjectPack
	for rows.Next() {
		var pack GitObjectPack
		if err := rows.Scan(
			&pack.WorkspaceID, &pack.PackID, &pack.ChecksumSHA256, &pack.SizeBytes, &pack.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan git object pack: %w", err)
		}
		out = append(out, pack)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list git object packs: %w", err)
	}
	return out, nil
}

func (s *Store) GetGitObjectPack(ctx context.Context, workspaceID, packID string) (*GitObjectPack, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, pack_id, checksum_sha256, size_bytes, content_blob, created_at
FROM git_workspace_object_packs
WHERE workspace_id = ? AND pack_id = ?`, workspaceID, packID)
	var pack GitObjectPack
	if err := row.Scan(
		&pack.WorkspaceID, &pack.PackID, &pack.ChecksumSHA256, &pack.SizeBytes, &pack.ContentBlob, &pack.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &pack, nil
}

func (s *Store) UpsertGitOverlayEntry(ctx context.Context, entry GitOverlayEntry) error {
	if strings.TrimSpace(entry.WorkspaceID) == "" {
		return fmt.Errorf("git workspace id is required")
	}
	if strings.TrimSpace(entry.Path) == "" {
		return fmt.Errorf("git overlay path is required")
	}
	relPath, _, _, err := cleanGitRelativePathForStore(entry.Path)
	if err != nil {
		return fmt.Errorf("invalid git overlay path: %w", err)
	}
	entry.Path = relPath
	if entry.Op == "" {
		entry.Op = GitOverlayOpUpsert
	}
	if entry.Kind == "" {
		entry.Kind = GitOverlayKindFile
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO git_workspace_overlay (
	workspace_id, path, path_hash, op, kind, mode, storage_type, storage_ref, storage_ref_hash,
	checksum_sha256, size_bytes, base_object_sha, content_blob, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))
ON DUPLICATE KEY UPDATE
	path = VALUES(path),
	op = VALUES(op),
	kind = VALUES(kind),
	mode = VALUES(mode),
	storage_type = VALUES(storage_type),
	storage_ref = VALUES(storage_ref),
	storage_ref_hash = VALUES(storage_ref_hash),
	checksum_sha256 = VALUES(checksum_sha256),
	size_bytes = VALUES(size_bytes),
	base_object_sha = VALUES(base_object_sha),
	content_blob = VALUES(content_blob),
	updated_at = UTC_TIMESTAMP(3)`,
		entry.WorkspaceID, entry.Path, gitPathHash(entry.Path), string(entry.Op), string(entry.Kind), entry.Mode, entry.StorageType, entry.StorageRef,
		entry.StorageRefHash, entry.ChecksumSHA256, entry.SizeBytes, entry.BaseObjectSHA, entry.ContentBlob)
	if err != nil {
		return fmt.Errorf("upsert git overlay entry %s: %w", entry.Path, err)
	}
	return nil
}

func (s *Store) ListGitOverlayEntries(ctx context.Context, workspaceID string) ([]GitOverlayEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT workspace_id, path, op, kind, mode, storage_type, storage_ref, storage_ref_hash,
	checksum_sha256, size_bytes, base_object_sha, content_blob, created_at, updated_at
FROM git_workspace_overlay
WHERE workspace_id = ?
ORDER BY path`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list git overlay entries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []GitOverlayEntry
	for rows.Next() {
		var e GitOverlayEntry
		var op, kind string
		if err := rows.Scan(
			&e.WorkspaceID, &e.Path, &op, &kind, &e.Mode, &e.StorageType, &e.StorageRef,
			&e.StorageRefHash, &e.ChecksumSHA256, &e.SizeBytes, &e.BaseObjectSHA, &e.ContentBlob, &e.CreatedAt, &e.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan git overlay entry: %w", err)
		}
		e.Op = GitOverlayOp(op)
		e.Kind = GitOverlayKind(kind)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list git overlay entries: %w", err)
	}
	return out, nil
}

func (s *Store) GetGitOverlayEntry(ctx context.Context, workspaceID, relPath string) (*GitOverlayEntry, error) {
	relPath, _, _, err := cleanGitRelativePathForStore(relPath)
	if err != nil {
		return nil, fmt.Errorf("invalid git overlay path: %w", err)
	}
	row := s.db.QueryRowContext(ctx, `
SELECT workspace_id, path, op, kind, mode, storage_type, storage_ref, storage_ref_hash,
	checksum_sha256, size_bytes, base_object_sha, content_blob, created_at, updated_at
FROM git_workspace_overlay
	WHERE workspace_id = ? AND path_hash = ? AND path = ?`, workspaceID, gitPathHash(relPath), relPath)
	var e GitOverlayEntry
	var op, kind string
	if err := row.Scan(
		&e.WorkspaceID, &e.Path, &op, &kind, &e.Mode, &e.StorageType, &e.StorageRef,
		&e.StorageRefHash, &e.ChecksumSHA256, &e.SizeBytes, &e.BaseObjectSHA, &e.ContentBlob, &e.CreatedAt, &e.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	e.Op = GitOverlayOp(op)
	e.Kind = GitOverlayKind(kind)
	return &e, nil
}

func gitPathHash(path string) string {
	return StorageRefHash(path)
}
