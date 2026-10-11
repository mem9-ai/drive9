// The workspace record (manifest.json), the tracked-path oracle
// (tree-index.json), the lifecycle state file, and the binary sidecar
// metadata documents (packs, git-state).
package gitws

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Manifest is the workspace record stored at <id>/manifest.json.
type Manifest struct {
	WorkspaceID       string
	RootPath          string
	WorkspaceKind     string // "main" | "linked" ("" = main)
	RepoURL           string
	RemoteName        string
	BranchName        string
	BaseCommit        string
	HeadCommit        string
	Mode              string // "fast" | "fast-blobless"
	CommonWorkspaceID string
	WorktreeName      string
	GitDirRel         string
	CreatedAt         string // RFC3339
	UpdatedAt         string // RFC3339

	extra map[string]json.RawMessage
}

var manifestKeys = []string{
	"workspace_id", "root_path", "workspace_kind", "repo_url", "remote_name",
	"branch_name", "base_commit", "head_commit", "mode", "common_workspace_id",
	"worktree_name", "gitdir_rel", "created_at", "updated_at",
}

// ParseManifest decodes a manifest document.
func ParseManifest(data []byte) (*Manifest, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("gitws: manifest: %w", err)
	}
	m := &Manifest{extra: extraKeys(raw, manifestKeys...)}
	var err error
	if m.WorkspaceID, err = decodeString(raw, "workspace_id"); err != nil {
		return nil, err
	}
	if m.RootPath, err = decodeString(raw, "root_path"); err != nil {
		return nil, err
	}
	if m.WorkspaceKind, err = decodeString(raw, "workspace_kind"); err != nil {
		return nil, err
	}
	if m.RepoURL, err = decodeString(raw, "repo_url"); err != nil {
		return nil, err
	}
	if m.RemoteName, err = decodeString(raw, "remote_name"); err != nil {
		return nil, err
	}
	if m.BranchName, err = decodeString(raw, "branch_name"); err != nil {
		return nil, err
	}
	if m.BaseCommit, err = decodeString(raw, "base_commit"); err != nil {
		return nil, err
	}
	if m.HeadCommit, err = decodeString(raw, "head_commit"); err != nil {
		return nil, err
	}
	if m.Mode, err = decodeString(raw, "mode"); err != nil {
		return nil, err
	}
	if m.CommonWorkspaceID, err = decodeString(raw, "common_workspace_id"); err != nil {
		return nil, err
	}
	if m.WorktreeName, err = decodeString(raw, "worktree_name"); err != nil {
		return nil, err
	}
	if m.GitDirRel, err = decodeString(raw, "gitdir_rel"); err != nil {
		return nil, err
	}
	if m.CreatedAt, err = decodeString(raw, "created_at"); err != nil {
		return nil, err
	}
	if m.UpdatedAt, err = decodeString(raw, "updated_at"); err != nil {
		return nil, err
	}
	return m, nil
}

// PreserveUnknownFrom carries another manifest's unknown keys into m, so
// a read-modify-write by an older writer cannot strip newer fields.
func (m *Manifest) PreserveUnknownFrom(prev *Manifest) {
	if prev != nil && prev.extra != nil {
		m.extra = prev.extra
	}
}

// EncodeManifest serializes the manifest, preserving unknown keys and the
// fields it owns.
func EncodeManifest(m *Manifest) ([]byte, error) {
	fields := map[string]any{
		"workspace_id":        strings.TrimSpace(m.WorkspaceID),
		"root_path":           strings.TrimSpace(m.RootPath),
		"workspace_kind":      strings.TrimSpace(m.WorkspaceKind),
		"repo_url":            m.RepoURL,
		"remote_name":         m.RemoteName,
		"branch_name":         m.BranchName,
		"base_commit":         m.BaseCommit,
		"head_commit":         m.HeadCommit,
		"mode":                m.Mode,
		"common_workspace_id": m.CommonWorkspaceID,
		"worktree_name":       m.WorktreeName,
		"gitdir_rel":          m.GitDirRel,
		"created_at":          m.CreatedAt,
		"updated_at":          m.UpdatedAt,
	}
	return mergeExtra(fields, m.extra)
}

// TreeEntry is one tracked path of the clean tree. Paths are relative to
// the workspace root (no leading slash). Mode is the git octal string
// form ("100644"); ObjectSHA backs blob hydration by the git tooling.
type TreeEntry struct {
	Path      string
	Kind      string
	Mode      string
	ObjectSHA string
	Size      int64

	extra map[string]json.RawMessage
}

// TreeIndex is the write-side oracle at <id>/tree-index.json: which paths
// belong to the recorded clean tree (first modification of such a path
// enters the dirty mirror) plus their attributes for the view.
type TreeIndex struct {
	Version int
	Commit  string
	Entries []TreeEntry

	extra map[string]json.RawMessage
}

var treeIndexKeys = []string{"version", "commit", "entries"}

// ParseTreeIndex decodes a tree index document.
func ParseTreeIndex(data []byte) (*TreeIndex, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("gitws: tree index: %w", err)
	}
	idx := &TreeIndex{extra: extraKeys(raw, treeIndexKeys...)}
	if v, ok := raw["version"]; ok {
		if err := json.Unmarshal(v, &idx.Version); err != nil {
			return nil, fmt.Errorf("gitws: tree index version: %w", err)
		}
	}
	if v, ok := raw["commit"]; ok {
		if err := json.Unmarshal(v, &idx.Commit); err != nil {
			return nil, fmt.Errorf("gitws: tree index commit: %w", err)
		}
	}
	if v, ok := raw["entries"]; ok {
		if err := json.Unmarshal(v, &idx.Entries); err != nil {
			return nil, fmt.Errorf("gitws: tree index entries: %w", err)
		}
	}
	if idx.Version == 0 {
		idx.Version = 1
	}
	return idx, nil
}

// UnmarshalJSON decodes one tree entry, preserving unknown keys.
func (e *TreeEntry) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("gitws: tree entry: %w", err)
	}
	e.extra = extraKeys(raw, "path", "kind", "mode", "object_sha", "size")
	var err error
	if e.Path, err = decodeString(raw, "path"); err != nil {
		return err
	}
	if e.Kind, err = decodeString(raw, "kind"); err != nil {
		return err
	}
	if e.Mode, err = decodeString(raw, "mode"); err != nil {
		return err
	}
	if e.ObjectSHA, err = decodeString(raw, "object_sha"); err != nil {
		return err
	}
	if v, ok := raw["size"]; ok {
		if err := json.Unmarshal(v, &e.Size); err != nil {
			return fmt.Errorf("gitws: tree entry size: %w", err)
		}
	}
	return nil
}

// MarshalJSON encodes one tree entry, preserving unknown keys.
func (e TreeEntry) MarshalJSON() ([]byte, error) {
	fields := map[string]any{
		"path":       e.Path,
		"kind":       e.Kind,
		"mode":       e.Mode,
		"object_sha": e.ObjectSHA,
	}
	if e.Size != 0 {
		fields["size"] = e.Size
	}
	return mergeExtra(fields, e.extra)
}

// EncodeTreeIndex serializes the tree index, preserving unknown keys.
func EncodeTreeIndex(idx *TreeIndex) ([]byte, error) {
	version := idx.Version
	if version == 0 {
		version = 1
	}
	fields := map[string]any{
		"version": version,
		"commit":  idx.Commit,
	}
	if idx.Entries == nil {
		fields["entries"] = []TreeEntry{}
	} else {
		fields["entries"] = idx.Entries
	}
	return mergeExtra(fields, idx.extra)
}

// ParseState decodes the <id>/state file body (default dormant).
func ParseState(data []byte) string {
	s := strings.TrimSpace(string(data))
	switch s {
	case StateDormant, StateArmed, StateDeleted:
		return s
	default:
		return StateDormant
	}
}

// GitStateStorageTarGzNoObjects is the .git state archive storage type:
// a tar.gz of the .git tree with every objects/ component excluded (the
// objects ride the content-addressed packs instead).
const GitStateStorageTarGzNoObjects = "tar.gz-no-objects"

// PackMeta is the sidecar document of one stored object pack.
type PackMeta struct {
	ChecksumSHA256 string    `json:"checksum_sha256"`
	SizeBytes      int64     `json:"size_bytes"`
	CreatedAt      time.Time `json:"created_at"`
}

// GitStateMeta is the sidecar document of the .git state archive.
type GitStateMeta struct {
	CheckpointCommit string    `json:"checkpoint_commit"`
	StorageType      string    `json:"storage_type"`
	ChecksumSHA256   string    `json:"checksum_sha256"`
	SizeBytes        int64     `json:"size_bytes"`
	CreatedAt        time.Time `json:"created_at"`
}
