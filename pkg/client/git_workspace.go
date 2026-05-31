package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type GitWorkspaceRequest struct {
	RootPath          string `json:"root_path"`
	RepoURL           string `json:"repo_url"`
	RemoteName        string `json:"remote_name,omitempty"`
	BranchName        string `json:"branch_name,omitempty"`
	BaseCommit        string `json:"base_commit,omitempty"`
	HeadCommit        string `json:"head_commit,omitempty"`
	Mode              string `json:"mode,omitempty"`
	WorkspaceKind     string `json:"workspace_kind,omitempty"`
	CommonWorkspaceID string `json:"common_workspace_id,omitempty"`
	WorktreeName      string `json:"worktree_name,omitempty"`
	GitDirRel         string `json:"gitdir_rel,omitempty"`
}

type GitWorkspace struct {
	WorkspaceID       string    `json:"workspace_id"`
	RootPath          string    `json:"root_path"`
	RepoURL           string    `json:"repo_url"`
	RemoteName        string    `json:"remote_name"`
	BranchName        string    `json:"branch_name"`
	BaseCommit        string    `json:"base_commit"`
	HeadCommit        string    `json:"head_commit"`
	Mode              string    `json:"mode"`
	WorkspaceKind     string    `json:"workspace_kind"`
	CommonWorkspaceID string    `json:"common_workspace_id"`
	WorktreeName      string    `json:"worktree_name"`
	GitDirRel         string    `json:"gitdir_rel"`
	Status            string    `json:"status"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type GitTreeReplaceRequest struct {
	CommitSHA string        `json:"commit_sha"`
	Nodes     []GitTreeNode `json:"nodes"`
}

type GitTreeNode struct {
	WorkspaceID string    `json:"workspace_id,omitempty"`
	CommitSHA   string    `json:"commit_sha,omitempty"`
	Path        string    `json:"path"`
	ParentPath  string    `json:"parent_path"`
	Name        string    `json:"name"`
	Kind        string    `json:"kind"`
	Mode        string    `json:"mode"`
	ObjectSHA   string    `json:"object_sha"`
	SizeBytes   int64     `json:"size_bytes"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
}

type GitStateRequest struct {
	CheckpointCommit string `json:"checkpoint_commit,omitempty"`
	StorageType      string `json:"storage_type,omitempty"`
	StorageRef       string `json:"storage_ref,omitempty"`
	StorageRefHash   string `json:"storage_ref_hash,omitempty"`
	ChecksumSHA256   string `json:"checksum_sha256,omitempty"`
	SizeBytes        int64  `json:"size_bytes,omitempty"`
	Content          []byte `json:"content,omitempty"`
}

type GitState struct {
	WorkspaceID      string    `json:"workspace_id"`
	CheckpointCommit string    `json:"checkpoint_commit"`
	StorageType      string    `json:"storage_type"`
	StorageRef       string    `json:"storage_ref"`
	StorageRefHash   string    `json:"storage_ref_hash"`
	ChecksumSHA256   string    `json:"checksum_sha256"`
	SizeBytes        int64     `json:"size_bytes"`
	Content          []byte    `json:"content,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type GitObjectPackRequest struct {
	Content []byte `json:"content"`
}

type GitObjectPack struct {
	WorkspaceID    string    `json:"workspace_id"`
	PackID         string    `json:"pack_id"`
	ChecksumSHA256 string    `json:"checksum_sha256"`
	SizeBytes      int64     `json:"size_bytes"`
	Content        []byte    `json:"content,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type GitOverlayEntryRequest struct {
	Path           string `json:"path"`
	Op             string `json:"op,omitempty"`
	Kind           string `json:"kind,omitempty"`
	Mode           string `json:"mode,omitempty"`
	StorageType    string `json:"storage_type,omitempty"`
	StorageRef     string `json:"storage_ref,omitempty"`
	StorageRefHash string `json:"storage_ref_hash,omitempty"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	BaseObjectSHA  string `json:"base_object_sha,omitempty"`
	Content        []byte `json:"content,omitempty"`
}

type GitOverlayEntry struct {
	WorkspaceID    string    `json:"workspace_id"`
	Path           string    `json:"path"`
	Op             string    `json:"op"`
	Kind           string    `json:"kind"`
	Mode           string    `json:"mode"`
	StorageType    string    `json:"storage_type"`
	StorageRef     string    `json:"storage_ref"`
	StorageRefHash string    `json:"storage_ref_hash"`
	ChecksumSHA256 string    `json:"checksum_sha256"`
	SizeBytes      int64     `json:"size_bytes"`
	BaseObjectSHA  string    `json:"base_object_sha"`
	Content        []byte    `json:"content,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (c *Client) UpsertGitWorkspace(ctx context.Context, req GitWorkspaceRequest) (*GitWorkspace, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/git-workspaces", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitWorkspace
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git workspace: %w", err)
	}
	return &out, nil
}

func (c *Client) GetGitWorkspaceByRoot(ctx context.Context, rootPath string) (*GitWorkspace, error) {
	if strings.TrimSpace(rootPath) == "" {
		return nil, fmt.Errorf("rootPath must not be empty")
	}
	u := c.baseURL + "/v1/git-workspaces?root_path=" + url.QueryEscape(rootPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitWorkspace
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git workspace: %w", err)
	}
	return &out, nil
}

func (c *Client) GetGitWorkspace(ctx context.Context, workspaceID string) (*GitWorkspace, error) {
	if strings.TrimSpace(workspaceID) == "" {
		return nil, fmt.Errorf("workspaceID must not be empty")
	}
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitWorkspace
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git workspace: %w", err)
	}
	return &out, nil
}

func (c *Client) DeleteGitWorkspace(ctx context.Context, workspaceID string) error {
	if strings.TrimSpace(workspaceID) == "" {
		return fmt.Errorf("workspaceID must not be empty")
	}
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

func (c *Client) ListGitWorkspaces(ctx context.Context) ([]GitWorkspace, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/git-workspaces", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Workspaces []GitWorkspace `json:"workspaces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git workspaces: %w", err)
	}
	return out.Workspaces, nil
}

func (c *Client) ReplaceGitTree(ctx context.Context, workspaceID string, req GitTreeReplaceRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/tree"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

func (c *Client) ListGitTree(ctx context.Context, workspaceID, commitSHA string) ([]GitTreeNode, error) {
	values := url.Values{}
	values.Set("commit_sha", commitSHA)
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/tree?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Nodes []GitTreeNode `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git tree: %w", err)
	}
	return out.Nodes, nil
}

func (c *Client) UpsertGitState(ctx context.Context, workspaceID string, req GitStateRequest) (*GitState, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/git-state"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitState
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git state: %w", err)
	}
	return &out, nil
}

func (c *Client) GetGitState(ctx context.Context, workspaceID string) (*GitState, error) {
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/git-state"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitState
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git state: %w", err)
	}
	return &out, nil
}

func (c *Client) PutGitObjectPack(ctx context.Context, workspaceID string, req GitObjectPackRequest) (*GitObjectPack, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/object-packs"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitObjectPack
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git object pack: %w", err)
	}
	return &out, nil
}

func (c *Client) ListGitObjectPacks(ctx context.Context, workspaceID string) ([]GitObjectPack, error) {
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/object-packs"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Packs []GitObjectPack `json:"packs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git object packs: %w", err)
	}
	return out.Packs, nil
}

func (c *Client) GetGitObjectPack(ctx context.Context, workspaceID, packID string) (*GitObjectPack, error) {
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/object-packs/" + url.PathEscape(packID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitObjectPack
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git object pack: %w", err)
	}
	return &out, nil
}

func (c *Client) PutGitOverlayEntry(ctx context.Context, workspaceID string, req GitOverlayEntryRequest) (*GitOverlayEntry, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/overlay"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitOverlayEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git overlay entry: %w", err)
	}
	return &out, nil
}

func (c *Client) GetGitOverlayEntry(ctx context.Context, workspaceID, relPath string) (*GitOverlayEntry, error) {
	values := url.Values{}
	values.Set("path", relPath)
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/overlay?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out GitOverlayEntry
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git overlay entry: %w", err)
	}
	return &out, nil
}

func (c *Client) ListGitOverlayEntries(ctx context.Context, workspaceID string) ([]GitOverlayEntry, error) {
	u := c.baseURL + "/v1/git-workspaces/" + url.PathEscape(workspaceID) + "/overlay"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Entries []GitOverlayEntry `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode git overlay entries: %w", err)
	}
	return out.Entries, nil
}
