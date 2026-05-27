package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

const (
	maxGitWorkspaceBodyBytes = 4 << 20
	maxGitTreeBodyBytes      = 128 << 20
	maxGitBlobBodyBytes      = 512 << 20
)

type gitWorkspaceRequest struct {
	RootPath   string `json:"root_path"`
	RepoURL    string `json:"repo_url"`
	RemoteName string `json:"remote_name,omitempty"`
	BranchName string `json:"branch_name,omitempty"`
	BaseCommit string `json:"base_commit,omitempty"`
	HeadCommit string `json:"head_commit,omitempty"`
	Mode       string `json:"mode,omitempty"`
}

type gitWorkspaceResponse struct {
	WorkspaceID string    `json:"workspace_id"`
	RootPath    string    `json:"root_path"`
	RepoURL     string    `json:"repo_url"`
	RemoteName  string    `json:"remote_name"`
	BranchName  string    `json:"branch_name"`
	BaseCommit  string    `json:"base_commit"`
	HeadCommit  string    `json:"head_commit"`
	Mode        string    `json:"mode"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type gitTreeReplaceRequest struct {
	CommitSHA string                `json:"commit_sha"`
	Nodes     []gitTreeNodeResponse `json:"nodes"`
}

type gitTreeNodeResponse struct {
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

type gitStateRequest struct {
	CheckpointCommit string `json:"checkpoint_commit,omitempty"`
	StorageType      string `json:"storage_type,omitempty"`
	StorageRef       string `json:"storage_ref,omitempty"`
	StorageRefHash   string `json:"storage_ref_hash,omitempty"`
	ChecksumSHA256   string `json:"checksum_sha256,omitempty"`
	SizeBytes        int64  `json:"size_bytes,omitempty"`
	Content          []byte `json:"content,omitempty"`
}

type gitStateResponse struct {
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

type gitOverlayEntryRequest struct {
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

type gitOverlayEntryResponse struct {
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

func (s *Server) handleGitWorkspaces(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	store := b.Store()
	switch {
	case r.URL.Path == "/v1/git-workspaces":
		switch r.Method {
		case http.MethodPost:
			s.handleGitWorkspaceUpsert(w, r, store)
		case http.MethodGet:
			if r.URL.Query().Get("root_path") == "" {
				s.handleGitWorkspaceList(w, r, store)
			} else {
				s.handleGitWorkspaceGetByRoot(w, r, store)
			}
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/"):
		s.handleGitWorkspaceObject(w, r, store)
	default:
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleGitWorkspaceUpsert(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	defer func() { _ = r.Body.Close() }()
	var req gitWorkspaceRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGitWorkspaceBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	root, err := pathutil.CanonicalizeDir(req.RootPath)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid root_path: "+err.Error())
		return
	}
	if root == "/" {
		errJSON(w, http.StatusBadRequest, "root_path cannot be / for a git workspace")
		return
	}
	repoURL := strings.TrimSpace(req.RepoURL)
	if repoURL == "" {
		errJSON(w, http.StatusBadRequest, "repo_url is required")
		return
	}
	if err := validateOptionalGitObjectID("base_commit", req.BaseCommit); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateOptionalGitObjectID("head_commit", req.HeadCommit); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.BaseCommit == "" {
		req.BaseCommit = req.HeadCommit
	}

	workspaceID := token.NewID()
	existing, err := store.GetGitWorkspaceByRoot(r.Context(), root)
	if err == nil {
		workspaceID = existing.WorkspaceID
	} else if !errors.Is(err, datastore.ErrNotFound) {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	ws := datastore.GitWorkspace{
		WorkspaceID: workspaceID,
		RootPath:    root,
		RepoURL:     repoURL,
		RemoteName:  strings.TrimSpace(req.RemoteName),
		BranchName:  strings.TrimSpace(req.BranchName),
		BaseCommit:  strings.TrimSpace(req.BaseCommit),
		HeadCommit:  strings.TrimSpace(req.HeadCommit),
		Mode:        strings.TrimSpace(req.Mode),
		Status:      datastore.GitWorkspaceStatusLive,
	}
	if ws.RemoteName == "" {
		ws.RemoteName = "origin"
	}
	if ws.Mode == "" {
		ws.Mode = datastore.GitWorkspaceModeFast
	}
	if err := store.UpsertGitWorkspace(r.Context(), ws); err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	out, err := store.GetGitWorkspaceByRoot(r.Context(), root)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toGitWorkspaceResponse(out))
}

func (s *Server) handleGitWorkspaceGetByRoot(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	rootRaw := r.URL.Query().Get("root_path")
	if rootRaw == "" {
		errJSON(w, http.StatusBadRequest, "root_path is required")
		return
	}
	root, err := pathutil.CanonicalizeDir(rootRaw)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid root_path: "+err.Error())
		return
	}
	ws, err := store.GetGitWorkspaceByRoot(r.Context(), root)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toGitWorkspaceResponse(ws))
}

func (s *Server) handleGitWorkspaceList(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	workspaces, err := store.ListGitWorkspaces(r.Context())
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	out := make([]gitWorkspaceResponse, 0, len(workspaces))
	for i := range workspaces {
		out = append(out, toGitWorkspaceResponse(&workspaces[i]))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": out})
}

func (s *Server) handleGitWorkspaceObject(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/git-workspaces/")
	rawID, sub, hasSub := strings.Cut(rest, "/")
	if rawID == "" {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	workspaceID, err := url.PathUnescape(rawID)
	if err != nil || workspaceID == "" {
		errJSON(w, http.StatusBadRequest, "invalid workspace id")
		return
	}
	switch {
	case !hasSub:
		if r.Method != http.MethodGet {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		ws, err := store.GetGitWorkspace(r.Context(), workspaceID)
		if err != nil {
			writeGitWorkspaceStoreError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(toGitWorkspaceResponse(ws))
	case sub == "tree":
		switch r.Method {
		case http.MethodPost:
			s.handleGitTreeReplace(w, r, store, workspaceID)
		case http.MethodGet:
			s.handleGitTreeList(w, r, store, workspaceID)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case sub == "git-state":
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			s.handleGitStateUpsert(w, r, store, workspaceID)
		case http.MethodGet:
			s.handleGitStateGet(w, r, store, workspaceID)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case sub == "overlay":
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("path") != "" {
				s.handleGitOverlayGet(w, r, store, workspaceID)
				return
			}
			entries, err := store.ListGitOverlayEntries(r.Context(), workspaceID)
			if err != nil {
				writeGitWorkspaceStoreError(w, err)
				return
			}
			out := make([]gitOverlayEntryResponse, 0, len(entries))
			for i := range entries {
				out = append(out, toGitOverlayEntryResponse(&entries[i]))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
		case http.MethodPost, http.MethodPut:
			s.handleGitOverlayUpsert(w, r, store, workspaceID)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	default:
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleGitTreeReplace(w http.ResponseWriter, r *http.Request, store *datastore.Store, workspaceID string) {
	defer func() { _ = r.Body.Close() }()
	var req gitTreeReplaceRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGitTreeBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	if err := validateGitObjectID("commit_sha", req.CommitSHA); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	nodes, err := normalizeGitTreeNodes(workspaceID, req.CommitSHA, req.Nodes)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := store.ReplaceGitTreeNodes(r.Context(), workspaceID, req.CommitSHA, nodes); err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "count": len(nodes)})
}

func (s *Server) handleGitTreeList(w http.ResponseWriter, r *http.Request, store *datastore.Store, workspaceID string) {
	commitSHA := strings.TrimSpace(r.URL.Query().Get("commit_sha"))
	if err := validateGitObjectID("commit_sha", commitSHA); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	nodes, err := store.ListGitTreeNodes(r.Context(), workspaceID, commitSHA)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	out := make([]gitTreeNodeResponse, 0, len(nodes))
	for i := range nodes {
		out = append(out, toGitTreeNodeResponse(&nodes[i]))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"nodes": out})
}

func (s *Server) handleGitStateUpsert(w http.ResponseWriter, r *http.Request, store *datastore.Store, workspaceID string) {
	defer func() { _ = r.Body.Close() }()
	var req gitStateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGitBlobBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	if err := validateOptionalGitObjectID("checkpoint_commit", req.CheckpointCommit); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := store.UpsertGitState(r.Context(), datastore.GitState{
		WorkspaceID:      workspaceID,
		CheckpointCommit: strings.TrimSpace(req.CheckpointCommit),
		StorageType:      strings.TrimSpace(req.StorageType),
		StorageRef:       strings.TrimSpace(req.StorageRef),
		StorageRefHash:   strings.TrimSpace(req.StorageRefHash),
		ChecksumSHA256:   strings.TrimSpace(req.ChecksumSHA256),
		SizeBytes:        req.SizeBytes,
		ContentBlob:      req.Content,
	}); err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	state, err := store.GetGitState(r.Context(), workspaceID)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toGitStateResponse(state))
}

func (s *Server) handleGitOverlayUpsert(w http.ResponseWriter, r *http.Request, store *datastore.Store, workspaceID string) {
	defer func() { _ = r.Body.Close() }()
	var req gitOverlayEntryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGitBlobBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	relPath, _, _, err := cleanGitRelativePath(req.Path)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid overlay path: "+err.Error())
		return
	}
	op := strings.TrimSpace(req.Op)
	if op == "" {
		op = datastore.GitOverlayOpUpsert
	}
	switch op {
	case datastore.GitOverlayOpUpsert, datastore.GitOverlayOpWhiteout, datastore.GitOverlayOpChmod, datastore.GitOverlayOpSymlink:
	default:
		errJSON(w, http.StatusBadRequest, "invalid overlay op")
		return
	}
	kind := strings.TrimSpace(req.Kind)
	if kind == "" {
		kind = datastore.GitTreeNodeKindFile
	}
	switch kind {
	case datastore.GitTreeNodeKindFile, datastore.GitTreeNodeKindDirectory, datastore.GitTreeNodeKindSymlink, datastore.GitTreeNodeKindSubmodule:
	default:
		errJSON(w, http.StatusBadRequest, "invalid overlay kind")
		return
	}
	entry := datastore.GitOverlayEntry{
		WorkspaceID:    workspaceID,
		Path:           relPath,
		Op:             op,
		Kind:           kind,
		Mode:           strings.TrimSpace(req.Mode),
		StorageType:    strings.TrimSpace(req.StorageType),
		StorageRef:     strings.TrimSpace(req.StorageRef),
		StorageRefHash: strings.TrimSpace(req.StorageRefHash),
		ChecksumSHA256: strings.TrimSpace(req.ChecksumSHA256),
		SizeBytes:      req.SizeBytes,
		BaseObjectSHA:  strings.TrimSpace(req.BaseObjectSHA),
		ContentBlob:    req.Content,
	}
	if entry.SizeBytes == 0 && len(entry.ContentBlob) > 0 {
		entry.SizeBytes = int64(len(entry.ContentBlob))
	}
	if err := store.UpsertGitOverlayEntry(r.Context(), entry); err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	stored, err := store.GetGitOverlayEntry(r.Context(), workspaceID, relPath)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toGitOverlayEntryResponse(stored))
}

func (s *Server) handleGitOverlayGet(w http.ResponseWriter, r *http.Request, store *datastore.Store, workspaceID string) {
	relPath, _, _, err := cleanGitRelativePath(r.URL.Query().Get("path"))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid overlay path: "+err.Error())
		return
	}
	entry, err := store.GetGitOverlayEntry(r.Context(), workspaceID, relPath)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toGitOverlayEntryResponse(entry))
}

func (s *Server) handleGitStateGet(w http.ResponseWriter, r *http.Request, store *datastore.Store, workspaceID string) {
	state, err := store.GetGitState(r.Context(), workspaceID)
	if err != nil {
		writeGitWorkspaceStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toGitStateResponse(state))
}

func normalizeGitTreeNodes(workspaceID, commitSHA string, in []gitTreeNodeResponse) ([]datastore.GitTreeNode, error) {
	out := make([]datastore.GitTreeNode, 0, len(in))
	for i := range in {
		n := in[i]
		path, parent, name, err := cleanGitRelativePath(n.Path)
		if err != nil {
			return nil, fmt.Errorf("invalid node path %q: %w", n.Path, err)
		}
		kind := strings.TrimSpace(n.Kind)
		switch kind {
		case datastore.GitTreeNodeKindFile, datastore.GitTreeNodeKindDirectory, datastore.GitTreeNodeKindSymlink, datastore.GitTreeNodeKindSubmodule:
		default:
			return nil, fmt.Errorf("invalid node kind %q", n.Kind)
		}
		if strings.TrimSpace(n.Mode) == "" {
			return nil, fmt.Errorf("node mode is required for %q", path)
		}
		if err := validateGitObjectID("object_sha", n.ObjectSHA); err != nil {
			return nil, fmt.Errorf("%s for %q", err.Error(), path)
		}
		out = append(out, datastore.GitTreeNode{
			WorkspaceID: workspaceID,
			CommitSHA:   commitSHA,
			Path:        path,
			ParentPath:  parent,
			Name:        name,
			Kind:        kind,
			Mode:        strings.TrimSpace(n.Mode),
			ObjectSHA:   strings.TrimSpace(n.ObjectSHA),
			SizeBytes:   n.SizeBytes,
		})
	}
	return out, nil
}

func cleanGitRelativePath(raw string) (path string, parent string, name string, err error) {
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

func validateOptionalGitObjectID(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return validateGitObjectID(field, value)
}

func validateGitObjectID(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) != 40 && len(value) != 64 {
		return fmt.Errorf("%s must be a 40 or 64 character git object id", field)
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return fmt.Errorf("%s must be hexadecimal", field)
	}
	return nil
}

func writeGitWorkspaceStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, datastore.ErrNotFound) {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	errJSON(w, http.StatusInternalServerError, err.Error())
}

func toGitWorkspaceResponse(ws *datastore.GitWorkspace) gitWorkspaceResponse {
	return gitWorkspaceResponse{
		WorkspaceID: ws.WorkspaceID,
		RootPath:    ws.RootPath,
		RepoURL:     ws.RepoURL,
		RemoteName:  ws.RemoteName,
		BranchName:  ws.BranchName,
		BaseCommit:  ws.BaseCommit,
		HeadCommit:  ws.HeadCommit,
		Mode:        ws.Mode,
		Status:      ws.Status,
		CreatedAt:   ws.CreatedAt,
		UpdatedAt:   ws.UpdatedAt,
	}
}

func toGitTreeNodeResponse(n *datastore.GitTreeNode) gitTreeNodeResponse {
	return gitTreeNodeResponse{
		WorkspaceID: n.WorkspaceID,
		CommitSHA:   n.CommitSHA,
		Path:        n.Path,
		ParentPath:  n.ParentPath,
		Name:        n.Name,
		Kind:        n.Kind,
		Mode:        n.Mode,
		ObjectSHA:   n.ObjectSHA,
		SizeBytes:   n.SizeBytes,
		CreatedAt:   n.CreatedAt,
	}
}

func toGitStateResponse(state *datastore.GitState) gitStateResponse {
	return gitStateResponse{
		WorkspaceID:      state.WorkspaceID,
		CheckpointCommit: state.CheckpointCommit,
		StorageType:      state.StorageType,
		StorageRef:       state.StorageRef,
		StorageRefHash:   state.StorageRefHash,
		ChecksumSHA256:   state.ChecksumSHA256,
		SizeBytes:        state.SizeBytes,
		Content:          state.ContentBlob,
		CreatedAt:        state.CreatedAt,
		UpdatedAt:        state.UpdatedAt,
	}
}

func toGitOverlayEntryResponse(entry *datastore.GitOverlayEntry) gitOverlayEntryResponse {
	return gitOverlayEntryResponse{
		WorkspaceID:    entry.WorkspaceID,
		Path:           entry.Path,
		Op:             entry.Op,
		Kind:           entry.Kind,
		Mode:           entry.Mode,
		StorageType:    entry.StorageType,
		StorageRef:     entry.StorageRef,
		StorageRefHash: entry.StorageRefHash,
		ChecksumSHA256: entry.ChecksumSHA256,
		SizeBytes:      entry.SizeBytes,
		BaseObjectSHA:  entry.BaseObjectSHA,
		Content:        entry.ContentBlob,
		CreatedAt:      entry.CreatedAt,
		UpdatedAt:      entry.UpdatedAt,
	}
}
