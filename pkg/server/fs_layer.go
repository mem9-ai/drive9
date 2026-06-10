package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	backendpkg "github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/journal"
	"github.com/mem9-ai/dat9/pkg/pathutil"
)

const maxFSLayerBodyBytes = 1 << 20
const maxFSLayerEntryBodyBytes = 128 << 20
const fsLayerRollbackTimeout = 60 * time.Second

type fsLayerCreateRequest struct {
	LayerID        string            `json:"layer_id,omitempty"`
	BaseRootPath   string            `json:"base_root_path"`
	Name           string            `json:"name,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	DurabilityMode string            `json:"durability_mode,omitempty"`
	ActorID        string            `json:"actor_id,omitempty"`
}

type fsLayerResponse struct {
	LayerID        string            `json:"layer_id"`
	BaseRootPath   string            `json:"base_root_path"`
	Name           string            `json:"name"`
	Tags           map[string]string `json:"tags,omitempty"`
	State          string            `json:"state"`
	DurabilityMode string            `json:"durability_mode"`
	ActorID        string            `json:"actor_id"`
	DurableSeq     int64             `json:"durable_seq"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	SealedAt       *time.Time        `json:"sealed_at,omitempty"`
}

type fsLayerEntryResponse struct {
	LayerID        string    `json:"layer_id"`
	Path           string    `json:"path"`
	ParentPath     string    `json:"parent_path"`
	Name           string    `json:"name"`
	Op             string    `json:"op"`
	Kind           string    `json:"kind"`
	BaseInodeID    string    `json:"base_inode_id"`
	BaseRevision   int64     `json:"base_revision"`
	StorageType    string    `json:"storage_type"`
	StorageRef     string    `json:"storage_ref"`
	StorageRefHash string    `json:"storage_ref_hash"`
	ChecksumSHA256 string    `json:"checksum_sha256"`
	SizeBytes      int64     `json:"size_bytes"`
	Mode           uint32    `json:"mode"`
	Content        []byte    `json:"content,omitempty"`
	ContentText    string    `json:"content_text,omitempty"`
	EntrySeq       int64     `json:"entry_seq"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type fsLayerEntryRequest struct {
	Path           string `json:"path"`
	Op             string `json:"op,omitempty"`
	Kind           string `json:"kind,omitempty"`
	BaseInodeID    string `json:"base_inode_id,omitempty"`
	BaseRevision   int64  `json:"base_revision,omitempty"`
	StorageType    string `json:"storage_type,omitempty"`
	StorageRef     string `json:"storage_ref,omitempty"`
	StorageRefHash string `json:"storage_ref_hash,omitempty"`
	Content        []byte `json:"content,omitempty"`
	ContentType    string `json:"content_type,omitempty"`
	ContentText    string `json:"content_text,omitempty"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	Mode           uint32 `json:"mode,omitempty"`
}

type fsLayerCommitResponse struct {
	Status    string                  `json:"status"`
	LayerID   string                  `json:"layer_id"`
	Applied   int                     `json:"applied,omitempty"`
	Conflicts []fsLayerCommitConflict `json:"conflicts,omitempty"`
}

type fsLayerCommitConflict struct {
	Path         string `json:"path"`
	Reason       string `json:"reason"`
	BaseRevision int64  `json:"base_revision,omitempty"`
	WantRevision int64  `json:"want_revision,omitempty"`
}

type fsLayerCheckpointRequest struct {
	CheckpointID string `json:"checkpoint_id,omitempty"`
	Label        string `json:"label,omitempty"`
}

type fsLayerCheckpointResponse struct {
	CheckpointID string    `json:"checkpoint_id"`
	LayerID      string    `json:"layer_id"`
	DurableSeq   int64     `json:"durable_seq"`
	Label        string    `json:"label"`
	CreatedAt    time.Time `json:"created_at"`
}

func (s *Server) handleFSLayers(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	store := b.Store()
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/fs-layer-checkpoints/"):
		s.handleFSLayerCheckpointObject(w, r, store)
	case r.URL.Path == "/v1/fs-layers":
		switch r.Method {
		case http.MethodPost:
			s.handleFSLayerCreate(w, r, store)
		case http.MethodGet:
			s.handleFSLayerList(w, r, store)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case strings.HasPrefix(r.URL.Path, "/v1/fs-layers/"):
		s.handleFSLayerObject(w, r, b, store)
	default:
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleFSLayerCheckpointObject(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rawID := strings.TrimPrefix(r.URL.Path, "/v1/fs-layer-checkpoints/")
	if rawID == "" {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	checkpointID, err := url.PathUnescape(rawID)
	if err != nil || strings.TrimSpace(checkpointID) == "" {
		errJSON(w, http.StatusBadRequest, "invalid checkpoint id")
		return
	}
	checkpoint, err := store.GetFSLayerCheckpoint(r.Context(), checkpointID)
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerCheckpointResponse(checkpoint))
}

func (s *Server) handleFSLayerCreate(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	defer func() { _ = r.Body.Close() }()
	var req fsLayerCreateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFSLayerBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	layerID := strings.TrimSpace(req.LayerID)
	if layerID == "" {
		layerID = journal.NewID("layer")
	}
	actorID := strings.TrimSpace(req.ActorID)
	if actorID == "" {
		actorID = strings.TrimSpace(r.Header.Get("X-Dat9-Actor"))
	}
	layer := datastore.FSLayer{
		LayerID:        layerID,
		BaseRootPath:   req.BaseRootPath,
		Name:           req.Name,
		Tags:           req.Tags,
		DurabilityMode: datastore.FSLayerDurabilityMode(strings.TrimSpace(req.DurabilityMode)),
		ActorID:        actorID,
	}
	if err := store.CreateFSLayer(r.Context(), &layer); err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	created, err := store.GetFSLayer(r.Context(), layerID)
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerResponse(created))
}

func (s *Server) handleFSLayerList(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	layers, err := store.ListFSLayers(r.Context())
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	out := make([]fsLayerResponse, 0, len(layers))
	for i := range layers {
		out = append(out, toFSLayerResponse(&layers[i]))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"layers": out})
}

func (s *Server) handleFSLayerObject(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/fs-layers/")
	rawID, sub, hasSub := strings.Cut(rest, "/")
	if rawID == "" {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	layerRef, err := url.PathUnescape(rawID)
	if err != nil || layerRef == "" {
		errJSON(w, http.StatusBadRequest, "invalid layer ref")
		return
	}
	layer, err := store.ResolveFSLayerRef(r.Context(), layerRef)
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	layerID := layer.LayerID
	switch {
	case !hasSub:
		if r.Method != http.MethodGet {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(toFSLayerResponse(layer))
	case sub == "diff":
		if r.Method != http.MethodGet {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		maxSeq, hasMaxSeq, err := fsLayerMaxSeqFromRequest(r)
		if err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		var entries []datastore.FSLayerEntry
		if hasMaxSeq {
			entries, err = store.ListFSLayerEntriesAtSeq(r.Context(), layerID, maxSeq)
		} else {
			entries, err = store.ListFSLayerEntries(r.Context(), layerID)
		}
		if err != nil {
			writeFSLayerStoreError(w, err)
			return
		}
		out := make([]fsLayerEntryResponse, 0, len(entries))
		for i := range entries {
			out = append(out, toFSLayerEntryResponse(&entries[i], false))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
	case sub == "checkpoint":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleFSLayerCheckpoint(w, r, store, layerID)
	case sub == "entries":
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			s.handleFSLayerEntryUpsert(w, r, b, store, layer)
		case http.MethodGet:
			s.handleFSLayerEntryGet(w, r, store, layer)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case sub == "rollback":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := store.RollbackFSLayer(r.Context(), layerID); err != nil {
			writeFSLayerStoreError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	case sub == "commit":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleFSLayerCommit(w, r, b, store, layer)
	default:
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleFSLayerEntryUpsert(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer) {
	if layer.State != datastore.FSLayerStateActive {
		errJSON(w, http.StatusConflict, "fs layer is not active")
		return
	}
	defer func() { _ = r.Body.Close() }()
	var req fsLayerEntryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFSLayerEntryBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	entry := datastore.FSLayerEntry{
		LayerID:        layer.LayerID,
		Path:           req.Path,
		Op:             datastore.FSLayerEntryOp(strings.TrimSpace(req.Op)),
		Kind:           datastore.FSLayerEntryKind(strings.TrimSpace(req.Kind)),
		BaseInodeID:    strings.TrimSpace(req.BaseInodeID),
		BaseRevision:   req.BaseRevision,
		StorageType:    strings.TrimSpace(req.StorageType),
		StorageRef:     strings.TrimSpace(req.StorageRef),
		StorageRefHash: strings.TrimSpace(req.StorageRefHash),
		ContentBlob:    req.Content,
		ContentType:    strings.TrimSpace(req.ContentType),
		ContentText:    req.ContentText,
		ChecksumSHA256: strings.TrimSpace(req.ChecksumSHA256),
		SizeBytes:      req.SizeBytes,
		Mode:           req.Mode,
	}
	if err := normalizeAndValidateFSLayerServerEntry(layer, &entry); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(entry.ContentBlob) > 0 {
		if entry.SizeBytes == 0 {
			entry.SizeBytes = int64(len(entry.ContentBlob))
		}
		if entry.ChecksumSHA256 == "" {
			sum := sha256.Sum256(entry.ContentBlob)
			entry.ChecksumSHA256 = hex.EncodeToString(sum[:])
		}
		if entry.ContentType == "" {
			entry.ContentType = http.DetectContentType(entry.ContentBlob)
		}
	}
	fillFSLayerBaseSnapshot(r.Context(), b, &entry)
	if err := store.UpsertFSLayerEntry(r.Context(), &entry); err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	stored, err := store.GetFSLayerEntry(r.Context(), layer.LayerID, entry.Path)
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerEntryResponse(stored, true))
}

func (s *Server) handleFSLayerEntryGet(w http.ResponseWriter, r *http.Request, store *datastore.Store, layer *datastore.FSLayer) {
	path := strings.TrimSpace(r.URL.Query().Get("path"))
	if path == "" {
		errJSON(w, http.StatusBadRequest, "path is required")
		return
	}
	path, err := canonicalFSLayerServerPath(path, "", "")
	if err != nil {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("invalid fs layer entry path: %v", err))
		return
	}
	if !fsLayerPathWithinBase(path, layer.BaseRootPath) {
		errJSON(w, http.StatusBadRequest, "fs layer entry path is outside base root")
		return
	}
	maxSeq, hasMaxSeq, err := fsLayerMaxSeqFromRequest(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	var entry *datastore.FSLayerEntry
	if hasMaxSeq {
		entry, err = store.GetFSLayerEntryAtSeq(r.Context(), layer.LayerID, path, maxSeq)
	} else {
		entry, err = store.GetFSLayerEntry(r.Context(), layer.LayerID, path)
	}
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerEntryResponse(entry, true))
}

func (s *Server) handleFSLayerCommit(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer) {
	if layer.State != datastore.FSLayerStateActive && layer.State != datastore.FSLayerStateSealed {
		errJSON(w, http.StatusConflict, "fs layer is not active or sealed")
		return
	}
	if err := store.BeginFSLayerCommit(r.Context(), layer.LayerID); err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	entries, err := store.ListFSLayerEntries(r.Context(), layer.LayerID)
	if err != nil {
		_ = store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateConflicted)
		writeFSLayerStoreError(w, err)
		return
	}
	if conflicts := validateFSLayerCommitScope(layer, entries); len(conflicts) > 0 {
		_ = store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateConflicted)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(fsLayerCommitResponse{
			Status:    "conflicted",
			LayerID:   layer.LayerID,
			Conflicts: conflicts,
		})
		return
	}
	conflicts := preflightFSLayerCommit(r.Context(), b, entries)
	if len(conflicts) > 0 {
		_ = store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateConflicted)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(fsLayerCommitResponse{
			Status:    "conflicted",
			LayerID:   layer.LayerID,
			Conflicts: conflicts,
		})
		return
	}
	snapshots := snapshotFSLayerCommit(r.Context(), b, entries)
	if err := validateFSLayerCommitSnapshots(snapshots); err != nil {
		_ = store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateConflicted)
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	rollbackCtx, rollbackCancel := context.WithTimeout(context.WithoutCancel(r.Context()), fsLayerRollbackTimeout)
	defer rollbackCancel()
	for i := range entries {
		if err := applyFSLayerEntry(r.Context(), b, &entries[i]); err != nil {
			rollbackErr := rollbackFSLayerCommit(rollbackCtx, b, snapshots)
			_ = store.SetFSLayerState(rollbackCtx, layer.LayerID, datastore.FSLayerStateConflicted)
			if rollbackErr != nil {
				errJSON(w, http.StatusInternalServerError, fmt.Sprintf("apply fs layer entry %s: %v; rollback failed: %v", entries[i].Path, err, rollbackErr))
				return
			}
			errJSON(w, http.StatusInternalServerError, fmt.Sprintf("apply fs layer entry %s: %v", entries[i].Path, err))
			return
		}
	}
	if err := store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateCommitted); err != nil {
		rollbackErr := rollbackFSLayerCommit(rollbackCtx, b, snapshots)
		_ = store.SetFSLayerState(rollbackCtx, layer.LayerID, datastore.FSLayerStateConflicted)
		if rollbackErr != nil {
			errJSON(w, http.StatusInternalServerError, fmt.Sprintf("finalize fs layer commit %s: %v; rollback failed: %v", layer.LayerID, err, rollbackErr))
			return
		}
		errJSON(w, http.StatusInternalServerError, fmt.Sprintf("finalize fs layer commit %s: %v", layer.LayerID, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fsLayerCommitResponse{
		Status:  "committed",
		LayerID: layer.LayerID,
		Applied: len(entries),
	})
}

func (s *Server) handleFSLayerCheckpoint(w http.ResponseWriter, r *http.Request, store *datastore.Store, layerID string) {
	defer func() { _ = r.Body.Close() }()
	var req fsLayerCheckpointRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFSLayerBodyBytes)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "malformed JSON")
		return
	}
	checkpointID := strings.TrimSpace(req.CheckpointID)
	if checkpointID == "" {
		checkpointID = journal.NewID("ckpt")
	}
	checkpoint := datastore.FSLayerCheckpoint{
		CheckpointID: checkpointID,
		LayerID:      layerID,
		Label:        strings.TrimSpace(req.Label),
	}
	if err := store.CreateFSLayerCheckpoint(r.Context(), &checkpoint); err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	stored, err := store.GetFSLayerCheckpoint(r.Context(), checkpointID)
	if err != nil {
		writeFSLayerStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerCheckpointResponse(stored))
}

func fsLayerMaxSeqFromRequest(r *http.Request) (int64, bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("max_seq"))
	if raw == "" {
		return 0, false, nil
	}
	seq, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seq < 0 {
		return 0, false, fmt.Errorf("invalid max_seq")
	}
	return seq, true, nil
}

func normalizeAndValidateFSLayerServerEntry(layer *datastore.FSLayer, entry *datastore.FSLayerEntry) error {
	if layer == nil || entry == nil {
		return fmt.Errorf("fs layer entry is required")
	}
	path, err := canonicalFSLayerServerPath(entry.Path, string(entry.Op), string(entry.Kind))
	if err != nil {
		return fmt.Errorf("invalid fs layer entry path: %w", err)
	}
	if !fsLayerPathWithinBase(path, layer.BaseRootPath) {
		return fmt.Errorf("fs layer entry path is outside base root")
	}
	entry.Path = path
	if entry.Op == datastore.FSLayerEntryOpRename {
		if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(path, "/") {
			return fmt.Errorf("fs layer directory rename is not supported")
		}
		target := strings.TrimSpace(entry.ContentText)
		if target == "" && len(entry.ContentBlob) > 0 {
			target = strings.TrimSpace(string(entry.ContentBlob))
		}
		if strings.HasSuffix(target, "/") {
			return fmt.Errorf("fs layer directory rename is not supported")
		}
		targetPath, err := canonicalFSLayerServerPath(target, "", "")
		if err != nil {
			return fmt.Errorf("invalid fs layer rename target: %w", err)
		}
		if !fsLayerPathWithinBase(targetPath, layer.BaseRootPath) {
			return fmt.Errorf("fs layer rename target is outside base root")
		}
		entry.ContentText = targetPath
	}
	return nil
}

func canonicalFSLayerServerPath(rawPath, op, kind string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", fmt.Errorf("path is required")
	}
	if kind == string(datastore.FSLayerEntryKindDir) || op == string(datastore.FSLayerEntryOpMkdir) || strings.HasSuffix(rawPath, "/") {
		return pathutil.CanonicalizeDir(rawPath)
	}
	return pathutil.Canonicalize(rawPath)
}

func fsLayerPathWithinBase(path, baseRoot string) bool {
	if baseRoot == "/" {
		return strings.HasPrefix(path, "/")
	}
	if path == baseRoot {
		return true
	}
	return strings.HasPrefix(path, baseRoot)
}

func validateFSLayerCommitScope(layer *datastore.FSLayer, entries []datastore.FSLayerEntry) []fsLayerCommitConflict {
	var conflicts []fsLayerCommitConflict
	for i := range entries {
		if !fsLayerPathWithinBase(entries[i].Path, layer.BaseRootPath) {
			conflicts = append(conflicts, fsLayerCommitConflict{Path: entries[i].Path, Reason: "entry outside base root"})
		}
		if entries[i].Op == datastore.FSLayerEntryOpRename {
			if entries[i].Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entries[i].Path, "/") {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entries[i].Path, Reason: "directory rename unsupported"})
				continue
			}
			target := strings.TrimSpace(entries[i].ContentText)
			if target == "" && len(entries[i].ContentBlob) > 0 {
				target = strings.TrimSpace(string(entries[i].ContentBlob))
			}
			targetPath, err := canonicalFSLayerServerPath(target, "", "")
			if err != nil {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entries[i].Path, Reason: "invalid rename target"})
				continue
			}
			if !fsLayerPathWithinBase(targetPath, layer.BaseRootPath) {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entries[i].Path, Reason: "rename target outside base root"})
			}
		}
	}
	return conflicts
}

type fsLayerBaseSnapshot struct {
	Path        string
	Exists      bool
	IsDir       bool
	IsSymlink   bool
	CleanupOnly bool
	Target      string
	Data        []byte
	Mode        uint32
	ModeIsSet   bool
	SnapshotOK  bool
}

func snapshotFSLayerCommit(ctx context.Context, b *backendpkg.Dat9Backend, entries []datastore.FSLayerEntry) []fsLayerBaseSnapshot {
	seen := make(map[string]struct{})
	var paths []string
	cleanupOnly := make(map[string]bool)
	addTrackedPath := func(p string, cleanup bool) {
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			if !cleanup {
				delete(cleanupOnly, p)
			}
			return
		}
		seen[p] = struct{}{}
		paths = append(paths, p)
		if cleanup {
			cleanupOnly[p] = true
		}
	}
	addParentCleanupPaths := func(p string) {
		for parent := pathutil.ParentPath(p); parent != "/" && parent != ""; parent = pathutil.ParentPath(parent) {
			addTrackedPath(parent, true)
		}
	}
	for i := range entries {
		addParentCleanupPaths(entries[i].Path)
		addTrackedPath(entries[i].Path, false)
		if entries[i].Op == datastore.FSLayerEntryOpRename {
			target := strings.TrimSpace(entries[i].ContentText)
			if target == "" && len(entries[i].ContentBlob) > 0 {
				target = strings.TrimSpace(string(entries[i].ContentBlob))
			}
			if targetPath, err := canonicalFSLayerServerPath(target, "", ""); err == nil {
				addParentCleanupPaths(targetPath)
				addTrackedPath(targetPath, false)
			}
		}
	}
	out := make([]fsLayerBaseSnapshot, 0, len(paths))
	for _, p := range paths {
		snap := snapshotFSLayerBasePath(ctx, b, p)
		snap.CleanupOnly = cleanupOnly[p]
		out = append(out, snap)
	}
	return out
}

func validateFSLayerCommitSnapshots(snapshots []fsLayerBaseSnapshot) error {
	for i := range snapshots {
		if !snapshots[i].SnapshotOK {
			return fmt.Errorf("snapshot fs layer base path %s failed", snapshots[i].Path)
		}
	}
	return nil
}

func snapshotFSLayerBasePath(ctx context.Context, b *backendpkg.Dat9Backend, path string) fsLayerBaseSnapshot {
	snap := fsLayerBaseSnapshot{Path: path, SnapshotOK: true}
	nf, err := b.StatNodeCtx(ctx, path)
	if errors.Is(err, datastore.ErrNotFound) {
		return snap
	}
	if err != nil {
		snap.SnapshotOK = false
		return snap
	}
	snap.Exists = true
	snap.IsDir = nf.Node.IsDirectory
	if nf.Mode != 0 {
		snap.Mode = nf.Mode
		snap.ModeIsSet = true
	}
	if nf.File != nil {
		if nf.File.Mode != 0 {
			snap.Mode = nf.File.Mode
			snap.ModeIsSet = true
		}
		snap.IsSymlink = isFSLayerSymlinkMode(nf.File.Mode)
		data, err := b.ReadCtx(ctx, path, 0, -1)
		if err != nil {
			snap.SnapshotOK = false
			return snap
		}
		if snap.IsSymlink {
			snap.Target = string(data)
		} else {
			snap.Data = append([]byte(nil), data...)
		}
	}
	return snap
}

func rollbackFSLayerCommit(ctx context.Context, b *backendpkg.Dat9Backend, snapshots []fsLayerBaseSnapshot) error {
	for i := len(snapshots) - 1; i >= 0; i-- {
		if err := restoreFSLayerBaseSnapshot(ctx, b, &snapshots[i]); err != nil {
			return err
		}
	}
	return nil
}

func restoreFSLayerBaseSnapshot(ctx context.Context, b *backendpkg.Dat9Backend, snap *fsLayerBaseSnapshot) error {
	if snap == nil || snap.Path == "" {
		return nil
	}
	if !snap.SnapshotOK {
		return fmt.Errorf("no rollback snapshot for %s", snap.Path)
	}
	if snap.CleanupOnly {
		if snap.Exists {
			return nil
		}
		return removeFSLayerBasePathIfExists(ctx, b, snap.Path)
	}
	if err := removeFSLayerBasePathIfExists(ctx, b, snap.Path); err != nil {
		return err
	}
	if !snap.Exists {
		return nil
	}
	if snap.IsDir {
		mode := snap.Mode & 0o777
		if mode == 0 {
			mode = 0o755
		}
		return b.MkdirCtx(ctx, snap.Path, mode)
	}
	if snap.IsSymlink {
		if err := b.CreateSymlinkCtx(ctx, snap.Path, snap.Target); err != nil {
			return err
		}
		return nil
	}
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, snap.Path, snap.Data, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		return err
	}
	if snap.ModeIsSet {
		if err := b.ChmodCtx(ctx, snap.Path, snap.Mode&0o777); err != nil {
			return err
		}
	}
	return nil
}

func removeFSLayerBasePathIfExists(ctx context.Context, b *backendpkg.Dat9Backend, path string) error {
	nf, err := b.StatNodeCtx(ctx, path)
	if errors.Is(err, datastore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if nf.Node.IsDirectory {
		return b.RemoveAllCtx(ctx, path)
	}
	return b.RemoveCtx(ctx, path)
}

func isFSLayerSymlinkMode(mode uint32) bool {
	return mode&0o170000 == 0o120000
}

func fillFSLayerBaseSnapshot(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) {
	if b == nil || entry == nil || entry.BaseRevision != 0 || entry.BaseInodeID != "" {
		return
	}
	nf, err := b.StatNodeCtx(ctx, entry.Path)
	if err != nil {
		return
	}
	if nf.File != nil {
		entry.BaseInodeID = nf.File.FileID
		entry.BaseRevision = nf.File.Revision
		return
	}
	if nf.Node.InodeID != "" {
		entry.BaseInodeID = nf.Node.InodeID
	}
}

func preflightFSLayerCommit(ctx context.Context, b *backendpkg.Dat9Backend, entries []datastore.FSLayerEntry) []fsLayerCommitConflict {
	var conflicts []fsLayerCommitConflict
	for i := range entries {
		entry := &entries[i]
		if entry.Op == datastore.FSLayerEntryOpRename {
			conflicts = append(conflicts, preflightFSLayerRename(ctx, b, entry)...)
		}
		if entry.Op == datastore.FSLayerEntryOpWhiteout && entry.BaseRevision == 0 {
			continue
		}
		if entry.BaseRevision == 0 && entry.BaseInodeID == "" {
			if _, err := b.StatNodeCtx(ctx, entry.Path); err == nil {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "base path exists"})
			} else if !errors.Is(err, datastore.ErrNotFound) {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
			}
			continue
		}
		if entry.BaseRevision <= 0 {
			nf, err := b.StatNodeCtx(ctx, entry.Path)
			if errors.Is(err, datastore.ErrNotFound) {
				conflicts = append(conflicts, fsLayerCommitConflict{
					Path:   entry.Path,
					Reason: "base path missing",
				})
				continue
			}
			if err != nil {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
				continue
			}
			baseInodeID := ""
			if nf.Node.InodeID != "" {
				baseInodeID = nf.Node.InodeID
			}
			if nf.File != nil && nf.File.FileID != "" {
				baseInodeID = nf.File.FileID
			}
			if baseInodeID != entry.BaseInodeID {
				conflicts = append(conflicts, fsLayerCommitConflict{
					Path:   entry.Path,
					Reason: "base inode changed",
				})
			}
			continue
		}
		nf, err := b.StatNodeCtx(ctx, entry.Path)
		if errors.Is(err, datastore.ErrNotFound) {
			conflicts = append(conflicts, fsLayerCommitConflict{
				Path:         entry.Path,
				Reason:       "base path missing",
				WantRevision: entry.BaseRevision,
			})
			continue
		}
		if err != nil {
			conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
			continue
		}
		if nf.File == nil {
			continue
		}
		if nf.File.Revision != entry.BaseRevision {
			conflicts = append(conflicts, fsLayerCommitConflict{
				Path:         entry.Path,
				Reason:       "base revision changed",
				BaseRevision: nf.File.Revision,
				WantRevision: entry.BaseRevision,
			})
		}
	}
	return conflicts
}

func preflightFSLayerRename(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) []fsLayerCommitConflict {
	var conflicts []fsLayerCommitConflict
	if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/") {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "directory rename unsupported"})
	}
	source, err := b.StatNodeCtx(ctx, entry.Path)
	if errors.Is(err, datastore.ErrNotFound) {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "base path missing"})
	}
	if err != nil {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
	}
	if source.Node.IsDirectory {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "directory rename unsupported"})
	}
	target := strings.TrimSpace(entry.ContentText)
	if target == "" && len(entry.ContentBlob) > 0 {
		target = strings.TrimSpace(string(entry.ContentBlob))
	}
	if target == "" {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "rename target is required"})
	}
	targetPath, err := canonicalFSLayerServerPath(target, "", "")
	if err != nil {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "invalid rename target"})
	}
	if targetPath == entry.Path {
		return conflicts
	}
	if _, err := b.StatNodeCtx(ctx, targetPath); err == nil {
		return append(conflicts, fsLayerCommitConflict{Path: targetPath, Reason: "rename target exists"})
	} else if !errors.Is(err, datastore.ErrNotFound) {
		return append(conflicts, fsLayerCommitConflict{Path: targetPath, Reason: err.Error()})
	}
	return conflicts
}

func applyFSLayerEntry(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) error {
	switch entry.Op {
	case datastore.FSLayerEntryOpUpsert:
		if entry.StorageRef != "" && len(entry.ContentBlob) == 0 {
			return fmt.Errorf("external layer storage refs are not supported by commit yet")
		}
		_, _, err := b.WriteCtxIfRevisionWithTagsResult(
			ctx,
			entry.Path,
			entry.ContentBlob,
			0,
			filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
			entry.BaseRevision,
			nil,
			"",
		)
		if err != nil {
			return err
		}
		if entry.Mode != 0 {
			return applyFSLayerEntryMode(ctx, b, entry.Path, entry.Mode&0o777)
		}
		return nil
	case datastore.FSLayerEntryOpMkdir:
		mode := entry.Mode
		if mode == 0 {
			mode = 0o755
		}
		return b.MkdirCtx(ctx, entry.Path, mode)
	case datastore.FSLayerEntryOpWhiteout:
		var err error
		if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/") {
			err = b.RemoveAllCtx(ctx, entry.Path)
		} else {
			err = b.RemoveCtx(ctx, entry.Path)
		}
		if errors.Is(err, datastore.ErrNotFound) {
			return nil
		}
		return err
	case datastore.FSLayerEntryOpChmod:
		mode := entry.Mode
		if mode == 0 {
			return nil
		}
		return b.ChmodCtx(ctx, entry.Path, mode)
	case datastore.FSLayerEntryOpSymlink:
		target := entry.ContentText
		if target == "" && len(entry.ContentBlob) > 0 {
			target = string(entry.ContentBlob)
		}
		if target == "" {
			return fmt.Errorf("symlink target is required")
		}
		return b.CreateSymlinkCtx(ctx, entry.Path, target)
	case datastore.FSLayerEntryOpRename:
		target := strings.TrimSpace(entry.ContentText)
		if target == "" && len(entry.ContentBlob) > 0 {
			target = strings.TrimSpace(string(entry.ContentBlob))
		}
		if target == "" {
			return fmt.Errorf("rename target is required")
		}
		return b.RenameFileNoReplaceCtx(ctx, entry.Path, target)
	default:
		return fmt.Errorf("unsupported fs layer op %q", entry.Op)
	}
}

func applyFSLayerEntryMode(ctx context.Context, b *backendpkg.Dat9Backend, path string, mode uint32) error {
	if mode == 0 {
		return nil
	}
	if err := b.ChmodCtx(ctx, path, mode); err != nil {
		if !errors.Is(err, datastore.ErrNotFound) {
			return err
		}
		nf, statErr := b.StatNodeCtx(ctx, path)
		if statErr == nil && nf.File != nil && nf.Node.InodeID == "" {
			return nil
		}
		if statErr != nil && !errors.Is(statErr, datastore.ErrNotFound) {
			return statErr
		}
		return err
	}
	return nil
}

func writeFSLayerStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, datastore.ErrNotFound) {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	if errors.Is(err, datastore.ErrFSLayerRefAmbiguous) {
		errJSON(w, http.StatusConflict, err.Error())
		return
	}
	if errors.Is(err, datastore.ErrFSLayerStateConflict) {
		errJSON(w, http.StatusConflict, err.Error())
		return
	}
	errJSON(w, http.StatusInternalServerError, err.Error())
}

func toFSLayerResponse(layer *datastore.FSLayer) fsLayerResponse {
	return fsLayerResponse{
		LayerID:        layer.LayerID,
		BaseRootPath:   layer.BaseRootPath,
		Name:           layer.Name,
		Tags:           layer.Tags,
		State:          string(layer.State),
		DurabilityMode: string(layer.DurabilityMode),
		ActorID:        layer.ActorID,
		DurableSeq:     layer.DurableSeq,
		CreatedAt:      layer.CreatedAt,
		UpdatedAt:      layer.UpdatedAt,
		SealedAt:       layer.SealedAt,
	}
}

func toFSLayerEntryResponse(entry *datastore.FSLayerEntry, includeContent bool) fsLayerEntryResponse {
	resp := fsLayerEntryResponse{
		LayerID:        entry.LayerID,
		Path:           entry.Path,
		ParentPath:     entry.ParentPath,
		Name:           entry.Name,
		Op:             string(entry.Op),
		Kind:           string(entry.Kind),
		BaseInodeID:    entry.BaseInodeID,
		BaseRevision:   entry.BaseRevision,
		StorageType:    entry.StorageType,
		StorageRef:     entry.StorageRef,
		StorageRefHash: entry.StorageRefHash,
		ChecksumSHA256: entry.ChecksumSHA256,
		SizeBytes:      entry.SizeBytes,
		Mode:           entry.Mode,
		ContentText:    entry.ContentText,
		EntrySeq:       entry.EntrySeq,
		CreatedAt:      entry.CreatedAt,
		UpdatedAt:      entry.UpdatedAt,
	}
	if includeContent {
		resp.Content = entry.ContentBlob
	}
	return resp
}

func toFSLayerCheckpointResponse(checkpoint *datastore.FSLayerCheckpoint) fsLayerCheckpointResponse {
	return fsLayerCheckpointResponse{
		CheckpointID: checkpoint.CheckpointID,
		LayerID:      checkpoint.LayerID,
		DurableSeq:   checkpoint.DurableSeq,
		Label:        checkpoint.Label,
		CreatedAt:    checkpoint.CreatedAt,
	}
}
