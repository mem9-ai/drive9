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
	"strings"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	backendpkg "github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/journal"
)

const maxFSLayerBodyBytes = 1 << 20
const maxFSLayerEntryBodyBytes = 128 << 20

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
		entries, err := store.ListFSLayerEntries(r.Context(), layerID)
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
	entry, err := store.GetFSLayerEntry(r.Context(), layer.LayerID, path)
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
	entries, err := store.ListFSLayerEntries(r.Context(), layer.LayerID)
	if err != nil {
		writeFSLayerStoreError(w, err)
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
	for i := range entries {
		if err := applyFSLayerEntry(r.Context(), b, &entries[i]); err != nil {
			_ = store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateConflicted)
			errJSON(w, http.StatusInternalServerError, fmt.Sprintf("apply fs layer entry %s: %v", entries[i].Path, err))
			return
		}
	}
	if err := store.SetFSLayerState(r.Context(), layer.LayerID, datastore.FSLayerStateCommitted); err != nil {
		writeFSLayerStoreError(w, err)
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
		return b.RenameCtx(ctx, entry.Path, target)
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
