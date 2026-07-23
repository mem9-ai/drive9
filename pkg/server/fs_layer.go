package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	backendpkg "github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/journal"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/pathutil"
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
	LayerID                string    `json:"layer_id"`
	Path                   string    `json:"path"`
	ParentPath             string    `json:"parent_path"`
	Name                   string    `json:"name"`
	Op                     string    `json:"op"`
	Kind                   string    `json:"kind"`
	BaseInodeID            string    `json:"base_inode_id"`
	BaseRevision           int64     `json:"base_revision"`
	StorageType            string    `json:"storage_type"`
	StorageRef             string    `json:"storage_ref"`
	StorageRefHash         string    `json:"storage_ref_hash"`
	StorageEncryptionMode  string    `json:"storage_encryption_mode"`
	StorageEncryptionKeyID string    `json:"storage_encryption_key_id"`
	ChecksumSHA256         string    `json:"checksum_sha256"`
	SizeBytes              int64     `json:"size_bytes"`
	Mode                   uint32    `json:"mode"`
	Content                []byte    `json:"content,omitempty"`
	ContentText            string    `json:"content_text,omitempty"`
	EntrySeq               int64     `json:"entry_seq"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}

type fsLayerEntryRequest struct {
	Path                   string `json:"path"`
	Op                     string `json:"op,omitempty"`
	Kind                   string `json:"kind,omitempty"`
	BaseInodeID            string `json:"base_inode_id,omitempty"`
	BaseRevision           int64  `json:"base_revision,omitempty"`
	StorageType            string `json:"storage_type,omitempty"`
	StorageRef             string `json:"storage_ref,omitempty"`
	StorageRefHash         string `json:"storage_ref_hash,omitempty"`
	StorageEncryptionMode  string `json:"storage_encryption_mode,omitempty"`
	StorageEncryptionKeyID string `json:"storage_encryption_key_id,omitempty"`
	Content                []byte `json:"content,omitempty"`
	ContentType            string `json:"content_type,omitempty"`
	ContentText            string `json:"content_text,omitempty"`
	ChecksumSHA256         string `json:"checksum_sha256,omitempty"`
	SizeBytes              int64  `json:"size_bytes,omitempty"`
	Mode                   uint32 `json:"mode,omitempty"`
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

type fsLayerEventResponse struct {
	EventID   string    `json:"event_id"`
	LayerID   string    `json:"layer_id"`
	Seq       int64     `json:"seq"`
	ActorID   string    `json:"actor_id"`
	Op        string    `json:"op"`
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Server) handleFSLayers(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	store := b.Store()
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/layer-checkpoints/"):
		s.handleFSLayerCheckpointObject(w, r, store)
	case r.URL.Path == "/v1/layers":
		switch r.Method {
		case http.MethodPost:
			s.handleFSLayerCreate(w, r, store)
		case http.MethodGet:
			s.handleFSLayerList(w, r, store)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case strings.HasPrefix(r.URL.Path, "/v1/layers/"):
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
	rawID := strings.TrimPrefix(r.URL.Path, "/v1/layer-checkpoints/")
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
		writeFSLayerStoreError(w, r, err)
		return
	}
	layer, err := store.GetFSLayer(r.Context(), checkpoint.LayerID)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	if !authorizeFS(w, r, FSOpRead, layer.BaseRootPath) {
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
	baseRoot, err := pathutil.CanonicalizeDir(req.BaseRootPath)
	if err != nil {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("invalid fs layer base root: %v", err))
		return
	}
	if !authorizeFS(w, r, FSOpWrite, baseRoot) {
		return
	}
	layer := datastore.FSLayer{
		LayerID:        layerID,
		BaseRootPath:   baseRoot,
		Name:           req.Name,
		Tags:           req.Tags,
		DurabilityMode: datastore.FSLayerDurabilityMode(strings.TrimSpace(req.DurabilityMode)),
		ActorID:        actorID,
	}
	if err := store.CreateFSLayer(r.Context(), &layer); err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	created, err := store.GetFSLayer(r.Context(), layerID)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerResponse(created))
}

func (s *Server) handleFSLayerList(w http.ResponseWriter, r *http.Request, store *datastore.Store) {
	layers, err := store.ListFSLayers(r.Context())
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	out := make([]fsLayerResponse, 0, len(layers))
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	for i := range layers {
		if !fsLayerVisibleToScope(scope, &layers[i]) {
			continue
		}
		out = append(out, toFSLayerResponse(&layers[i]))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"layers": out})
}

func fsLayerVisibleToScope(scope *TenantScope, layer *datastore.FSLayer) bool {
	if scope == nil || layer == nil {
		return false
	}
	if !scope.IsScoped {
		return true
	}
	return scope.AuthorizeFS(FSOpRead, layer.BaseRootPath) == nil || scope.AuthorizeFS(FSOpWrite, layer.BaseRootPath) == nil
}

func authorizeFSLayerEntryMutation(w http.ResponseWriter, r *http.Request, entry *datastore.FSLayerEntry) bool {
	if entry == nil {
		errJSON(w, http.StatusBadRequest, "fs layer entry is required")
		return false
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpWhiteout:
		return authorizeFS(w, r, FSOpDelete, entry.Path)
	case datastore.FSLayerEntryOpRename:
		target := strings.TrimSpace(entry.ContentText)
		if target == "" && len(entry.ContentBlob) > 0 {
			target = strings.TrimSpace(string(entry.ContentBlob))
		}
		return authorizeFSPair(w, r, FSOpDelete, entry.Path, FSOpWrite, target)
	default:
		return authorizeFS(w, r, FSOpWrite, entry.Path)
	}
}

func (s *Server) handleFSLayerObject(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/layers/")
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
		writeFSLayerStoreError(w, r, err)
		return
	}
	layerID := layer.LayerID
	switch {
	case !hasSub:
		if r.Method != http.MethodGet {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpRead, layer.BaseRootPath) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(toFSLayerResponse(layer))
	case sub == "diff":
		if r.Method != http.MethodGet {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpRead, layer.BaseRootPath) {
			return
		}
		maxSeq, hasMaxSeq, err := fsLayerMaxSeqFromRequest(r)
		if err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		var entries []datastore.FSLayerEntry
		replayLog := fsLayerReplayLogFromRequest(r)
		if replayLog && hasMaxSeq {
			entries, err = store.ListFSLayerEntryLogAtSeq(r.Context(), layerID, maxSeq)
		} else if replayLog {
			entries, err = store.ListFSLayerEntryLog(r.Context(), layerID)
		} else if hasMaxSeq {
			entries, err = store.ListFSLayerEntriesAtSeq(r.Context(), layerID, maxSeq)
		} else {
			entries, err = store.ListFSLayerEntries(r.Context(), layerID)
		}
		if err != nil {
			writeFSLayerStoreError(w, r, err)
			return
		}
		out := make([]fsLayerEntryResponse, 0, len(entries))
		for i := range entries {
			out = append(out, toFSLayerEntryResponse(&entries[i], false))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": out})
	case sub == "checkpoints":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpWrite, layer.BaseRootPath) {
			return
		}
		s.handleFSLayerCheckpoint(w, r, store, layerID)
	case sub == "entries":
		switch r.Method {
		case http.MethodPost, http.MethodPut:
			if !authorizeFS(w, r, FSOpWrite, layer.BaseRootPath) {
				return
			}
			s.handleFSLayerEntryUpsert(w, r, b, store, layer)
		case http.MethodGet:
			s.handleFSLayerEntryGet(w, r, store, layer)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case sub == "objects":
		if r.Method == http.MethodGet {
			s.handleFSLayerObjectRead(w, r, b, store, layer)
			return
		}
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpWrite, layer.BaseRootPath) {
			return
		}
		s.handleFSLayerObjectUpload(w, r, b, store, layer)
	case sub == "events":
		if r.Method != http.MethodGet {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpRead, layer.BaseRootPath) {
			return
		}
		s.handleFSLayerEvents(w, r, store, layer)
	case sub == "rollback":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpWrite, layer.BaseRootPath) {
			return
		}
		if err := store.RollbackFSLayer(r.Context(), layerID); err != nil {
			writeFSLayerStoreError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	case sub == "commit":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !authorizeFS(w, r, FSOpWrite, layer.BaseRootPath) {
			return
		}
		s.handleFSLayerCommit(w, r, b, store, layer)
	default:
		errJSON(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleFSLayerObjectRead(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer) {
	rawPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if rawPath == "" {
		errJSON(w, http.StatusBadRequest, "path is required")
		return
	}
	path, err := canonicalFSLayerServerPath(rawPath, "", string(datastore.FSLayerEntryKindFile))
	if err != nil {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("invalid fs layer object path: %v", err))
		return
	}
	if !fsLayerPathWithinBase(path, layer.BaseRootPath) {
		errJSON(w, http.StatusBadRequest, "fs layer object path is outside base root")
		return
	}
	if !authorizeFS(w, r, FSOpRead, path) {
		return
	}
	maxSeq := parseFSLayerInt64Query(r, "max_seq")
	var (
		entry *datastore.FSLayerEntry
	)
	if maxSeq > 0 {
		entry, err = store.GetFSLayerEntryAtSeq(r.Context(), layer.LayerID, path, maxSeq)
	} else {
		entry, err = store.GetFSLayerEntry(r.Context(), layer.LayerID, path)
	}
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	if entry.Op != datastore.FSLayerEntryOpUpsert || entry.Kind != datastore.FSLayerEntryKindFile {
		errJSON(w, http.StatusBadRequest, "fs layer object path is not a file upsert")
		return
	}
	rc, err := b.OpenFSLayerEntryData(r.Context(), entry)
	if err != nil {
		logger.Error(r.Context(), "fs_layer_object_read_failed", eventFields(r.Context(), "fs_layer_object_read_failed", "path", entry.Path, "error", err)...)
		writeBackendError(w, r, err)
		return
	}
	defer func() { _ = rc.Close() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	if entry.SizeBytes > 0 || (entry.SizeBytes == 0 && len(entry.ContentBlob) == 0) {
		w.Header().Set("Content-Length", strconv.FormatInt(entry.SizeBytes, 10))
	}
	if _, err := io.Copy(w, rc); err != nil {
		return
	}
}

func (s *Server) handleFSLayerObjectUpload(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer) {
	if layer.State != datastore.FSLayerStateActive {
		errJSON(w, http.StatusConflict, "fs layer is not active")
		return
	}
	defer func() { _ = r.Body.Close() }()
	rawPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if rawPath == "" {
		errJSON(w, http.StatusBadRequest, "path is required")
		return
	}
	size := r.ContentLength
	if rawSize := strings.TrimSpace(r.URL.Query().Get("size")); rawSize != "" {
		parsed, err := strconv.ParseInt(rawSize, 10, 64)
		if err != nil || parsed < 0 {
			errJSON(w, http.StatusBadRequest, "invalid size")
			return
		}
		size = parsed
	}
	if size < 0 {
		errJSON(w, http.StatusBadRequest, "content length is required")
		return
	}
	if size > s.maxUploadBytes {
		errJSON(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("upload too large: max %d bytes", s.maxUploadBytes))
		return
	}
	prepared := datastore.FSLayerEntry{
		LayerID:      layer.LayerID,
		Path:         rawPath,
		Op:           datastore.FSLayerEntryOpUpsert,
		Kind:         datastore.FSLayerEntryKindFile,
		BaseRevision: parseFSLayerInt64Query(r, "base_revision"),
	}
	if mode, ok := parseFSLayerModeQuery(r, "mode"); ok {
		prepared.Mode = mode
	}
	if prepared.Mode == 0 {
		prepared.Mode = 0o644
	}
	if err := normalizeAndValidateFSLayerServerEntry(layer, &prepared); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if !authorizeFSLayerEntryMutation(w, r, &prepared) {
		return
	}
	fillFSLayerBaseSnapshot(r.Context(), b, &prepared)
	body := http.MaxBytesReader(w, r.Body, s.maxUploadBytes)
	entry, err := b.PutFSLayerObject(r.Context(), layer.LayerID, prepared.Path, body, size)
	if err != nil {
		logger.Error(r.Context(), "fs_layer_object_upload_failed", eventFields(r.Context(), "fs_layer_object_upload_failed", "path", prepared.Path, "error", err)...)
		writeBackendError(w, r, err)
		return
	}
	entry.LayerID = layer.LayerID
	entry.Path = prepared.Path
	entry.Op = prepared.Op
	entry.Kind = prepared.Kind
	entry.Mode = prepared.Mode
	entry.BaseRevision = prepared.BaseRevision
	entry.BaseInodeID = prepared.BaseInodeID
	if err := store.UpsertFSLayerEntry(r.Context(), entry); err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	stored, err := store.GetFSLayerEntry(r.Context(), layer.LayerID, entry.Path)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerEntryResponse(stored, false))
}

func (s *Server) handleFSLayerEvents(w http.ResponseWriter, r *http.Request, store *datastore.Store, layer *datastore.FSLayer) {
	since := parseFSLayerInt64Query(r, "since")
	events, err := store.ListFSLayerEvents(r.Context(), layer.LayerID, since, 1000)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"events": toFSLayerEventResponses(events)})
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
		LayerID:                layer.LayerID,
		Path:                   req.Path,
		Op:                     datastore.FSLayerEntryOp(strings.TrimSpace(req.Op)),
		Kind:                   datastore.FSLayerEntryKind(strings.TrimSpace(req.Kind)),
		BaseInodeID:            strings.TrimSpace(req.BaseInodeID),
		BaseRevision:           req.BaseRevision,
		StorageType:            strings.TrimSpace(req.StorageType),
		StorageRef:             strings.TrimSpace(req.StorageRef),
		StorageRefHash:         strings.TrimSpace(req.StorageRefHash),
		StorageEncryptionMode:  datastore.StorageEncryptionMode(strings.TrimSpace(req.StorageEncryptionMode)),
		StorageEncryptionKeyID: strings.TrimSpace(req.StorageEncryptionKeyID),
		ContentBlob:            req.Content,
		ContentType:            strings.TrimSpace(req.ContentType),
		ContentText:            req.ContentText,
		ChecksumSHA256:         strings.TrimSpace(req.ChecksumSHA256),
		SizeBytes:              req.SizeBytes,
		Mode:                   req.Mode,
	}
	if err := normalizeAndValidateFSLayerServerEntry(layer, &entry); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if !authorizeFSLayerEntryMutation(w, r, &entry) {
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
		writeFSLayerStoreError(w, r, err)
		return
	}
	stored, err := store.GetFSLayerEntry(r.Context(), layer.LayerID, entry.Path)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerEntryResponse(stored, true))
}

func parseFSLayerInt64Query(r *http.Request, key string) int64 {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseFSLayerModeQuery(r *http.Request, key string) (uint32, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		n, err = strconv.ParseUint(raw, 10, 32)
	}
	if err != nil {
		return 0, false
	}
	return uint32(n) & 0o777, true
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
	if !authorizeFS(w, r, FSOpRead, path) {
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
		writeFSLayerStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(toFSLayerEntryResponse(entry, true))
}

func (s *Server) handleFSLayerCommit(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer) {
	if layer.State == datastore.FSLayerStateCommitted {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fsLayerCommitResponse{
			Status:  "committed",
			LayerID: layer.LayerID,
		})
		return
	}
	if layer.State != datastore.FSLayerStateActive && layer.State != datastore.FSLayerStateSealed {
		errJSON(w, http.StatusConflict, "fs layer is not active or sealed")
		return
	}
	if err := store.BeginFSLayerCommit(r.Context(), layer.LayerID); err != nil {
		writeFSLayerStoreError(w, r, err)
		return
	}
	entries, err := store.ListFSLayerEntryLog(r.Context(), layer.LayerID)
	if err != nil {
		_ = store.SetFSLayerStateIf(r.Context(), layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateConflicted)
		writeFSLayerStoreError(w, r, err)
		return
	}
	if conflicts := validateFSLayerCommitScope(layer, entries); len(conflicts) > 0 {
		_ = store.SetFSLayerStateIf(r.Context(), layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateConflicted)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(fsLayerCommitResponse{
			Status:    "conflicted",
			LayerID:   layer.LayerID,
			Conflicts: conflicts,
		})
		return
	}
	conflicts := preflightFSLayerCommit(r.Context(), b, entries, false)
	if len(conflicts) > 0 {
		_ = store.SetFSLayerStateIf(r.Context(), layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateConflicted)
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
		_ = store.SetFSLayerStateIf(r.Context(), layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateConflicted)
		logger.Error(r.Context(), "fs_layer_commit_validate_failed", eventFields(r.Context(), "fs_layer_commit_validate_failed", "layer_id", layer.LayerID, "error", err)...)
		writeBackendError(w, r, err)
		return
	}
	rollbackCtx, rollbackCancel := context.WithTimeout(context.WithoutCancel(r.Context()), fsLayerRollbackTimeout)
	defer rollbackCancel()
	touchedPaths := make(map[string]struct{})
	for i := range entries {
		modeMayBeSuperseded := fsLayerEntryModeSuperseded(entries, i)
		if err := applyFSLayerEntry(r.Context(), b, &entries[i], false, touchedPaths, modeMayBeSuperseded); err != nil {
			rollbackErr := rollbackFSLayerCommit(rollbackCtx, b, filterFSLayerSnapshotsForEntries(snapshots, entries[:i]))
			_ = store.SetFSLayerStateIf(rollbackCtx, layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateConflicted)
			if rollbackErr != nil {
				logger.Error(r.Context(), "fs_layer_commit_apply_failed", eventFields(r.Context(), "fs_layer_commit_apply_failed", "path", entries[i].Path, "error", err, "rollback_error", rollbackErr)...)
				writeBackendError(w, r, err)
				return
			}
			logger.Error(r.Context(), "fs_layer_commit_apply_failed", eventFields(r.Context(), "fs_layer_commit_apply_failed", "path", entries[i].Path, "error", err)...)
			writeBackendError(w, r, err)
			return
		}
		markFSLayerEntryTouched(touchedPaths, &entries[i])
	}
	if err := store.SetFSLayerStateIf(r.Context(), layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateCommitted); err != nil {
		rollbackErr := rollbackFSLayerCommit(rollbackCtx, b, snapshots)
		_ = store.SetFSLayerStateIf(rollbackCtx, layer.LayerID, []datastore.FSLayerState{datastore.FSLayerStateCommitting}, datastore.FSLayerStateConflicted)
		if rollbackErr != nil {
			logger.Error(r.Context(), "fs_layer_commit_finalize_failed", eventFields(r.Context(), "fs_layer_commit_finalize_failed", "layer_id", layer.LayerID, "error", err, "rollback_error", rollbackErr)...)
			writeBackendError(w, r, err)
			return
		}
		logger.Error(r.Context(), "fs_layer_commit_finalize_failed", eventFields(r.Context(), "fs_layer_commit_finalize_failed", "layer_id", layer.LayerID, "error", err)...)
		writeBackendError(w, r, err)
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
		writeFSLayerStoreError(w, r, err)
		return
	}
	stored, err := store.GetFSLayerCheckpoint(r.Context(), checkpointID)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
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

func fsLayerReplayLogFromRequest(r *http.Request) bool {
	raw := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("replay")))
	if raw == "1" || raw == "true" || raw == "yes" {
		return true
	}
	mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
	return mode == "log" || mode == "replay"
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
		target := strings.TrimSpace(entry.ContentText)
		if target == "" && len(entry.ContentBlob) > 0 {
			target = strings.TrimSpace(string(entry.ContentBlob))
		}
		targetPath, err := canonicalFSLayerServerPath(target, "", string(entry.Kind))
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
	baseRoot = strings.TrimRight(baseRoot, "/")
	if path == baseRoot {
		return true
	}
	return strings.HasPrefix(path, baseRoot+"/")
}

func validateFSLayerCommitScope(layer *datastore.FSLayer, entries []datastore.FSLayerEntry) []fsLayerCommitConflict {
	var conflicts []fsLayerCommitConflict
	for i := range entries {
		if !fsLayerPathWithinBase(entries[i].Path, layer.BaseRootPath) {
			conflicts = append(conflicts, fsLayerCommitConflict{Path: entries[i].Path, Reason: "entry outside base root"})
		}
		if entries[i].Op == datastore.FSLayerEntryOpRename {
			target := strings.TrimSpace(entries[i].ContentText)
			if target == "" && len(entries[i].ContentBlob) > 0 {
				target = strings.TrimSpace(string(entries[i].ContentBlob))
			}
			targetPath, err := canonicalFSLayerServerPath(target, "", string(entries[i].Kind))
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
	var addDirTreePaths func(string)
	addDirTreePaths = func(dir string) {
		children, err := b.ReadDirCtx(ctx, dir)
		if err != nil {
			return
		}
		if !strings.HasSuffix(dir, "/") {
			dir += "/"
		}
		for _, child := range children {
			childPath := dir + child.Name
			if child.IsDir {
				childPath += "/"
			}
			addTrackedPath(childPath, false)
			if child.IsDir {
				addDirTreePaths(childPath)
			}
		}
	}
	for i := range entries {
		addParentCleanupPaths(entries[i].Path)
		addTrackedPath(entries[i].Path, false)
		if entries[i].Op == datastore.FSLayerEntryOpRename {
			if entries[i].Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entries[i].Path, "/") {
				addDirTreePaths(entries[i].Path)
			}
			target := strings.TrimSpace(entries[i].ContentText)
			if target == "" && len(entries[i].ContentBlob) > 0 {
				target = strings.TrimSpace(string(entries[i].ContentBlob))
			}
			if targetPath, err := canonicalFSLayerServerPath(target, "", string(entries[i].Kind)); err == nil {
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

func filterFSLayerSnapshotsForEntries(snapshots []fsLayerBaseSnapshot, entries []datastore.FSLayerEntry) []fsLayerBaseSnapshot {
	if len(snapshots) == 0 || len(entries) == 0 {
		return nil
	}
	needed := make(map[string]struct{})
	addPath := func(p string) {
		if p != "" {
			needed[p] = struct{}{}
		}
	}
	addParentCleanupPaths := func(p string) {
		for parent := pathutil.ParentPath(p); parent != "/" && parent != ""; parent = pathutil.ParentPath(parent) {
			addPath(parent)
		}
	}
	addDirTreePaths := func(dir string) {
		for i := range snapshots {
			if snapshots[i].Path == "" || snapshots[i].Path == dir {
				continue
			}
			if fsLayerPathDescendsFrom(snapshots[i].Path, dir) {
				addPath(snapshots[i].Path)
			}
		}
	}
	for i := range entries {
		entry := &entries[i]
		addParentCleanupPaths(entry.Path)
		addPath(entry.Path)
		if entry.Op != datastore.FSLayerEntryOpRename {
			continue
		}
		if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/") {
			addDirTreePaths(entry.Path)
		}
		target := strings.TrimSpace(entry.ContentText)
		if target == "" && len(entry.ContentBlob) > 0 {
			target = strings.TrimSpace(string(entry.ContentBlob))
		}
		if targetPath, err := canonicalFSLayerServerPath(target, "", string(entry.Kind)); err == nil {
			addParentCleanupPaths(targetPath)
			addPath(targetPath)
		}
	}
	out := make([]fsLayerBaseSnapshot, 0, len(snapshots))
	for i := range snapshots {
		if _, ok := needed[snapshots[i].Path]; ok {
			out = append(out, snapshots[i])
		}
	}
	return out
}

func fsLayerPathDescendsFrom(path, dir string) bool {
	if dir == "" || dir == "/" {
		return strings.HasPrefix(path, "/")
	}
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	return strings.HasPrefix(path, dir)
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
	if snap.IsDir {
		if nf, err := b.StatNodeCtx(ctx, snap.Path); err == nil && nf.Node.IsDirectory {
			if snap.ModeIsSet {
				return b.ChmodCtx(ctx, snap.Path, snap.Mode&0o777)
			}
			return nil
		} else if err != nil && !errors.Is(err, datastore.ErrNotFound) {
			return err
		}
		if err := removeFSLayerBasePathIfExists(ctx, b, snap.Path); err != nil {
			return err
		}
		mode := snap.Mode & 0o777
		if mode == 0 {
			mode = 0o755
		}
		return b.MkdirCtx(ctx, snap.Path, mode)
	}
	if err := removeFSLayerBasePathIfExists(ctx, b, snap.Path); err != nil {
		return err
	}
	if !snap.Exists {
		return nil
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
		return b.RemoveDirCtx(ctx, path)
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

func preflightFSLayerCommit(ctx context.Context, b *backendpkg.Dat9Backend, entries []datastore.FSLayerEntry, recoveringCommit bool) []fsLayerCommitConflict {
	var conflicts []fsLayerCommitConflict
	for i := range entries {
		entry := &entries[i]
		if recoveringCommit {
			applied, err := fsLayerEntryAlreadyApplied(ctx, b, entry)
			if err != nil {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
				continue
			}
			if applied {
				continue
			}
		}
		if entry.Op == datastore.FSLayerEntryOpRename {
			conflicts = append(conflicts, preflightFSLayerRename(ctx, b, entry)...)
		}
		if entry.Op == datastore.FSLayerEntryOpWhiteout && (entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/")) {
			children, err := b.ReadDirCtx(ctx, entry.Path)
			if err == nil && len(children) > 0 {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "directory whiteout requires empty directory"})
				continue
			}
			if err != nil && !errors.Is(err, datastore.ErrNotFound) {
				conflicts = append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
				continue
			}
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

func fsLayerEntryAlreadyApplied(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) (bool, error) {
	if entry == nil {
		return false, nil
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpUpsert:
		nf, err := b.StatNodeCtx(ctx, entry.Path)
		if errors.Is(err, datastore.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if nf.File == nil || nf.Node.IsDirectory {
			return false, nil
		}
		if nf.File.SizeBytes != entry.SizeBytes {
			return false, nil
		}
		if entry.ChecksumSHA256 != "" && nf.File.ChecksumSHA256 != entry.ChecksumSHA256 {
			return false, nil
		}
		if entry.Mode != 0 && nf.File.Mode != 0 && nf.File.Mode&0o777 != entry.Mode&0o777 {
			return false, nil
		}
		return true, nil
	case datastore.FSLayerEntryOpMkdir:
		nf, err := b.StatNodeCtx(ctx, entry.Path)
		if errors.Is(err, datastore.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return nf.Node.IsDirectory, nil
	case datastore.FSLayerEntryOpWhiteout:
		_, err := b.StatNodeCtx(ctx, entry.Path)
		if errors.Is(err, datastore.ErrNotFound) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	case datastore.FSLayerEntryOpChmod:
		if entry.Mode == 0 {
			return true, nil
		}
		nf, err := b.StatNodeCtx(ctx, entry.Path)
		if errors.Is(err, datastore.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		mode := nf.Mode
		if nf.File != nil && nf.File.Mode != 0 {
			mode = nf.File.Mode
		}
		return mode&0o777 == entry.Mode&0o777, nil
	case datastore.FSLayerEntryOpSymlink:
		nf, err := b.StatNodeCtx(ctx, entry.Path)
		if errors.Is(err, datastore.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if nf.File == nil || !isFSLayerSymlinkMode(nf.File.Mode) {
			return false, nil
		}
		target := entry.ContentText
		if target == "" && len(entry.ContentBlob) > 0 {
			target = string(entry.ContentBlob)
		}
		data, err := b.ReadCtx(ctx, entry.Path, 0, -1)
		if err != nil {
			return false, err
		}
		return string(data) == target, nil
	case datastore.FSLayerEntryOpRename:
		target := strings.TrimSpace(entry.ContentText)
		if target == "" && len(entry.ContentBlob) > 0 {
			target = strings.TrimSpace(string(entry.ContentBlob))
		}
		if target == "" {
			return false, nil
		}
		targetPath, err := canonicalFSLayerServerPath(target, "", "")
		if err != nil {
			return false, err
		}
		_, srcErr := b.StatNodeCtx(ctx, entry.Path)
		_, dstErr := b.StatNodeCtx(ctx, targetPath)
		if errors.Is(srcErr, datastore.ErrNotFound) && dstErr == nil {
			return true, nil
		}
		if srcErr != nil && !errors.Is(srcErr, datastore.ErrNotFound) {
			return false, srcErr
		}
		if dstErr != nil && !errors.Is(dstErr, datastore.ErrNotFound) {
			return false, dstErr
		}
		return false, nil
	default:
		return false, nil
	}
}

func preflightFSLayerRename(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) []fsLayerCommitConflict {
	var conflicts []fsLayerCommitConflict
	source, err := b.StatNodeCtx(ctx, entry.Path)
	if errors.Is(err, datastore.ErrNotFound) {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "base path missing"})
	}
	if err != nil {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: err.Error()})
	}
	target := strings.TrimSpace(entry.ContentText)
	if target == "" && len(entry.ContentBlob) > 0 {
		target = strings.TrimSpace(string(entry.ContentBlob))
	}
	if target == "" {
		return append(conflicts, fsLayerCommitConflict{Path: entry.Path, Reason: "rename target is required"})
	}
	targetKind := string(entry.Kind)
	if source.Node.IsDirectory {
		targetKind = string(datastore.FSLayerEntryKindDir)
	}
	targetPath, err := canonicalFSLayerServerPath(target, "", targetKind)
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

func ensureFSLayerEntryBaseStillMatches(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry, touched map[string]struct{}) error {
	if entry == nil || b == nil {
		return nil
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpWhiteout, datastore.FSLayerEntryOpChmod, datastore.FSLayerEntryOpRename:
	default:
		return nil
	}
	if _, ok := touched[entry.Path]; ok {
		return nil
	}
	if entry.BaseRevision == 0 && entry.BaseInodeID == "" {
		return nil
	}
	nf, err := b.StatNodeCtx(ctx, entry.Path)
	if errors.Is(err, datastore.ErrNotFound) {
		return fmt.Errorf("base path missing")
	}
	if err != nil {
		return err
	}
	if entry.BaseInodeID != "" {
		got := nf.Node.InodeID
		if nf.File != nil && nf.File.FileID != "" {
			got = nf.File.FileID
		}
		if got != entry.BaseInodeID {
			return fmt.Errorf("base inode changed")
		}
	}
	if entry.BaseRevision > 0 && nf.File != nil && nf.File.Revision != entry.BaseRevision {
		return fmt.Errorf("base revision changed")
	}
	return nil
}

func markFSLayerEntryTouched(touched map[string]struct{}, entry *datastore.FSLayerEntry) {
	if touched == nil || entry == nil {
		return
	}
	if entry.Path != "" {
		touched[entry.Path] = struct{}{}
	}
	if entry.Op != datastore.FSLayerEntryOpRename {
		return
	}
	target := strings.TrimSpace(entry.ContentText)
	if target == "" && len(entry.ContentBlob) > 0 {
		target = strings.TrimSpace(string(entry.ContentBlob))
	}
	if target != "" {
		touched[target] = struct{}{}
	}
}

func fsLayerPathTouched(touched map[string]struct{}, path string) bool {
	if touched == nil || path == "" {
		return false
	}
	_, ok := touched[path]
	return ok
}

func fsLayerApplyExpectedRevision(entry *datastore.FSLayerEntry, touched map[string]struct{}) int64 {
	if entry == nil {
		return -1
	}
	if fsLayerPathTouched(touched, entry.Path) {
		return -1
	}
	return entry.BaseRevision
}

func fsLayerEntryModeSuperseded(entries []datastore.FSLayerEntry, idx int) bool {
	if idx < 0 || idx >= len(entries) {
		return false
	}
	path := entries[idx].Path
	if path == "" {
		return false
	}
	for i := idx + 1; i < len(entries); i++ {
		next := &entries[i]
		if next.Path == path {
			return true
		}
		if next.Op == datastore.FSLayerEntryOpWhiteout && (next.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(next.Path, "/")) && fsLayerPathDescendsFrom(path, next.Path) {
			return true
		}
		if next.Op == datastore.FSLayerEntryOpRename && (next.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(next.Path, "/")) && fsLayerPathDescendsFrom(path, next.Path) {
			return true
		}
	}
	return false
}

func applyFSLayerEntry(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry, recoveringCommit bool, touched map[string]struct{}, modeMayBeSuperseded bool) error {
	if recoveringCommit {
		applied, err := fsLayerEntryAlreadyApplied(ctx, b, entry)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	if err := ensureFSLayerEntryBaseStillMatches(ctx, b, entry, touched); err != nil {
		return err
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpUpsert:
		expectedRevision := fsLayerApplyExpectedRevision(entry, touched)
		if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/") {
			mode := entry.Mode
			if mode == 0 {
				mode = 0o755
			}
			err := b.MkdirCtx(ctx, entry.Path, mode&0o777)
			if errors.Is(err, datastore.ErrPathConflict) {
				nf, statErr := b.StatNodeCtx(ctx, entry.Path)
				if statErr != nil {
					return statErr
				}
				if !nf.Node.IsDirectory {
					return err
				}
				if entry.Mode != 0 {
					return applyFSLayerEntryMode(ctx, b, entry.Path, entry.Mode&0o777, modeMayBeSuperseded)
				}
				return nil
			}
			return err
		}
		if entry.StorageRef != "" || entry.StorageType == string(datastore.StorageS3) {
			writeEntry := *entry
			writeEntry.BaseRevision = expectedRevision
			if _, err := b.WriteStoredObjectCtxIfRevision(ctx, entry.Path, &writeEntry); err != nil {
				return err
			}
		} else {
			_, _, err := b.WriteCtxIfRevisionWithTagsResult(
				ctx,
				entry.Path,
				entry.ContentBlob,
				0,
				filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate,
				expectedRevision,
				nil,
				"",
			)
			if err != nil {
				return err
			}
		}
		if entry.Mode != 0 {
			return applyFSLayerEntryMode(ctx, b, entry.Path, entry.Mode&0o777, modeMayBeSuperseded)
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
			children, readErr := b.ReadDirCtx(ctx, entry.Path)
			if errors.Is(readErr, datastore.ErrNotFound) {
				return nil
			}
			if readErr != nil {
				return readErr
			}
			if len(children) > 0 {
				return fmt.Errorf("directory whiteout requires empty directory")
			}
			err = b.RemoveDirCtx(ctx, entry.Path)
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
		return applyFSLayerEntryMode(ctx, b, entry.Path, mode&0o777, modeMayBeSuperseded)
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
		if entry.Kind == datastore.FSLayerEntryKindDir || strings.HasSuffix(entry.Path, "/") {
			return b.RenameCtx(ctx, entry.Path, target)
		}
		return b.RenameFileNoReplaceCtx(ctx, entry.Path, target)
	default:
		return fmt.Errorf("unsupported fs layer op %q", entry.Op)
	}
}

func applyFSLayerEntryMode(ctx context.Context, b *backendpkg.Dat9Backend, path string, mode uint32, allowMissing bool) error {
	if mode == 0 {
		return nil
	}
	if err := b.ChmodCtx(ctx, path, mode); err != nil {
		if allowMissing && errors.Is(err, datastore.ErrNotFound) {
			return nil
		}
		if !errors.Is(err, datastore.ErrNotFound) {
			return fmt.Errorf("chmod fs layer path %s: %w", path, err)
		}
		nf, statErr := b.StatNodeCtx(ctx, path)
		if statErr == nil && nf.File != nil && nf.Node.InodeID == "" {
			return nil
		}
		if statErr != nil && !errors.Is(statErr, datastore.ErrNotFound) {
			return fmt.Errorf("stat fs layer chmod path %s: %w", path, statErr)
		}
		return fmt.Errorf("chmod fs layer path %s: %w", path, err)
	}
	return nil
}

func writeFSLayerStoreError(w http.ResponseWriter, r *http.Request, err error) {
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
	logger.Error(r.Context(), "fs_layer_store_error", eventFields(r.Context(), "fs_layer_store_error", "error", err)...)
	writeBackendError(w, r, err)
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
		LayerID:                entry.LayerID,
		Path:                   entry.Path,
		ParentPath:             entry.ParentPath,
		Name:                   entry.Name,
		Op:                     string(entry.Op),
		Kind:                   string(entry.Kind),
		BaseInodeID:            entry.BaseInodeID,
		BaseRevision:           entry.BaseRevision,
		StorageType:            entry.StorageType,
		StorageRef:             entry.StorageRef,
		StorageRefHash:         entry.StorageRefHash,
		StorageEncryptionMode:  string(entry.StorageEncryptionMode),
		StorageEncryptionKeyID: entry.StorageEncryptionKeyID,
		ChecksumSHA256:         entry.ChecksumSHA256,
		SizeBytes:              entry.SizeBytes,
		Mode:                   entry.Mode,
		ContentText:            entry.ContentText,
		EntrySeq:               entry.EntrySeq,
		CreatedAt:              entry.CreatedAt,
		UpdatedAt:              entry.UpdatedAt,
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

func toFSLayerEventResponses(events []datastore.FSLayerEvent) []fsLayerEventResponse {
	out := make([]fsLayerEventResponse, 0, len(events))
	for i := range events {
		out = append(out, fsLayerEventResponse{
			EventID:   events[i].EventID,
			LayerID:   events[i].LayerID,
			Seq:       events[i].Seq,
			ActorID:   events[i].ActorID,
			Op:        events[i].Op,
			Path:      events[i].Path,
			CreatedAt: events[i].CreatedAt,
		})
	}
	return out
}
