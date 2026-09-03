package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/tagutil"
)

// maxSetMetaBodyBytes caps the setmeta request body. The payload is a small
// JSON object ({tags, description}); even a large tag map with maximum-length
// keys/values and a maximum-length description stays well under this limit.
const maxSetMetaBodyBytes = 1 << 20

type setFileMetadataRequest struct {
	// Tags replaces the tag set when present (null/absent leaves tags
	// unchanged; an empty object clears all tags).
	Tags map[string]string `json:"tags"`
	// Description sets the description when present (null/absent leaves it
	// unchanged; an empty string clears it).
	Description *string `json:"description"`
}

func (s *Server) handleSetMeta(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "setmeta_missing_scope", "path", path)...)
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req setFileMetadataRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSetMetaBodyBytes)).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "setmeta_body_too_large", "path", path, "max", maxSetMetaBodyBytes)...)
			errJSON(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "setmeta_bad_body", "path", path, "error", err)...)
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Tags == nil && req.Description == nil {
		errJSON(w, http.StatusBadRequest, "nothing to update: provide tags and/or description")
		return
	}
	if req.Tags != nil {
		if err := tagutil.ValidateMap(req.Tags); err != nil {
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "setmeta_invalid_tags", "path", path, "error", err)...)
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.Description != nil && utf8.RuneCountInString(*req.Description) > backend.MaxDescriptionLen {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("description exceeds %d characters", backend.MaxDescriptionLen))
		return
	}
	revision, err := b.SetFileMetadataCtx(r.Context(), path, backend.FileMetadataUpdate{Tags: req.Tags, Description: req.Description})
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			// The dentry was delete+recreated between path resolution and the
			// update transaction; the client can retry against the new file.
			logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "setmeta_revision_conflict", "path", path, "detail", err.Error())...)
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, backend.ErrSetMetadataOnDirectory) {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		if errJSONInvalidRootDentry(w, err) {
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "setmeta_failed", "path", path, "error", err)...)
		writeBackendError(w, r, err)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "setmeta_ok", "path", path)...)
	s.publishEvent(r, path, "setmeta")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": revision})
}
