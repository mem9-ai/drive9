package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"go.uber.org/zap"
)

// handleUploadStatus handles GET /uploads/{id}/status endpoint
// Returns list of already-uploaded parts with their ETags/checksums
func (s *Server) handleUploadStatus(w http.ResponseWriter, r *http.Request, uploadID string) {
	ctx := r.Context()
	start := time.Now()

	// Get backend from request context
	b := backendFromRequest(r)
	if b == nil {
		logger.Warn(ctx, "upload_status_missing_scope")
		http.Error(w, "missing tenant scope", http.StatusUnauthorized)
		return
	}

	// Get upload metadata
	upload, err := b.GetUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(ctx, "upload_status_not_found", zap.String("upload_id", uploadID))
			http.Error(w, "upload not found", http.StatusNotFound)
			return
		}
		logger.Error(ctx, "upload_status_get_failed", zap.String("upload_id", uploadID), zap.Error(err))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Check if upload is still active
	if upload.Status != datastore.UploadUploading {
		logger.Warn(ctx, "upload_status_not_active", zap.String("upload_id", uploadID), zap.String("status", upload.Status))
		http.Error(w, "upload not active", http.StatusConflict)
		return
	}

	// Check if upload has expired
	if upload.ExpiresAt.Before(time.Now()) {
		logger.Warn(ctx, "upload_status_expired", zap.String("upload_id", uploadID))
		http.Error(w, "upload expired", http.StatusGone)
		return
	}

	// List already-uploaded parts
	uploadedParts, err := b.s3.ListParts(ctx, upload.S3Key, upload.S3UploadID)
	if err != nil {
		logger.Error(ctx, "upload_status_list_parts_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("server", "upload_status", "error", time.Since(start))
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Convert to response format
	parts := make([]UploadPartStatus, 0, len(uploadedParts))
	for _, p := range uploadedParts {
		parts = append(parts, UploadPartStatus{
			PartNumber: p.Number,
			ETag:       p.ETag,
			Size:       p.Size,
		})
	}

	// Calculate total uploaded size
	var totalUploadedSize int64
	for _, p := range uploadedParts {
		totalUploadedSize += p.Size
	}

	// Prepare response
	response := UploadStatusResponse{
		UploadID:           uploadID,
		TotalSize:          upload.TotalSize,
		PartSize:           upload.PartSize,
		TotalParts:         s3client.CalcParts(upload.TotalSize, upload.PartSize),
		UploadedParts:      parts,
		TotalUploadedSize:  totalUploadedSize,
		UploadedPartsCount: len(parts),
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error(ctx, "upload_status_encode_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("server", "upload_status", "error", time.Since(start))
		return
	}

	logger.Info(ctx, "upload_status_ok", zap.String("upload_id", uploadID), zap.Int("uploaded_parts", len(parts)))
	metrics.RecordOperation("server", "upload_status", "ok", time.Since(start))
}

// UploadPartStatus represents a single uploaded part
type UploadPartStatus struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

// UploadStatusResponse is the response for GET /uploads/{id}/status
type UploadStatusResponse struct {
	UploadID           string             `json:"upload_id"`
	TotalSize          int64              `json:"total_size"`
	PartSize           int64              `json:"part_size"`
	TotalParts         []s3client.Part    `json:"total_parts"`
	UploadedParts      []UploadPartStatus `json:"uploaded_parts"`
	TotalUploadedSize  int64              `json:"total_uploaded_size"`
	UploadedPartsCount int                `json:"uploaded_parts_count"`
}