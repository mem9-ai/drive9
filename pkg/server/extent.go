package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
)

const (
	headerContentLayout   = "X-Dat9-Content-Layout"
	headerSliceGeneration = "X-Dat9-Slice-Generation"
	headerStorageClass    = "X-Dat9-Storage-Class"
)

func parseContentLayoutHeader(h http.Header) datastore.ContentLayout {
	raw := strings.TrimSpace(h.Get(headerContentLayout))
	if raw == "" {
		return ""
	}
	layout, err := datastore.ParseContentLayout(raw)
	if err != nil {
		return datastore.ContentLayout(strings.ToLower(raw))
	}
	return layout
}

func storageClassFor(f *datastore.File) string {
	if f != nil && f.IsExtent() {
		return "extent"
	}
	return "standard"
}

func (s *Server) handleExtentIngestWrite(w http.ResponseWriter, r *http.Request, b *backend.Dat9Backend, path string, writeCtx context.Context, size, expectedRevision int64, writeTags map[string]string) {
	if len(writeTags) > 0 || strings.TrimSpace(r.Header.Get("X-Dat9-Description")) != "" {
		logger.Warn(r.Context(), "server_event", eventFields(r.Context(), "write_extent_put_tag_unsupported", "path", path)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		errJSON(w, http.StatusBadRequest, backend.ErrExtentExtraMetadata.Error())
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.maxUploadBytes)
	reader := io.Reader(body)
	if size > 0 {
		reader = io.LimitReader(body, size)
	}
	written, committedRevision, _, err := b.IngestExtentReader(writeCtx, path, reader, size, expectedRevision)
	if err != nil {
		if errors.Is(err, backend.ErrUploadTooLarge) {
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if isBackendQuotaExceeded(err) {
			metricEvent(r.Context(), "fs_write", "result", "error")
			errJSON(w, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrRevisionConflict) {
			metricEvent(r.Context(), "fs_write", "result", "conflict")
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, datastore.ErrExtentUseCommit) ||
			errors.Is(err, datastore.ErrInvalidContentLayout) || errors.Is(err, datastore.ErrAppendLogUnsupported) ||
			errors.Is(err, datastore.ErrNotExtent) {
			metricEvent(r.Context(), "fs_write", "result", "error")
			s.writeExtentError(w, r, path, "write", err)
			return
		}
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), "write_failed", "path", path, "error", err)...)
		metricEvent(r.Context(), "fs_write", "result", "error")
		writeBackendError(w, r, err)
		return
	}
	logger.Info(r.Context(), "server_event", eventFields(r.Context(), "write_ok", "path", path, "bytes", written)...)
	metricEvent(r.Context(), "fs_write", "result", "ok")
	recordTenantFileBytes(r.Context(), "fs", "write", "write", written)
	s.publishEvent(r, path, "write")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "revision": committedRevision})
}

func (s *Server) handlePrepareBlocks(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req backend.PrepareBlocksRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	blocks, err := b.PrepareBlocks(r.Context(), path, req.Ranges)
	if err != nil {
		s.writeExtentError(w, r, path, "prepare_blocks", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"blocks": blocks})
}

func (s *Server) handleCommitSlices(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req backend.CommitSlicesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	result, err := b.CommitSlices(r.Context(), path, req)
	if err != nil {
		s.writeExtentError(w, r, path, "commit_slices", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(headerSliceGeneration, strconv.FormatInt(result.Generation, 10))
	out := map[string]any{
		"revision":   result.Revision,
		"generation": result.Generation,
		"size_bytes": result.SizeBytes,
		"replayed":   result.Replayed,
	}
	if len(result.ChunkRows) > 0 {
		out["chunk_rows"] = result.ChunkRows
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleReadPlan(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpRead, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	start, end := int64(0), int64(-1)
	if v := strings.TrimSpace(r.URL.Query().Get("off")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			errJSON(w, http.StatusBadRequest, "invalid off")
			return
		}
		start = n
	}
	if v := strings.TrimSpace(r.URL.Query().Get("len")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			errJSON(w, http.StatusBadRequest, "invalid len")
			return
		}
		end = start + n
	}
	if rng := strings.TrimSpace(r.Header.Get("Range")); rng != "" {
		meta, err := b.PlanExtentRead(r.Context(), path, 0, 0)
		if err != nil {
			s.writeExtentError(w, r, path, "read_plan", err)
			return
		}
		s0, e0, ok := parseBytesRange(rng, meta.SizeBytes)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.SizeBytes))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, end = s0, e0+1
	}
	plan, err := b.PlanExtentRead(r.Context(), path, start, end)
	if err != nil {
		s.writeExtentError(w, r, path, "read_plan", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(headerContentLayout, string(datastore.ContentLayoutExtent))
	w.Header().Set(headerSliceGeneration, strconv.FormatInt(plan.Generation, 10))
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleCompactSlices(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req backend.CompactSlicesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if err := b.CompactSlices(r.Context(), path, req); err != nil {
		s.writeExtentError(w, r, path, "compact_slices", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
}

func (s *Server) handleCloneRange(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req struct {
		Src                string `json:"src"`
		SrcOff             int64  `json:"src_off"`
		DstOff             int64  `json:"dst_off"`
		Length             int64  `json:"length"`
		ExpectedRevision   int64  `json:"expected_revision"`
		ExpectedGeneration int64  `json:"expected_generation"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Src == "" {
		errJSON(w, http.StatusBadRequest, "src is required")
		return
	}
	if !authorizeFS(w, r, FSOpRead, req.Src) {
		return
	}
	result, err := b.CopyFileRange(r.Context(), req.Src, path, req.SrcOff, req.DstOff, req.Length, req.ExpectedRevision, req.ExpectedGeneration)
	if err != nil {
		s.writeExtentError(w, r, path, "clone_range", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(headerSliceGeneration, strconv.FormatInt(result.Generation, 10))
	out := map[string]any{
		"revision":   result.Revision,
		"generation": result.Generation,
		"size_bytes": result.SizeBytes,
		"replayed":   result.Replayed,
	}
	if len(result.ChunkRows) > 0 {
		out["chunk_rows"] = result.ChunkRows
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handlePresignPut(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req struct {
		BlockKey       string `json:"block_key"`
		Len            int64  `json:"len"`
		ChecksumSHA256 string `json:"checksum_sha256"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	blk, err := b.PresignPutBlock(r.Context(), path, req.BlockKey, req.Len, req.ChecksumSHA256)
	if err != nil {
		s.writeExtentError(w, r, path, "presign_put", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(blk)
}

func (s *Server) handleGetSlices(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpRead, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	rows, rev, gen, err := b.GetSlices(r.Context(), path)
	if err != nil {
		s.writeExtentError(w, r, path, "get_slices", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(headerSliceGeneration, strconv.FormatInt(gen, 10))
	_ = json.NewEncoder(w).Encode(map[string]any{
		"revision":   rev,
		"generation": gen,
		"slices":     rows,
	})
}

func (s *Server) handleSetattr(w http.ResponseWriter, r *http.Request, path string) {
	if !authorizeFS(w, r, FSOpWrite, path) {
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req struct {
		ContentLayout string `json:"content_layout"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	layout, err := datastore.ParseContentLayout(req.ContentLayout)
	if err != nil {
		s.writeExtentError(w, r, path, "setattr", err)
		return
	}
	if err := b.SetContentLayout(r.Context(), path, layout); err != nil {
		s.writeExtentError(w, r, path, "setattr", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "content_layout": layout})
}

func (s *Server) assembleExtentRead(w http.ResponseWriter, r *http.Request, b *backend.Dat9Backend, plan *backend.ReadPlan) {
	start, end := int64(0), plan.Size
	status := http.StatusOK
	if rng := strings.TrimSpace(r.Header.Get("Range")); rng != "" {
		s0, e0, ok := parseBytesRange(rng, plan.Size)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", plan.Size))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, end = s0, e0+1
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s0, e0, plan.Size))
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	if err := b.AssembleExtent(r.Context(), plan.InodeID, start, end, w); err != nil {
		logger.Error(r.Context(), "extent_assemble_failed", zap.Error(err))
	}
}

func parseBytesRange(header string, size int64) (start, end int64, ok bool) {
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(strings.ToLower(header), "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimSpace(header[len("bytes="):])
	if i := strings.Index(spec, ","); i >= 0 {
		spec = spec[:i]
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if parts[1] == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, false
		}
	}
	if start >= size {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

func (s *Server) writeExtentError(w http.ResponseWriter, r *http.Request, path, op string, err error) {
	switch {
	case errors.Is(err, datastore.ErrNotFound):
		errJSON(w, http.StatusNotFound, err.Error())
	case errors.Is(err, datastore.ErrCompactAborted):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "code": "compact_conflict"})
	case errors.Is(err, datastore.ErrRevisionConflict), errors.Is(err, datastore.ErrGenerationConflict):
		nf, _ := backendFromRequest(r).Store().Stat(r.Context(), path)
		extra := map[string]any{"error": err.Error(), "code": "cas_conflict"}
		if nf != nil && nf.File != nil {
			extra["revision"] = nf.File.Revision
			extra["generation"] = nf.File.SliceGeneration
			extra["size_bytes"] = nf.File.SizeBytes
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(extra)
	case errors.Is(err, datastore.ErrBlockNotLanded):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "code": "block_not_landed"})
	case errors.Is(err, datastore.ErrPendingExpired):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "code": "pending_expired"})
	case errors.Is(err, datastore.ErrNotExtent),
		errors.Is(err, datastore.ErrExtentCoverageHole),
		errors.Is(err, datastore.ErrExtentUseCommit),
		errors.Is(err, backend.ErrExtentExtraMetadata),
		errors.Is(err, datastore.ErrInvalidContentLayout),
		errors.Is(err, datastore.ErrAppendLogUnsupported):
		errJSON(w, http.StatusBadRequest, err.Error())
	default:
		logger.Error(r.Context(), "server_event", eventFields(r.Context(), op+"_failed", "path", path, "error", err)...)
		writeBackendError(w, r, err)
	}
}
