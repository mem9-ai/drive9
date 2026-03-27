// Package server implements the dat9 HTTP server.
// All file operations go through /v1/fs/{path}.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

type Server struct {
	backend *backend.Dat9Backend
	mux     *http.ServeMux
}

func New(b *backend.Dat9Backend) *Server {
	s := &Server{backend: b}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/fs/", s.handleFS)
	mux.HandleFunc("/v1/uploads", s.handleUploads)
	mux.HandleFunc("/v1/uploads/", s.handleUploadAction)
	mux.HandleFunc("/v1/sql", s.handleSQL)

	// Register local S3 handler for presigned URL support in dev/test mode
	if local, ok := b.S3().(*s3client.LocalS3Client); ok {
		mux.Handle("/s3/", http.StripPrefix("/s3", local.Handler()))
	}

	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleFS(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if path == "" {
		path = "/"
	}

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("list") {
			s.handleList(w, r, path)
		} else {
			s.handleRead(w, r, path)
		}
	case http.MethodPut:
		s.handleWrite(w, r, path)
	case http.MethodHead:
		s.handleStat(w, r, path)
	case http.MethodDelete:
		s.handleDelete(w, r, path)
	case http.MethodPost:
		if r.URL.Query().Has("copy") {
			s.handleCopy(w, r, path)
		} else if r.URL.Query().Has("rename") {
			s.handleRename(w, r, path)
		} else if r.URL.Query().Has("mkdir") {
			s.handleMkdir(w, r, path)
		} else {
			errJSON(w, http.StatusBadRequest, "unknown POST action")
		}
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, path string) {
	// Check if this is an S3-stored file — redirect to presigned URL
	if s.backend.S3() != nil {
		url, err := s.backend.PresignGetObject(r.Context(), path)
		if err == nil {
			http.Redirect(w, r, url, http.StatusFound)
			return
		}
		// Not an S3 file or error — fall through to local read
	}

	data, err := s.backend.Read(path, 0, -1)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request, path string) {
	entries, err := s.backend.ReadDir(path)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	type entry struct {
		Name  string `json:"name"`
		Size  int64  `json:"size"`
		IsDir bool   `json:"isDir"`
	}
	result := struct {
		Entries []entry `json:"entries"`
	}{Entries: make([]entry, 0, len(entries))}

	for _, e := range entries {
		result.Entries = append(result.Entries, entry{
			Name: e.Name, Size: e.Size, IsDir: e.IsDir,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, path string) {
	// Bifurcate by size. Prefer X-Dat9-Content-Length because Go's net/http
	// normalizes Content-Length to 0 when the request body is http.NoBody.
	cl := r.ContentLength
	if h := r.Header.Get("X-Dat9-Content-Length"); h != "" {
		cl, _ = strconv.ParseInt(h, 10, 64)
	}
	if cl > 0 && s.backend.IsLargeFile(cl) {
		plan, err := s.backend.InitiateUpload(r.Context(), path, cl)
		if err != nil {
			if errors.Is(err, meta.ErrUploadConflict) {
				errJSON(w, http.StatusConflict, err.Error())
				return
			}
			errJSON(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(plan)
		return
	}

	// Small file: proxy through server
	data, err := io.ReadAll(r.Body)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}

	_, err = s.backend.Write(path, data, 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request, path string) {
	// Single call to store.Stat to get both FileInfo and revision
	nf, err := s.backend.Store().Stat(path)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var size int64
	if nf.File != nil {
		size = nf.File.SizeBytes
	}
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("X-Dat9-IsDir", fmt.Sprintf("%v", nf.Node.IsDirectory))

	if nf.File != nil {
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(nf.File.Revision, 10))
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, path string) {
	recursive := r.URL.Query().Has("recursive")

	var err error
	if recursive {
		err = s.backend.RemoveAll(path)
	} else {
		err = s.backend.Remove(path)
	}
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCopy(w http.ResponseWriter, r *http.Request, dstPath string) {
	srcPath := r.Header.Get("X-Dat9-Copy-Source")
	if srcPath == "" {
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Copy-Source header")
		return
	}

	if err := s.backend.CopyFile(srcPath, dstPath); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request, newPath string) {
	oldPath := r.Header.Get("X-Dat9-Rename-Source")
	if oldPath == "" {
		errJSON(w, http.StatusBadRequest, "missing X-Dat9-Rename-Source header")
		return
	}

	if err := s.backend.Rename(oldPath, newPath); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request, path string) {
	if err := s.backend.Mkdir(path, 0o755); err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleUploads handles GET /v1/uploads?path=...&status=...
func (s *Server) handleUploads(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		errJSON(w, http.StatusBadRequest, "missing path parameter")
		return
	}
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "UPLOADING"
	}

	uploads, err := s.backend.ListUploads(path, meta.UploadStatus(status))
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	type uploadEntry struct {
		UploadID   string `json:"upload_id"`
		Path       string `json:"path"`
		TotalSize  int64  `json:"total_size"`
		PartsTotal int    `json:"parts_total"`
		Status     string `json:"status"`
		CreatedAt  string `json:"created_at"`
		ExpiresAt  string `json:"expires_at"`
	}
	result := make([]uploadEntry, 0, len(uploads))
	for _, u := range uploads {
		result = append(result, uploadEntry{
			UploadID:   u.UploadID,
			Path:       u.TargetPath,
			TotalSize:  u.TotalSize,
			PartsTotal: u.PartsTotal,
			Status:     string(u.Status),
			CreatedAt:  u.CreatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
			ExpiresAt:  u.ExpiresAt.Format("2006-01-02T15:04:05.000Z07:00"),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"uploads": result})
}

// handleUploadAction handles /v1/uploads/{id}/complete, /v1/uploads/{id}/resume, DELETE /v1/uploads/{id}
func (s *Server) handleUploadAction(w http.ResponseWriter, r *http.Request) {
	// Parse: /v1/uploads/{id} or /v1/uploads/{id}/complete or /v1/uploads/{id}/resume
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	parts := strings.SplitN(rest, "/", 2)
	uploadID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	if uploadID == "" {
		errJSON(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	switch {
	case r.Method == http.MethodPost && action == "complete":
		s.handleUploadComplete(w, r, uploadID)
	case r.Method == http.MethodPost && action == "resume":
		s.handleUploadResume(w, r, uploadID)
	case r.Method == http.MethodDelete && action == "":
		s.handleUploadAbort(w, r, uploadID)
	default:
		errJSON(w, http.StatusBadRequest, "unknown upload action")
	}
}

func (s *Server) handleUploadComplete(w http.ResponseWriter, r *http.Request, uploadID string) {
	if err := s.backend.ConfirmUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, meta.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, meta.ErrPathConflict) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleUploadResume(w http.ResponseWriter, r *http.Request, uploadID string) {
	plan, err := s.backend.ResumeUpload(r.Context(), uploadID)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, meta.ErrUploadExpired) {
			errJSON(w, http.StatusGone, err.Error())
			return
		}
		if errors.Is(err, meta.ErrUploadNotActive) {
			errJSON(w, http.StatusConflict, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleUploadAbort(w http.ResponseWriter, r *http.Request, uploadID string) {
	if err := s.backend.AbortUpload(r.Context(), uploadID); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Query == "" {
		errJSON(w, http.StatusBadRequest, "empty query")
		return
	}

	rows, err := s.backend.ExecSQL(r.Context(), req.Query)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rows)
}

// ListenAndServe starts the server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	log.Printf("dat9 server listening on %s", addr)
	return http.ListenAndServe(addr, s)
}
