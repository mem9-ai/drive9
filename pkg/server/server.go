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
)

type Server struct {
	backend *backend.Dat9Backend
	mux     *http.ServeMux
}

func New(b *backend.Dat9Backend) *Server {
	s := &Server{backend: b}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/fs/", s.handleFS)
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

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ListenAndServe starts the server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	log.Printf("dat9 server listening on %s", addr)
	return http.ListenAndServe(addr, s)
}
