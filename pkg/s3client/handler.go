package s3client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Handler returns an http.Handler that serves the local S3 presigned URLs.
// Mount this at the baseURL path prefix (e.g. "/s3").
func (c *LocalS3Client) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/upload/", c.handleUploadPart)
	mux.HandleFunc("/objects/", c.handleGetObject)
	return mux
}

// handleUploadPart handles PUT /upload/{uploadID}/{partNumber}
func (c *LocalS3Client) handleUploadPart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse: /upload/{uploadID}/{partNumber}
	rest := strings.TrimPrefix(r.URL.Path, "/upload/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	uploadID := parts[0]
	partNumber, err := strconv.Atoi(parts[1])
	if err != nil {
		http.Error(w, "invalid part number", http.StatusBadRequest)
		return
	}

	etag, err := c.UploadPart(context.Background(), uploadID, partNumber, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GET /objects/{key...}
func (c *LocalS3Client) handleGetObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/objects/")
	rc, err := c.GetObject(context.Background(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	io.Copy(w, rc)
}
