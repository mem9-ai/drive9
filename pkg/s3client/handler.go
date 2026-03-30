package s3client

import (
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

	etag, err := c.UploadPart(r.Context(), uploadID, partNumber, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GET /objects/{key...}
// Supports ?range=START-END query parameter for byte-range reads
// (used by PresignGetObjectRange in the local mock).
func (c *LocalS3Client) handleGetObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/objects/")
	rc, err := c.GetObject(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", "application/octet-stream")

	// Check for range query parameter (e.g. ?range=0-8388607)
	if rangeParam := r.URL.Query().Get("range"); rangeParam != "" {
		parts := strings.SplitN(rangeParam, "-", 2)
		if len(parts) == 2 {
			startByte, err1 := strconv.ParseInt(parts[0], 10, 64)
			endByte, err2 := strconv.ParseInt(parts[1], 10, 64)
			if err1 == nil && err2 == nil && startByte >= 0 && endByte >= startByte {
				// Read the full object then slice — simple and correct for local mock.
				data, err := io.ReadAll(rc)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if startByte >= int64(len(data)) {
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				if endByte >= int64(len(data)) {
					endByte = int64(len(data)) - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startByte, endByte, len(data)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[startByte : endByte+1])
				return
			}
		}
	}

	_, _ = io.Copy(w, rc)
}
