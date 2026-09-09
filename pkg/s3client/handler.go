package s3client

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
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
	key := strings.TrimPrefix(r.URL.Path, "/objects/")
	switch r.Method {
	case http.MethodPut:
		c.handlePutObject(w, r, key)
		return
	case http.MethodHead:
		head, err := c.HeadObject(r.Context(), key)
		if err != nil || head == nil || !head.Exists {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(head.Size, 10))
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodGet:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rc, err := c.GetObject(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer func() { _ = rc.Close() }()

	w.Header().Set("Content-Type", "application/octet-stream")

	// Parse range from ?range= query param or standard Range header.
	var startByte, endByte int64 = -1, -1
	if rangeParam := r.URL.Query().Get("range"); rangeParam != "" {
		parts := strings.SplitN(rangeParam, "-", 2)
		if len(parts) == 2 {
			s, e1 := strconv.ParseInt(parts[0], 10, 64)
			e, e2 := strconv.ParseInt(parts[1], 10, 64)
			if e1 == nil && e2 == nil && s >= 0 && e >= s {
				startByte, endByte = s, e
			}
		}
	} else if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
		// Parse "bytes=START-END"
		rangeHdr = strings.TrimPrefix(rangeHdr, "bytes=")
		parts := strings.SplitN(rangeHdr, "-", 2)
		if len(parts) == 2 {
			s, e1 := strconv.ParseInt(parts[0], 10, 64)
			e, e2 := strconv.ParseInt(parts[1], 10, 64)
			if e1 == nil && e2 == nil && s >= 0 && e >= s {
				startByte, endByte = s, e
			}
		}
	}

	if startByte >= 0 {
		seeker, ok := rc.(io.Seeker)
		if !ok {
			http.Error(w, "local object is not seekable", http.StatusInternalServerError)
			return
		}
		totalSize, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if startByte >= totalSize {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if endByte >= totalSize {
			endByte = totalSize - 1
		}
		if _, err := seeker.Seek(startByte, io.SeekStart); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startByte, endByte, totalSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.CopyN(w, rc, endByte-startByte+1)
		return
	}

	_, _ = io.Copy(w, rc)
}

func (c *LocalS3Client) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	if r.Header.Get("If-None-Match") == "*" {
		if _, err := os.Stat(c.objectPath(key)); err == nil {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if want := r.Header.Get("x-amz-checksum-sha256"); want != "" {
		sum := sha256.Sum256(body)
		gotB64 := base64.StdEncoding.EncodeToString(sum[:])
		gotHex := hex.EncodeToString(sum[:])
		if want != gotB64 && !strings.EqualFold(want, gotHex) {
			http.Error(w, "checksum mismatch", http.StatusBadRequest)
			return
		}
	}
	if err := c.PutObject(r.Context(), key, bytes.NewReader(body), int64(len(body)), EncryptionOpts{}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
