package mockserver

import (
	"fmt"
	"time"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// handleS3 is the embedded S3-compatible endpoint (spec §21 note): a
// directory-backed object store under <Dir>/objects serving the subset the
// extent data plane needs — PUT/GET/DELETE object, bucket creation
// (PUT bucket), ListObjectsV2 and health probing. SigV4 signatures are NOT
// verified (mock trust boundary = the process); when the simulator stack
// manages a rustfs instance it sets Config.S3Endpoint and this handler is
// not mounted at all.
func (s *Server) handleS3(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/s3/")
	parts := strings.SplitN(rest, "/", 2)
	bucket := parts[0]
	if bucket == "" {
		errJSON(w, http.StatusBadRequest, "missing bucket")
		return
	}
	if bucket == "health" && len(parts) == 1 {
		w.WriteHeader(http.StatusOK)
		return
	}
	root := filepath.Join(s.cfg.Dir, "objects", bucket)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	switch {
	case key == "" && r.Method == http.MethodPut: // create bucket
		if err := os.MkdirAll(root, 0o755); err != nil {
			errJSON(w, http.StatusInternalServerError, "create bucket")
			return
		}
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodGet: // list-objects-v2
		s.s3List(w, r, root)
	case r.Method == http.MethodPut:
		if err := os.MkdirAll(filepath.Dir(s3Path(root, key)), 0o755); err != nil {
			errJSON(w, http.StatusInternalServerError, "mkdir")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "read body")
			return
		}
		if err := os.WriteFile(s3Path(root, key), body, 0o644); err != nil {
			errJSON(w, http.StatusInternalServerError, "write object")
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, len(body)))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet:
		data, err := os.ReadFile(s3Path(root, key))
		if err != nil {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey", key)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	case r.Method == http.MethodDelete:
		if err := os.Remove(s3Path(root, key)); err != nil && !os.IsNotExist(err) {
			errJSON(w, http.StatusInternalServerError, "delete object")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodHead:
		st, err := os.Stat(s3Path(root, key))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(st.Size()))
		w.WriteHeader(http.StatusOK)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) s3List(w http.ResponseWriter, r *http.Request, root string) {
	prefix := r.URL.Query().Get("prefix")
	type obj struct{ key string; size int64 }
	var objs []obj
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = url.PathEscape(rel)
		rel = strings.ReplaceAll(rel, "%2F", "/")
		if strings.HasPrefix(rel, prefix) {
			objs = append(objs, obj{rel, info.Size()})
		}
		return nil
	})
	sort.Slice(objs, func(i, j int) bool { return objs[i].key < objs[j].key })
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, `<Name>%s</Name><IsTruncated>false</IsTruncated>`, filepath.Base(root))
	for _, o := range objs {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size></Contents>`, o.key, o.size)
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

func s3Path(root, key string) string {
	clean := filepath.Clean("/" + key)
	return filepath.Join(root, clean)
}

func writeS3Error(w http.ResponseWriter, status int, code, key string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Key>%s</Key></Error>`, code, key)
}

// handleEvents is the minimal Tier-0 SSE stream (spec §9): a subscribed
// client gets an open text/event-stream with heartbeats and no resets for
// a quiet tenant. Without this the mount daemons treat the missing stream
// as a broken subscription and periodically invalidate their whole
// directory cache (observed as flapping readdirs on second mounts).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		errJSON(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	// a stable head seq: no events are ever generated, so every client's
	// cursor (since) is <= 1 and the stream never resets it. A heartbeat
	// carrying seq 0 would read as seq_too_old for any since > 0 and cause
	// a reset storm.
	_, _ = fmt.Fprintf(w, "event: reset\ndata: {\"seq\":1,\"reason\":\"initial_sync\"}\n\n")
	flusher.Flush()
	heartbeat := func() {
		_, _ = fmt.Fprintf(w, "event: heartbeat\ndata: {\"seq\":1}\n\n")
		flusher.Flush()
	}
	heartbeat()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeat()
		}
	}
}
