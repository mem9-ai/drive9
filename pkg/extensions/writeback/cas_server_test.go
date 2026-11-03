package writeback

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// casFileServer is a one-file CAS backend for commit-queue tests (ported
// from v1's writeback_generation_fence_test.go): conditional PUTs enforce
// the expected revision, and HEAD/GET report the durable state.
type casFileServer struct {
	t    *testing.T
	path string

	mu        sync.Mutex
	revision  int64
	body      []byte
	puts      []casFilePut
	headCalls int
	getCalls  int

	firstPutStarted chan struct{}
	releaseFirstPut chan struct{}
	firstPutGate    sync.Once
}

type casFilePut struct {
	expected string
	revision int64
	body     []byte
}

func newCASFileServer(t *testing.T, path string, revision int64, body []byte) (*casFileServer, *httptest.Server) {
	t.Helper()
	s := &casFileServer{
		t:        t,
		path:     path,
		revision: revision,
		body:     append([]byte(nil), body...),
	}
	ts := httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s, ts
}

func (s *casFileServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/fs"+s.path {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPut:
		expected := r.Header.Get("X-Dat9-Expected-Revision")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		s.firstPutGate.Do(func() {
			if s.firstPutStarted != nil && s.releaseFirstPut != nil {
				close(s.firstPutStarted)
				<-s.releaseFirstPut
			}
		})
		s.mu.Lock()
		defer s.mu.Unlock()
		wantExpected := fmt.Sprintf("%d", s.revision)
		if expected != wantExpected {
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
			return
		}
		s.revision++
		s.body = append([]byte(nil), body...)
		s.puts = append(s.puts, casFilePut{
			expected: expected,
			revision: s.revision,
			body:     append([]byte(nil), body...),
		})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": s.revision})
	case http.MethodHead:
		s.mu.Lock()
		s.headCalls++
		revision := s.revision
		size := len(s.body)
		s.mu.Unlock()
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", fmt.Sprintf("%d", revision))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		s.mu.Lock()
		s.getCalls++
		body := append([]byte(nil), s.body...)
		s.mu.Unlock()
		_, _ = w.Write(body)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *casFileServer) snapshot() (int64, []byte, []casFilePut) {
	s.mu.Lock()
	defer s.mu.Unlock()
	puts := make([]casFilePut, len(s.puts))
	copy(puts, s.puts)
	return s.revision, append([]byte(nil), s.body...), puts
}
