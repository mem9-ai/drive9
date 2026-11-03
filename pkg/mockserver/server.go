// Package mockserver provides an embedded drive9 backend for the simulator:
// the wire contract of simulator/drive9-server-api-spec.md implemented over
// SQLite (metadb + per-tenant tenantdb) and a directory-backed S3-compatible
// endpoint. It exists so client-side cases run without TiDB or an external
// object store; it is a fidelity mock of the API surface, never of the
// production storage engines. Tier coverage is tracked in spec §21.
package mockserver

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Config configures the mock backend.
type Config struct {
	// Dir holds meta.db, tenant-<id>.db files and (unless S3Endpoint is
	// set) the object store directory.
	Dir string
	// InlineThreshold is the DB-inline vs S3 cutoff reported by /v1/status.
	// Tier 0 accepts every PUT inline regardless; the value only shapes the
	// advertised contract.
	InlineThreshold int64
	// MaxUploadBytes caps /v1/uploads total_size.
	MaxUploadBytes int64
	// S3Endpoint, when set, is advertised by /v1/data-credential as the
	// extent data plane (e.g. the embedded rustfs the simulator stack
	// manages). Empty = scheme "file" pointing at Dir/objects.
	S3Endpoint string
	S3Bucket   string
	S3AK       string
	S3SK       string
	Logger     *zap.Logger
}

// ErrNotFound / ErrPathConflict / ErrDirectoryNotEmpty mirror the spec's
// status semantics (§19.2).
var (
	ErrNotFound         = errors.New("not found")
	ErrPathConflict     = errors.New("path conflict")
	ErrDirectoryNotEmpty = errors.New("directory not empty")
)

type Server struct {
	cfg   Config
	meta  *metaStore
	mu    sync.Mutex
	tenants map[string]*tenantDB // tenant_id -> open handle
	hubs  map[string]*eventHub   // tenant_id -> change bus
	log   *zap.Logger
	mux   *http.ServeMux
}

func (s *Server) hubFor(tenant string) *eventHub {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, ok := s.hubs[tenant]; ok {
		return h
	}
	h := newEventHub()
	s.hubs[tenant] = h
	return h
}

// New opens (creating if needed) the mock backend under cfg.Dir.
func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.InlineThreshold <= 0 {
		cfg.InlineThreshold = 50_000 // mirror the production default: same direct-PUT vs multipart decision shape
	}
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = 1 << 32
	}
	meta, err := openMetaStore(cfg.Dir, cfg.InlineThreshold, cfg.MaxUploadBytes)
	if err != nil {
		return nil, fmt.Errorf("open meta store: %w", err)
	}
	s := &Server{
		cfg:     cfg,
		meta:    meta,
		tenants: map[string]*tenantDB{},
		hubs:    map[string]*eventHub{},
		log:     cfg.Logger,
	}
	s.routes()
	return s, nil
}

// Close releases every database handle.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, t := range s.tenants {
		if err := t.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := s.meta.close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Handler returns the routed HTTP handler.
func (s *Server) Handler() http.Handler { return s.logRequests(s.mux) }

// logRequests records method, path and duration per request — the mock's
// equivalent of an access log, which makes backend-side stalls visible
// (a hung client request shows up as the last line with no completion).
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.log.Info("request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("query", r.URL.RawQuery),
			zap.Duration("dur", time.Since(start)),
		)
	})
}

func (s *Server) routes() {
	mux := http.NewServeMux()
	business := http.HandlerFunc(s.handleBusiness)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/v1/provision", s.handleProvision)
	mux.HandleFunc("/v1/status", s.auth(s.handleStatus))
	mux.HandleFunc("/v1/tenant", s.auth(s.handleTenantDelete))
	mux.Handle("/v1/fs", business)           // batch selectors live here
	mux.Handle("/v1/fs/", business)
	mux.Handle("/v1/uploads", s.auth(s.handleUploads))
	mux.Handle("/v1/uploads/", s.auth(s.handleUploads))
	mux.HandleFunc("/v1/extent/meta", s.auth(s.handleExtentMeta))
	mux.HandleFunc("/v1/data-credential", s.auth(s.handleDataCredential))
	mux.HandleFunc("/v1/sql", s.auth(s.handleSQL))
	mux.HandleFunc("/v1/events", s.auth(s.handleEvents))
	// embedded S3-compatible endpoint (dir-backed, no SigV4 verification —
	// mock only; swap to rustfs via Config.S3Endpoint for the data plane)
	if s.cfg.S3Endpoint == "" {
		mux.HandleFunc("/s3/", s.handleS3)
	}
	s.mux = mux
}

// ---------------------------------------------------------------- helpers

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errJSON(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func errJSONCode(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}

// auth wraps a tenant-authenticated handler (spec §2.1).
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := s.authenticate(w, r)
		if !ok {
			return
		}
		next(w, r.WithContext(withTenant(r, tenant)))
	}
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (*tenantDB, bool) {
	key := r.Header.Get("Authorization")
	key = strings.TrimPrefix(key, "Bearer ")
	if key == "" {
		key = r.Header.Get("X-API-Key")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return nil, false
	}
	id, err := s.meta.tenantIDByKey(key)
	if err != nil {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return nil, false
	}
	tdb, err := s.tenant(id)
	if err != nil {
		errJSON(w, http.StatusUnauthorized, "invalid API key")
		return nil, false
	}
	return tdb, true
}

func (s *Server) tenant(id string) (*tenantDB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tenants[id]; ok {
		return t, nil
	}
	t, err := openTenantDB(s.cfg.Dir, id)
	if err != nil {
		return nil, err
	}
	s.tenants[id] = t
	return t, nil
}

func (s *Server) dropTenant(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tenants[id]; ok {
		_ = t.close()
		delete(s.tenants, id)
	}
}

// ------------------------------------------------------------ lifecycle

func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// The mock ignores quota fields entirely (spec §3.2 shape is echoed).
	id := "t" + hex.EncodeToString(randomBytes(10))
	raw := randomBytes(24)
	key := "drive9_" + base64.RawURLEncoding.EncodeToString(raw)
	if err := s.meta.createTenant(id, key); err != nil {
		errJSON(w, http.StatusInternalServerError, "provision failed")
		return
	}
	if _, err := s.tenant(id); err != nil {
		errJSON(w, http.StatusInternalServerError, "tenant init failed")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"tenant_id": id,
		"api_key":   key,
		"status":    "active",
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	tdb := tenantFrom(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "active",
		"max_upload_bytes": s.cfg.MaxUploadBytes,
		"inline_threshold": s.cfg.InlineThreshold,
	})
	_ = tdb
}

func (s *Server) handleTenantDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tdb := tenantFrom(r)
	if err := s.meta.deleteTenant(tdb.id); err != nil {
		errJSON(w, http.StatusInternalServerError, "delete failed")
		return
	}
	s.dropTenant(tdb.id)
	_ = tdb.dropFile()
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// parseContentRange handles a single-range Range header for reads.
func parseContentRange(rng string, size int64) (start, end int64, ok bool) {
	if rng == "" || size == 0 {
		return 0, size - 1, false
	}
	const p = "bytes="
	if !strings.HasPrefix(rng, p) {
		return 0, -1, false
	}
	spec := strings.TrimPrefix(rng, p)
	if strings.Contains(spec, ",") {
		return 0, -1, false // multi-range unsupported → 416 per Tier 0
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, -1, false
	}
	ls, us := strings.TrimSpace(spec[:dash]), strings.TrimSpace(spec[dash+1:])
	if ls == "" {
		// suffix range
		n, err := strconv.ParseInt(us, 10, 64)
		if err != nil || n <= 0 {
			return 0, -1, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}
	start, err := strconv.ParseInt(ls, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, -1, false
	}
	if us == "" {
		return start, size - 1, true
	}
	end, err = strconv.ParseInt(us, 10, 64)
	if err != nil || end < start {
		return 0, -1, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

var _ = time.Now
