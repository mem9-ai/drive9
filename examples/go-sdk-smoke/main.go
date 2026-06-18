// Package main is a runnable smoke demo for the dat9/drive9 Go SDK.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

const mockAPIKey = "demo-api-key"

type demoConfig struct {
	baseURL string
	apiKey  string
	root    string
	mock    bool
}

type demoResult struct {
	RootPath              string
	StatusHits            int64
	MaxUploadBytes        int64
	SmallFileThreshold    int64
	CachedThreshold       int64
	SmallRevision         int64
	LargeRevision         int64
	LargeUploadMode       string
	LargeUploadParts      int
	BatchStatCount        int
	BatchReadSmallCount   int
	DirectUploadMode      string
	PatchChecksumVerified bool
}

func main() {
	var cfg demoConfig
	flag.BoolVar(&cfg.mock, "mock", false, "run against an in-process mock server")
	flag.StringVar(&cfg.baseURL, "base-url", firstNonEmpty(os.Getenv("DRIVE9_SERVER"), os.Getenv("DRIVE9_BASE_URL"), os.Getenv("DRIVE9_BASE")), "drive9 server base URL")
	flag.StringVar(&cfg.apiKey, "api-key", os.Getenv("DRIVE9_API_KEY"), "drive9 API key")
	flag.StringVar(&cfg.root, "root", "/go-sdk-smoke-"+time.Now().UTC().Format("20060102T150405Z"), "remote root path for live mode")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	result, err := runDemo(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sdk smoke failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("drive9 Go SDK smoke passed")
	fmt.Printf("root=%s\n", result.RootPath)
	fmt.Printf("status_hits=%d\n", result.StatusHits)
	fmt.Printf("max_upload_bytes=%d\n", result.MaxUploadBytes)
	fmt.Printf("small_file_threshold=%d cached=%d\n", result.SmallFileThreshold, result.CachedThreshold)
	fmt.Printf("small_revision=%d large_revision=%d\n", result.SmallRevision, result.LargeRevision)
	fmt.Printf("large_upload_mode=%s parts=%d\n", result.LargeUploadMode, result.LargeUploadParts)
	fmt.Printf("batch_stat_results=%d batch_read_small_results=%d\n", result.BatchStatCount, result.BatchReadSmallCount)
	fmt.Printf("direct_upload_mode=%s\n", result.DirectUploadMode)
	fmt.Printf("patch_checksum_header_verified=%t\n", result.PatchChecksumVerified)
}

func runDemo(ctx context.Context, cfg demoConfig) (demoResult, error) {
	var mock *mockDrive9Server
	if cfg.mock {
		mock = newMockDrive9Server(mockAPIKey)
		defer mock.Close()
		cfg.baseURL = mock.URL()
		cfg.apiKey = mockAPIKey
		cfg.root = "/go-sdk-smoke-mock"
	}
	if strings.TrimSpace(cfg.baseURL) == "" {
		return demoResult{}, fmt.Errorf("missing base URL: pass -mock or set DRIVE9_BASE_URL")
	}
	if strings.TrimSpace(cfg.apiKey) == "" {
		return demoResult{}, fmt.Errorf("missing API key: pass -mock or set DRIVE9_API_KEY")
	}
	cfg.root = cleanRemoteDir(cfg.root)

	c := client.New(cfg.baseURL, cfg.apiKey)
	c.SetActor("go-sdk-smoke")

	statusBefore := int64(0)
	if mock != nil {
		statusBefore = mock.StatusHits()
	}
	c.Warm(ctx)
	maxUpload := c.MaxUploadBytes(ctx)
	threshold := c.SmallFileThreshold(ctx)
	cachedThreshold := c.CachedSmallFileThreshold()
	if threshold <= 0 {
		return demoResult{}, fmt.Errorf("server did not advertise inline threshold")
	}
	if cachedThreshold != threshold {
		return demoResult{}, fmt.Errorf("cached threshold = %d, want %d", cachedThreshold, threshold)
	}
	statusHits := int64(0)
	if mock != nil {
		statusHits = mock.StatusHits() - statusBefore
		if statusHits != 1 {
			return demoResult{}, fmt.Errorf("status cache fetched %d times, want 1", statusHits)
		}
	}

	_ = c.RemoveAllCtx(ctx, cfg.root)
	if err := c.MkdirCtx(ctx, cfg.root, 0o755); err != nil {
		return demoResult{}, fmt.Errorf("mkdir root: %w", err)
	}

	smallPath := cfg.root + "/hello.txt"
	smallData := []byte("hello from drive9 go sdk")
	smallRev, err := c.WriteCtxConditionalWithRevision(ctx, smallPath, smallData, 0)
	if err != nil {
		return demoResult{}, fmt.Errorf("write small with create CAS: %w", err)
	}
	if smallRev <= 0 {
		return demoResult{}, fmt.Errorf("small write returned revision %d", smallRev)
	}
	if err := c.WriteCtxConditional(ctx, smallPath, []byte("must conflict"), 0); !errors.Is(err, client.ErrConflict) {
		return demoResult{}, fmt.Errorf("create CAS conflict error = %v, want ErrConflict", err)
	}
	if err := c.WriteCtxConditionalWithDescription(ctx, cfg.root+"/described.txt", []byte("described"), 0, "go sdk smoke description"); err != nil {
		return demoResult{}, fmt.Errorf("write description: %w", err)
	}
	directPath := cfg.root + "/direct-stream.txt"
	directData := []byte("stream direct put")
	directSummary, err := c.WriteStreamWithSummary(ctx, directPath, bytes.NewReader(directData), int64(len(directData)), nil)
	if err != nil {
		return demoResult{}, fmt.Errorf("write stream direct: %w", err)
	}
	if directSummary.Mode != "direct_put" {
		return demoResult{}, fmt.Errorf("direct stream mode = %q, want direct_put", directSummary.Mode)
	}

	stat, err := c.StatCtx(ctx, smallPath)
	if err != nil {
		return demoResult{}, fmt.Errorf("stat small: %w", err)
	}
	if stat.Size != int64(len(smallData)) || stat.Revision != smallRev {
		return demoResult{}, fmt.Errorf("stat small = size %d rev %d, want size %d rev %d", stat.Size, stat.Revision, len(smallData), smallRev)
	}
	readBack, err := c.ReadCtx(ctx, smallPath)
	if err != nil {
		return demoResult{}, fmt.Errorf("read small: %w", err)
	}
	if !bytes.Equal(readBack, smallData) {
		return demoResult{}, fmt.Errorf("read small mismatch")
	}
	rangeBack, err := c.ReadAtCtx(ctx, smallPath, 6, 4)
	if err != nil {
		return demoResult{}, fmt.Errorf("range read small: %w", err)
	}
	if string(rangeBack) != "from" {
		return demoResult{}, fmt.Errorf("range read = %q, want %q", rangeBack, "from")
	}

	batchStat, err := c.BatchStatCtx(ctx, []string{smallPath, cfg.root + "/missing.txt", smallPath})
	if err != nil {
		return demoResult{}, fmt.Errorf("batch stat: %w", err)
	}
	if len(batchStat) != 3 || !batchStat[0].OK() || batchStat[1].OK() || !batchStat[2].OK() {
		return demoResult{}, fmt.Errorf("unexpected batch stat results: %+v", batchStat)
	}
	batchRead, err := c.BatchReadSmallCtx(ctx, []string{smallPath, cfg.root + "/missing.txt", smallPath}, int64(len(smallData)+16))
	if err != nil {
		return demoResult{}, fmt.Errorf("batch read small: %w", err)
	}
	if len(batchRead) != 3 || !batchRead[0].OK() || string(batchRead[0].Data) != string(smallData) || batchRead[1].OK() || !batchRead[2].OK() {
		return demoResult{}, fmt.Errorf("unexpected batch read-small results: %+v", batchRead)
	}

	meta, err := c.StatMetadataCompatCtx(ctx, smallPath)
	if err != nil {
		return demoResult{}, fmt.Errorf("stat metadata compat: %w", err)
	}
	if meta.Size != int64(len(smallData)) || meta.Revision != smallRev {
		return demoResult{}, fmt.Errorf("stat metadata = size %d rev %d", meta.Size, meta.Revision)
	}

	copyPath := cfg.root + "/copy.txt"
	renamedPath := cfg.root + "/renamed.txt"
	if err := c.CopyCtx(ctx, smallPath, copyPath); err != nil {
		return demoResult{}, fmt.Errorf("copy: %w", err)
	}
	if err := c.RenameCtx(ctx, copyPath, renamedPath); err != nil {
		return demoResult{}, fmt.Errorf("rename: %w", err)
	}
	if err := c.ChmodCtx(ctx, renamedPath, 0o640); err != nil {
		return demoResult{}, fmt.Errorf("chmod: %w", err)
	}
	entries, err := c.ListCtx(ctx, cfg.root+"/")
	if err != nil {
		return demoResult{}, fmt.Errorf("list root: %w", err)
	}
	if len(entries) < 3 {
		return demoResult{}, fmt.Errorf("list returned %d entries, want at least 3", len(entries))
	}

	largePath := cfg.root + "/large.bin"
	largeSize := int(threshold) + 64
	if largeSize < 96 {
		largeSize = 96
	}
	largeData := patternedBytes(largeSize)
	largeSummary, err := c.WriteStreamWithSummaryAndTags(ctx, largePath, bytes.NewReader(largeData), int64(len(largeData)), nil, map[string]string{
		"demo": "go-sdk-smoke",
		"kind": "large",
	})
	if err != nil {
		return demoResult{}, fmt.Errorf("write large stream: %w", err)
	}
	if largeSummary.Mode == "" || largeSummary.TotalParts <= 0 {
		return demoResult{}, fmt.Errorf("large summary incomplete: %+v", largeSummary)
	}
	largeStat, err := c.StatCtx(ctx, largePath)
	if err != nil {
		return demoResult{}, fmt.Errorf("stat large: %w", err)
	}
	if largeStat.Size != int64(len(largeData)) {
		return demoResult{}, fmt.Errorf("large size = %d, want %d", largeStat.Size, len(largeData))
	}

	partSize := largeSummary.PartSizeBytes
	if partSize <= 0 {
		partSize = int64(len(largeData))
	}
	patchData := append([]byte(nil), largeData...)
	patchData[0] = 'P'
	if err := c.PatchFile(ctx, largePath, int64(len(patchData)), []int{1}, func(partNumber int, gotPartSize int64, origData []byte) ([]byte, error) {
		start := int64(partNumber-1) * partSize
		end := start + gotPartSize
		if end > int64(len(patchData)) {
			end = int64(len(patchData))
		}
		return append([]byte(nil), patchData[start:end]...), nil
	}, nil, client.WithPartSize(partSize), client.WithExpectedRevision(largeStat.Revision)); err != nil {
		return demoResult{}, fmt.Errorf("patch large: %w", err)
	}
	patchedPrefix, err := c.ReadAtCtx(ctx, largePath, 0, 4)
	if err != nil {
		return demoResult{}, fmt.Errorf("read patched range: %w", err)
	}
	if string(patchedPrefix) != string(patchData[:4]) {
		return demoResult{}, fmt.Errorf("patch data mismatch: got %q want %q", patchedPrefix, patchData[:4])
	}
	largeStatAfterPatch, err := c.StatCtx(ctx, largePath)
	if err != nil {
		return demoResult{}, fmt.Errorf("stat large after patch: %w", err)
	}

	patchChecksumVerified := true
	if mock != nil {
		patchChecksumVerified = mock.PatchChecksumHeaderSeen()
		if !patchChecksumVerified {
			return demoResult{}, fmt.Errorf("mock did not observe patch checksum header")
		}
	}

	if err := c.RemoveAllCtx(ctx, cfg.root); err != nil {
		return demoResult{}, fmt.Errorf("cleanup remove root: %w", err)
	}

	return demoResult{
		RootPath:              cfg.root,
		StatusHits:            statusHits,
		MaxUploadBytes:        maxUpload,
		SmallFileThreshold:    threshold,
		CachedThreshold:       cachedThreshold,
		SmallRevision:         smallRev,
		LargeRevision:         largeStatAfterPatch.Revision,
		LargeUploadMode:       largeSummary.Mode,
		LargeUploadParts:      largeSummary.TotalParts,
		BatchStatCount:        len(batchStat),
		BatchReadSmallCount:   len(batchRead),
		DirectUploadMode:      directSummary.Mode,
		PatchChecksumVerified: patchChecksumVerified,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func cleanRemoteDir(p string) string {
	p = "/" + strings.Trim(strings.TrimSpace(p), "/")
	if p == "/" {
		return "/go-sdk-smoke"
	}
	return p
}

func patternedBytes(size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte('a' + i%26)
	}
	return out
}

type mockDrive9Server struct {
	server              *httptest.Server
	apiKey              string
	inlineThreshold     int64
	maxUploadBytes      int64
	statusHits          atomic.Int64
	patchChecksumHeader atomic.Bool

	mu       sync.Mutex
	nextRev  int64
	files    map[string]*mockFile
	uploads  map[string]*mockUpload
	uploadID int64
}

type mockFile struct {
	data        []byte
	revision    int64
	isDir       bool
	mode        uint32
	tags        map[string]string
	description string
	mtime       int64
}

type mockUpload struct {
	id       string
	kind     string
	path     string
	size     int64
	partSize int64
	parts    map[int][]byte
	tags     map[string]string
}

func newMockDrive9Server(apiKey string) *mockDrive9Server {
	m := &mockDrive9Server{
		apiKey:          apiKey,
		inlineThreshold: 32,
		maxUploadBytes:  1 << 30,
		nextRev:         1,
		files:           map[string]*mockFile{},
		uploads:         map[string]*mockUpload{},
	}
	m.files["/"] = &mockFile{isDir: true, mode: 0o755, revision: m.nextRevisionLocked(), mtime: time.Now().Unix()}
	m.server = httptest.NewServer(m)
	return m
}

func (m *mockDrive9Server) URL() string {
	return m.server.URL
}

func (m *mockDrive9Server) Close() {
	m.server.Close()
}

func (m *mockDrive9Server) StatusHits() int64 {
	return m.statusHits.Load()
}

func (m *mockDrive9Server) PatchChecksumHeaderSeen() bool {
	return m.patchChecksumHeader.Load()
}

func (m *mockDrive9Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v1/status":
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.statusHits.Add(1)
		writeJSON(w, http.StatusOK, map[string]any{
			"status":           "ok",
			"max_upload_bytes": m.maxUploadBytes,
			"inline_threshold": m.inlineThreshold,
		})
	case r.URL.Path == "/v1/fs:batch-stat":
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.handleBatchStat(w, r)
	case r.URL.Path == "/v1/fs:batch-read-small":
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.handleBatchReadSmall(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/fs/"):
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.handleFS(w, r)
	case r.URL.Path == "/v2/uploads/initiate":
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.handleV2Initiate(w, r)
	case strings.HasPrefix(r.URL.Path, "/v2/uploads/"):
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.handleV2Upload(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/uploads/"):
		m.requireAuth(w, r)
		if w.Header().Get("X-Demo-Auth-Failed") != "" {
			return
		}
		m.handleV1Upload(w, r)
	case strings.HasPrefix(r.URL.Path, "/mock-object/"):
		m.handleObjectPUT(w, r)
	case strings.HasPrefix(r.URL.Path, "/mock-patch/"):
		m.handlePatchPUT(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockDrive9Server) requireAuth(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+m.apiKey {
		w.Header().Set("X-Demo-Auth-Failed", "1")
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or wrong bearer token"})
	}
}

func (m *mockDrive9Server) handleFS(w http.ResponseWriter, r *http.Request) {
	remotePath := strings.TrimPrefix(r.URL.Path, "/v1/fs")
	if remotePath == "" {
		remotePath = "/"
	}
	remotePath, _ = url.PathUnescape(remotePath)
	q := r.URL.Query()

	switch r.Method {
	case http.MethodPost:
		switch {
		case hasQuery(q, "mkdir"):
			m.mkdir(w, remotePath)
		case hasQuery(q, "copy"):
			m.copy(w, r.Header.Get("X-Dat9-Copy-Source"), remotePath)
		case hasQuery(q, "rename"):
			m.rename(w, r.Header.Get("X-Dat9-Rename-Source"), remotePath)
		case hasQuery(q, "chmod"):
			m.chmod(w, r, remotePath)
		default:
			http.NotFound(w, r)
		}
	case http.MethodPut:
		m.writeFile(w, r, remotePath)
	case http.MethodGet:
		switch {
		case hasQuery(q, "list"):
			m.list(w, remotePath)
		case hasQuery(q, "stat"):
			m.statMetadata(w, remotePath)
		default:
			m.readFile(w, remotePath)
		}
	case http.MethodHead:
		m.head(w, remotePath)
	case http.MethodDelete:
		m.delete(w, remotePath, hasQuery(q, "recursive"))
	case http.MethodPatch:
		m.patchPlan(w, r, remotePath)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockDrive9Server) mkdir(w http.ResponseWriter, remotePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[cleanRemoteDir(remotePath)] = &mockFile{isDir: true, mode: 0o755, revision: m.nextRevisionLocked(), mtime: time.Now().Unix()}
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) writeFile(w http.ResponseWriter, r *http.Request, remotePath string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	expected, hasExpected := parseIntHeader(r.Header.Get("X-Dat9-Expected-Revision"))
	m.mu.Lock()
	defer m.mu.Unlock()
	if hasExpected {
		cur, ok := m.files[remotePath]
		switch {
		case expected == 0 && ok:
			writeJSON(w, http.StatusConflict, map[string]string{"error": "path already exists"})
			return
		case expected > 0 && (!ok || cur.revision != expected):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "revision conflict"})
			return
		}
	}
	rev := m.nextRevisionLocked()
	m.files[remotePath] = &mockFile{
		data:        append([]byte(nil), body...),
		revision:    rev,
		mode:        0o644,
		tags:        parseTagHeaders(r.Header.Values("X-Dat9-Tag")),
		description: r.Header.Get("X-Dat9-Description"),
		mtime:       time.Now().Unix(),
	}
	writeJSON(w, http.StatusOK, map[string]int64{"revision": rev})
}

func (m *mockDrive9Server) readFile(w http.ResponseWriter, remotePath string) {
	m.mu.Lock()
	f, ok := m.files[remotePath]
	m.mu.Unlock()
	if !ok || f.isDir {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(f.data)
}

func (m *mockDrive9Server) head(w http.ResponseWriter, remotePath string) {
	m.mu.Lock()
	f, ok := m.files[remotePath]
	m.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(f.data)))
	w.Header().Set("X-Dat9-Revision", strconv.FormatInt(f.revision, 10))
	w.Header().Set("X-Dat9-IsDir", strconv.FormatBool(f.isDir))
	w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(f.mtime, 10))
	w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(f.mode), 10))
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) statMetadata(w http.ResponseWriter, remotePath string) {
	m.mu.Lock()
	f, ok := m.files[remotePath]
	m.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	mtime := f.mtime
	writeJSON(w, http.StatusOK, map[string]any{
		"size":          len(f.data),
		"isdir":         f.isDir,
		"revision":      f.revision,
		"mtime":         mtime,
		"content_type":  "text/plain",
		"semantic_text": f.description,
		"tags":          f.tags,
	})
}

func (m *mockDrive9Server) list(w http.ResponseWriter, remotePath string) {
	prefix := cleanRemoteDir(remotePath)
	if prefix != "/" {
		prefix += "/"
	}
	seen := map[string]client.FileInfo{}
	m.mu.Lock()
	for p, f := range m.files {
		if p == remotePath || p == "/" || !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := strings.TrimPrefix(p, prefix)
		name := strings.Split(rest, "/")[0]
		if name == "" {
			continue
		}
		info := client.FileInfo{Name: name, IsDir: f.isDir, Size: int64(len(f.data)), Mtime: f.mtime, Mode: f.mode, HasMode: true}
		if strings.Contains(rest, "/") {
			info.IsDir = true
			info.Size = 0
		}
		seen[name] = info
	}
	m.mu.Unlock()
	entries := make([]client.FileInfo, 0, len(seen))
	for _, entry := range seen {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (m *mockDrive9Server) copy(w http.ResponseWriter, srcPath, dstPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src, ok := m.files[srcPath]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "source not found"})
		return
	}
	clone := *src
	clone.data = append([]byte(nil), src.data...)
	clone.tags = cloneMap(src.tags)
	clone.revision = m.nextRevisionLocked()
	clone.mtime = time.Now().Unix()
	m.files[dstPath] = &clone
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) rename(w http.ResponseWriter, srcPath, dstPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src, ok := m.files[srcPath]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "source not found"})
		return
	}
	delete(m.files, srcPath)
	src.revision = m.nextRevisionLocked()
	src.mtime = time.Now().Unix()
	m.files[dstPath] = src
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) chmod(w http.ResponseWriter, r *http.Request, remotePath string) {
	var req struct {
		Mode uint32 `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[remotePath]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	f.mode = req.Mode
	f.revision = m.nextRevisionLocked()
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) delete(w http.ResponseWriter, remotePath string, recursive bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.files[remotePath]; !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	delete(m.files, remotePath)
	if recursive {
		prefix := cleanRemoteDir(remotePath) + "/"
		for p := range m.files {
			if strings.HasPrefix(p, prefix) {
				delete(m.files, p)
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) handleBatchStat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Paths []string `json:"paths"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	results := make([]client.BatchStatResult, 0, len(req.Paths))
	m.mu.Lock()
	for _, p := range req.Paths {
		if f, ok := m.files[p]; ok {
			results = append(results, client.BatchStatResult{Path: p, Status: http.StatusOK, Size: int64(len(f.data)), IsDir: f.isDir, Revision: f.revision, Mtime: f.mtime, Mode: f.mode, HasMode: true})
		} else {
			results = append(results, client.BatchStatResult{Path: p, Status: http.StatusNotFound, Error: "not found"})
		}
	}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (m *mockDrive9Server) handleBatchReadSmall(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Paths    []string `json:"paths"`
		MaxBytes int64    `json:"max_bytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	results := make([]client.BatchReadSmallResult, 0, len(req.Paths))
	m.mu.Lock()
	for _, p := range req.Paths {
		if f, ok := m.files[p]; ok && !f.isDir && (req.MaxBytes <= 0 || int64(len(f.data)) <= req.MaxBytes) {
			results = append(results, client.BatchReadSmallResult{Path: p, Status: http.StatusOK, Data: append([]byte(nil), f.data...), Size: int64(len(f.data)), Revision: f.revision, Mtime: f.mtime})
		} else {
			results = append(results, client.BatchReadSmallResult{Path: p, Status: http.StatusNotFound, Error: "not found"})
		}
	}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (m *mockDrive9Server) handleV2Initiate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path      string `json:"path"`
		TotalSize int64  `json:"total_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.TotalSize <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "total_size must be positive"})
		return
	}
	partSize := int64(16)
	totalParts := int((req.TotalSize + partSize - 1) / partSize)
	uploadID := m.newUpload("v2", req.Path, req.TotalSize, partSize)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"upload_id":   uploadID,
		"key":         "mock/" + uploadID,
		"part_size":   partSize,
		"total_parts": totalParts,
		"expires_at":  time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		"resumable":   true,
		"checksum_contract": map[string]any{
			"supported": []string{},
			"required":  false,
		},
	})
}

func (m *mockDrive9Server) handleV2Upload(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v2/uploads/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	uploadID, action := parts[0], parts[1]
	switch action {
	case "presign-batch":
		m.presignBatch(w, r, uploadID)
	case "presign":
		m.presignOne(w, r, uploadID)
	case "complete":
		m.completeV2(w, r, uploadID)
	case "abort":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockDrive9Server) presignBatch(w http.ResponseWriter, r *http.Request, uploadID string) {
	var req struct {
		Parts []struct {
			PartNumber int `json:"part_number"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(req.Parts))
	for _, p := range req.Parts {
		out = append(out, m.presignedObjectPart(uploadID, p.PartNumber))
	}
	writeJSON(w, http.StatusOK, map[string]any{"parts": out})
}

func (m *mockDrive9Server) presignOne(w http.ResponseWriter, r *http.Request, uploadID string) {
	var req struct {
		PartNumber int `json:"part_number"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, m.presignedObjectPart(uploadID, req.PartNumber))
}

func (m *mockDrive9Server) presignedObjectPart(uploadID string, partNumber int) map[string]any {
	m.mu.Lock()
	u := m.uploads[uploadID]
	m.mu.Unlock()
	size := partSize(u.size, u.partSize, partNumber)
	return map[string]any{
		"number":     partNumber,
		"url":        fmt.Sprintf("%s/mock-object/%s/%d", m.URL(), uploadID, partNumber),
		"size":       size,
		"headers":    map[string]string{"x-mock-upload": uploadID},
		"expires_at": time.Now().Add(time.Hour),
	}
}

func (m *mockDrive9Server) handleObjectPUT(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.NotFound(w, r)
		return
	}
	uploadID, partNumber, ok := parseMockPart(r.URL.Path, "/mock-object/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	u := m.uploads[uploadID]
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "upload not found"})
		return
	}
	u.parts[partNumber] = body
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%s-%d", uploadID, partNumber)))
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) completeV2(w http.ResponseWriter, r *http.Request, uploadID string) {
	var req struct {
		Tags map[string]string `json:"tags"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	m.mu.Lock()
	defer m.mu.Unlock()
	u := m.uploads[uploadID]
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "upload not found"})
		return
	}
	data := assembleParts(u)
	rev := m.nextRevisionLocked()
	m.files[u.path] = &mockFile{data: data, revision: rev, mode: 0o644, tags: cloneMap(req.Tags), mtime: time.Now().Unix()}
	delete(m.uploads, uploadID)
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) patchPlan(w http.ResponseWriter, r *http.Request, remotePath string) {
	var req struct {
		NewSize          int64  `json:"new_size"`
		DirtyParts       []int  `json:"dirty_parts"`
		PartSize         int64  `json:"part_size"`
		ExpectedRevision *int64 `json:"expected_revision"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.PartSize <= 0 {
		req.PartSize = 16
	}
	m.mu.Lock()
	if req.ExpectedRevision != nil {
		if f := m.files[remotePath]; f == nil || f.revision != *req.ExpectedRevision {
			m.mu.Unlock()
			writeJSON(w, http.StatusConflict, map[string]string{"error": "revision conflict"})
			return
		}
	}
	m.mu.Unlock()
	uploadID := m.newUpload("patch", remotePath, req.NewSize, req.PartSize)
	uploadParts := make([]map[string]any, 0, len(req.DirtyParts))
	for _, part := range req.DirtyParts {
		uploadParts = append(uploadParts, map[string]any{
			"number":     part,
			"url":        fmt.Sprintf("%s/mock-patch/%s/%d", m.URL(), uploadID, part),
			"size":       partSize(req.NewSize, req.PartSize, part),
			"headers":    map[string]string{"x-amz-checksum-sha256": "signed-placeholder"},
			"expires_at": time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		})
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"upload_id":    uploadID,
		"part_size":    req.PartSize,
		"upload_parts": uploadParts,
		"copied_parts": []int{},
	})
}

func (m *mockDrive9Server) handlePatchPUT(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.NotFound(w, r)
		return
	}
	uploadID, partNumber, ok := parseMockPart(r.URL.Path, "/mock-patch/")
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("x-amz-checksum-sha256") != "" && r.Header.Get("x-amz-checksum-sha256") != "signed-placeholder" {
		m.patchChecksumHeader.Store(true)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	u := m.uploads[uploadID]
	if u == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "upload not found"})
		return
	}
	u.parts[partNumber] = body
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%s-%d", uploadID, partNumber)))
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) handleV1Upload(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[1] != "complete" {
		http.NotFound(w, r)
		return
	}
	uploadID := parts[0]
	m.mu.Lock()
	defer m.mu.Unlock()
	u := m.uploads[uploadID]
	if u == nil || u.kind != "patch" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "upload not found"})
		return
	}
	cur := m.files[u.path]
	if cur == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "target not found"})
		return
	}
	data := make([]byte, u.size)
	copy(data, cur.data)
	for number, partData := range u.parts {
		start := int64(number-1) * u.partSize
		copy(data[start:], partData)
	}
	rev := m.nextRevisionLocked()
	m.files[u.path] = &mockFile{data: data, revision: rev, mode: cur.mode, tags: cloneMap(cur.tags), description: cur.description, mtime: time.Now().Unix()}
	delete(m.uploads, uploadID)
	w.WriteHeader(http.StatusOK)
}

func (m *mockDrive9Server) newUpload(kind, remotePath string, size, partSizeBytes int64) string {
	id := fmt.Sprintf("%s-%d", kind, atomic.AddInt64(&m.uploadID, 1))
	m.mu.Lock()
	m.uploads[id] = &mockUpload{id: id, kind: kind, path: remotePath, size: size, partSize: partSizeBytes, parts: map[int][]byte{}}
	m.mu.Unlock()
	return id
}

func (m *mockDrive9Server) nextRevisionLocked() int64 {
	rev := m.nextRev
	m.nextRev++
	return rev
}

func assembleParts(u *mockUpload) []byte {
	data := make([]byte, 0, u.size)
	for i := 1; int64(len(data)) < u.size; i++ {
		data = append(data, u.parts[i]...)
	}
	if int64(len(data)) > u.size {
		data = data[:u.size]
	}
	return data
}

func partSize(total, standard int64, partNumber int) int64 {
	if standard <= 0 {
		standard = total
	}
	start := int64(partNumber-1) * standard
	if start >= total {
		return 0
	}
	remaining := total - start
	if remaining < standard {
		return remaining
	}
	return standard
}

func parseMockPart(requestPath, prefix string) (string, int, bool) {
	rest := strings.TrimPrefix(requestPath, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 2 {
		return "", 0, false
	}
	n, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, false
	}
	return parts[0], n, true
}

func hasQuery(q url.Values, key string) bool {
	_, ok := q[key]
	return ok
}

func parseIntHeader(value string) (int64, bool) {
	if strings.TrimSpace(value) == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(value, 10, 64)
	return n, err == nil
}

func parseTagHeaders(values []string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, value := range values {
		k, v, ok := strings.Cut(value, "=")
		if ok {
			out[k] = v
		}
	}
	return out
}

func cloneMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func baseName(p string) string {
	return path.Base(strings.TrimSuffix(p, "/"))
}
