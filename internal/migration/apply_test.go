package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type applyRemote struct {
	mu             sync.Mutex
	exists         bool
	revision       int64
	resourceID     string
	mode           uint32
	body           []byte
	expected       []string
	operations     []string
	putStatus      int
	postHeadErr    int
	chmodStatus    int
	afterPut       func()
	checksum       string
	nlink          uint32
	nlinkAfterHead uint32
	nlinkOnPut     uint32
}

func (r *applyRemote) handler(w http.ResponseWriter, request *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	remote := strings.TrimPrefix(request.URL.Path, "/v1/fs/data")
	switch {
	case request.Method == http.MethodHead:
		r.operations = append(r.operations, "stat "+remote)
		if !r.exists {
			http.NotFound(w, request)
			return
		}
		if r.postHeadErr > 0 {
			status := r.postHeadErr
			r.postHeadErr = 0
			http.Error(w, "injected reread failure", status)
			return
		}
		sum := sha256.Sum256(r.body)
		w.Header().Set("Content-Length", fmt.Sprint(len(r.body)))
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", fmt.Sprint(r.revision))
		w.Header().Set("X-Dat9-Resource-ID", r.resourceID)
		w.Header().Set("X-Dat9-Mode", fmt.Sprint(r.mode))
		nlink := r.nlink
		if nlink == 0 {
			nlink = 1
		}
		w.Header().Set("X-Dat9-Nlink", fmt.Sprint(nlink))
		if r.nlinkAfterHead != 0 {
			r.nlink = r.nlinkAfterHead
			r.nlinkAfterHead = 0
		}
		checksum := r.checksum
		if checksum == "" {
			checksum = hex.EncodeToString(sum[:])
		}
		w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPut:
		r.operations = append(r.operations, "write "+remote)
		r.expected = append(r.expected, request.Header.Get("X-Dat9-Expected-Revision"))
		if r.putStatus != 0 {
			http.Error(w, "injected write failure", r.putStatus)
			return
		}
		if r.nlinkOnPut != 0 {
			r.nlink = r.nlinkOnPut
			r.nlinkOnPut = 0
		}
		body, _ := io.ReadAll(request.Body)
		r.body = body
		if r.afterPut != nil {
			r.afterPut()
		}
		r.exists = true
		r.revision++
		if r.resourceID == "" {
			r.resourceID = "resource-file"
		}
		if r.mode == 0 {
			r.mode = 0o644
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": r.revision})
	case request.Method == http.MethodPost && request.URL.Query().Has("mkdir"):
		r.operations = append(r.operations, "mkdir "+remote)
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && request.URL.Query().Has("hardlink"):
		r.operations = append(r.operations, "hardlink "+remote)
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && request.URL.Query().Has("symlink"):
		r.operations = append(r.operations, "symlink "+remote)
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && request.URL.Query().Has("chmod"):
		r.operations = append(r.operations, "chmod "+remote)
		if r.chmodStatus != 0 {
			status := r.chmodStatus
			r.chmodStatus = 0
			http.Error(w, "injected chmod failure", status)
			return
		}
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodDelete:
		r.operations = append(r.operations, "delete "+remote)
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, request)
	}
}

func newApplyFixture(t *testing.T, body string) (*Scanner, SourceEntry) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return scanner, scan.Entries["/file"]
}

func newTestApplyEngine(t *testing.T, server *httptest.Server, scanner *Scanner, threshold int64) *ApplyEngine {
	t.Helper()
	api := client.New(server.URL, "test-key")
	api.SetSmallFileThresholdForTests(threshold)
	engine, err := NewApplyEngine(api, scanner, ApplyConfig{
		Prefix: "/data", Phase: PhaseSyncing, SmallFileThreshold: threshold,
		SmallWorkers: 2, LargeWorkers: 2, MaxBytesPerSecond: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func TestApplyRegularUsesConditionalCreateAndUpdate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		remote     *applyRemote
		target     map[string]TargetEntry
		wantExpect string
	}{
		{name: "create", remote: &applyRemote{}, target: map[string]TargetEntry{}, wantExpect: "0"},
		{name: "update", remote: &applyRemote{exists: true, revision: 7, resourceID: "resource-file", mode: 0o644, body: []byte("old")}, target: map[string]TargetEntry{
			"/file": {Path: "/file", Kind: EntryRegular, Size: 3, Mode: 0o644, HasMode: true, Revision: 7, ResourceID: "resource-file", Nlink: 1},
		}, wantExpect: "7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := tc.remote
			server := httptest.NewServer(http.HandlerFunc(remote.handler))
			defer server.Close()
			scanner, source := newApplyFixture(t, "new-content")
			if err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, tc.target); err != nil {
				t.Fatal(err)
			}
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if len(remote.expected) != 1 || remote.expected[0] != tc.wantExpect || string(remote.body) != "new-content" {
				t.Fatalf("expected=%v body=%q", remote.expected, remote.body)
			}
		})
	}
}

func TestApplyRegularRejectsTargetNlinkChangeBeforeWrite(t *testing.T) {
	remote := &applyRemote{exists: true, revision: 7, resourceID: "resource-file", mode: 0o644, body: []byte("old"), nlink: 2}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	scanner, source := newApplyFixture(t, "new-content")
	target := map[string]TargetEntry{
		"/file": {Path: "/file", Kind: EntryRegular, Size: 3, Mode: 0o644, HasMode: true, Revision: 7, ResourceID: "resource-file", Nlink: 1},
	}

	err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, target)
	if !errors.Is(err, ErrApplyRescan) {
		t.Fatalf("Nlink drift error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("Nlink drift reached write: %v", remote.operations)
	}
}

func TestApplyRegularRejectsUnaccountedStableTargetNlinkBeforeWrite(t *testing.T) {
	remote := &applyRemote{exists: true, revision: 7, resourceID: "resource-file", mode: 0o644, body: []byte("old"), nlink: 2}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	scanner, source := newApplyFixture(t, "new-content")
	target := map[string]TargetEntry{
		"/file": {Path: "/file", Kind: EntryRegular, Size: 3, Mode: 0o644, HasMode: true, Revision: 7, ResourceID: "resource-file", Nlink: 2},
	}

	err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, target)
	if !errors.Is(err, ErrUnsafeApply) {
		t.Fatalf("unaccounted Nlink error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("unaccounted Nlink reached write: %v", remote.operations)
	}
}

func TestApplyRegularRevalidatesTargetLinksImmediatelyBeforeWrite(t *testing.T) {
	remote := &applyRemote{
		exists: true, revision: 7, resourceID: "resource-file", mode: 0o644,
		body: []byte("old"), nlink: 1, nlinkAfterHead: 2,
	}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	scanner, source := newApplyFixture(t, "new-content")
	target := map[string]TargetEntry{
		"/file": {Path: "/file", Kind: EntryRegular, Size: 3, Mode: 0o644, HasMode: true, Revision: 7, ResourceID: "resource-file", Nlink: 1},
	}

	err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, target)
	if !errors.Is(err, ErrApplyRescan) {
		t.Fatalf("pre-write target link race error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("pre-write target link race reached write: %v", remote.operations)
	}
}

func TestApplyRegularRejectsTargetLinkRaceDuringCommit(t *testing.T) {
	remote := &applyRemote{
		exists: true, revision: 7, resourceID: "resource-file", mode: 0o644,
		body: []byte("old"), nlink: 1, nlinkOnPut: 2,
	}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	scanner, source := newApplyFixture(t, "new-content")
	target := map[string]TargetEntry{
		"/file": {Path: "/file", Kind: EntryRegular, Size: 3, Mode: 0o644, HasMode: true, Revision: 7, ResourceID: "resource-file", Nlink: 1},
	}

	err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, target)
	if !errors.Is(err, ErrApplyRescan) {
		t.Fatalf("in-commit target link race error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if !containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("in-commit target link race did not reach write: %v", remote.operations)
	}
}

func TestApplyHardlinkPrimaryRejectsUnaccountedStableTargetNlink(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("new-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "a"), filepath.Join(root, "b")); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	source, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	remote := &applyRemote{exists: true, revision: 7, resourceID: "resource-hardlink", mode: 0o644, body: []byte("old"), nlink: 2}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	target := map[string]TargetEntry{
		"/a": {Path: "/a", Kind: EntryRegular, Size: 3, Mode: 0o644, HasMode: true, Revision: 7, ResourceID: "resource-hardlink", Nlink: 2},
	}

	err = newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), source.Entries, target)
	if !errors.Is(err, ErrUnsafeApply) {
		t.Fatalf("partial hardlink ownership error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("partial hardlink ownership reached write: %v", remote.operations)
	}
}

func TestApplyReportsOnlyActualConditionalCommitAttempts(t *testing.T) {
	for _, tc := range []struct {
		name           string
		threshold      int64
		directStatus   int
		partStatus     int
		completeStatus int
		wantCAS        int32
	}{
		{name: "direct PUT failure", threshold: 1024, directStatus: http.StatusServiceUnavailable, wantCAS: 1},
		{name: "part upload failure", threshold: 1, partStatus: http.StatusServiceUnavailable, completeStatus: http.StatusOK, wantCAS: 0},
		{name: "Complete failure", threshold: 1, partStatus: http.StatusOK, completeStatus: http.StatusServiceUnavailable, wantCAS: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var casCalls, completeCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch {
				case request.Method == http.MethodHead && request.URL.Path == "/v1/fs/data/file":
					http.NotFound(w, request)
				case request.Method == http.MethodPut && request.URL.Path == "/v1/fs/data/file":
					http.Error(w, "injected direct PUT failure", tc.directStatus)
				case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/initiate":
					w.WriteHeader(http.StatusAccepted)
					_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "upload", "part_size": 8, "total_parts": 1})
				case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/upload/presign-batch":
					_ = json.NewEncoder(w).Encode(map[string]any{"parts": []map[string]any{{
						"number": 1, "url": "http://" + request.Host + "/part", "size": 8, "expires_at": time.Now().Add(time.Minute),
					}}})
				case request.Method == http.MethodPut && request.URL.Path == "/part":
					if tc.partStatus >= http.StatusMultipleChoices {
						http.Error(w, "injected part failure", tc.partStatus)
						return
					}
					w.Header().Set("ETag", `"etag"`)
					w.WriteHeader(tc.partStatus)
				case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/upload/complete":
					completeCalls.Add(1)
					http.Error(w, "injected Complete failure", tc.completeStatus)
				case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/upload/abort":
					w.WriteHeader(http.StatusNoContent)
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()

			scanner, source := newApplyFixture(t, "12345678")
			engine := newTestApplyEngine(t, server, scanner, tc.threshold)
			engine.onCAS = func(SourceEntry, *client.StatResult, int64, time.Time, error) {
				casCalls.Add(1)
			}
			if err := engine.Apply(context.Background(), map[string]SourceEntry{source.Path: source}, nil); err == nil {
				t.Fatal("Apply unexpectedly succeeded")
			}
			if got := casCalls.Load(); got != tc.wantCAS {
				t.Fatalf("CAS reports=%d, want %d; Complete calls=%d", got, tc.wantCAS, completeCalls.Load())
			}
		})
	}
}

func TestApplyConflictAndSourceMutationRequestRescan(t *testing.T) {
	t.Run("CAS conflict", func(t *testing.T) {
		remote := &applyRemote{putStatus: http.StatusConflict}
		server := httptest.NewServer(http.HandlerFunc(remote.handler))
		defer server.Close()
		scanner, source := newApplyFixture(t, "content")
		err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
		var statusErr *client.StatusError
		if !errors.Is(err, ErrApplyRescan) || !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusConflict {
			t.Fatalf("error=%T %v", err, err)
		}
	})

	t.Run("source changes between hash and open", func(t *testing.T) {
		scanner, source := newApplyFixture(t, "content")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodHead {
				_ = os.WriteFile(filepath.Join(scanner.root, "file"), []byte("changed"), 0o644)
				http.NotFound(w, request)
				return
			}
			t.Error("upload reached after source changed")
		}))
		defer server.Close()
		err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
		if !errors.Is(err, ErrSourceChanged) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("source changes while hashing", func(t *testing.T) {
		var hits int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(http.StatusNotFound) }))
		defer server.Close()
		scanner, source := newApplyFixture(t, "content")
		scanner.afterRead = func(string) {
			_ = os.WriteFile(filepath.Join(scanner.root, "file"), []byte("changed"), 0o644)
		}
		err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
		if !errors.Is(err, ErrSourceChanged) || hits != 0 {
			t.Fatalf("error=%v remote hits=%d", err, hits)
		}
	})

	t.Run("source changes while uploading", func(t *testing.T) {
		scanner, source := newApplyFixture(t, "content")
		remote := &applyRemote{afterPut: func() {
			_ = os.WriteFile(filepath.Join(scanner.root, "file"), []byte("changed"), 0o644)
		}}
		server := httptest.NewServer(http.HandlerFunc(remote.handler))
		defer server.Close()
		err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
		if !errors.Is(err, ErrSourceChanged) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestApplyChmodNotFoundRequestsRescan(t *testing.T) {
	remote := &applyRemote{chmodStatus: http.StatusNotFound}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	scanner, source := newApplyFixture(t, "content")

	err := newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
	var statusErr *client.StatusError
	if !errors.Is(err, ErrApplyRescan) || !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("error=%T %v", err, err)
	}
}

func TestApplyCurrentTargetChangeMatrix(t *testing.T) {
	observed := TargetEntry{Path: "/file", Kind: EntryRegular, Mode: 0o644, HasMode: true, Revision: 1, ResourceID: "file"}
	for _, tc := range []struct {
		name, headers string
		status        int
		exists        bool
		wantRescan    bool
	}{
		{name: "appeared", headers: "valid", status: http.StatusOK, wantRescan: true},
		{name: "disappeared", status: http.StatusNotFound, exists: true, wantRescan: true},
		{name: "revision changed", headers: "revision-two", status: http.StatusOK, exists: true, wantRescan: true},
		{name: "server error", status: http.StatusServiceUnavailable, exists: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.headers != "" {
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Mode", fmt.Sprint(uint32(0o100644)))
					w.Header().Set("X-Dat9-Revision", "1")
					w.Header().Set("X-Dat9-Resource-ID", "file")
					if tc.headers == "revision-two" {
						w.Header().Set("X-Dat9-Revision", "2")
					}
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			scanner, _ := newApplyFixture(t, "content")
			engine := newTestApplyEngine(t, server, scanner, 1<<20)
			_, _, err := engine.currentTarget(context.Background(), "/data/file", observed, tc.exists)
			if errors.Is(err, ErrApplyRescan) != tc.wantRescan {
				t.Fatalf("error=%v", err)
			}
			if !tc.wantRescan {
				var statusErr *client.StatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != tc.status {
					t.Fatalf("error=%T %v", err, err)
				}
			}
		})
	}
}

func TestApplySourceStabilityHelpersFailClosed(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	scanner, source := newApplyFixture(t, "content")
	engine := newTestApplyEngine(t, server, scanner, 1<<20)
	if _, err := engine.openSource(SourceEntry{Path: "/missing", Version: source.Version}); err == nil {
		t.Fatal("missing source opened")
	}
	if _, err := engine.openSource(SourceEntry{Path: "../escape", Version: source.Version}); !errors.Is(err, ErrUnsafeSourcePath) {
		t.Fatalf("unsafe open error=%v", err)
	}
	if err := engine.validateSource("/missing", source.Version); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("missing validation error=%v", err)
	}
	if err := engine.validateSource("../escape", source.Version); !errors.Is(err, ErrUnsafeSourcePath) {
		t.Fatalf("unsafe validation error=%v", err)
	}
	originalIdentity := scanner.identity
	scanner.identity = func(string, os.FileInfo) (fileIdentity, error) {
		return fileIdentity{}, errors.New("injected identity failure")
	}
	if err := engine.validateSource("/file", source.Version); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("identity validation error=%v", err)
	}
	scanner.identity = originalIdentity
}

func TestApplyCreateOnlyConflictsRequestRescan(t *testing.T) {
	for _, kind := range []EntryKind{EntryDirectory, EntrySymlink} {
		t.Run(string(kind), func(t *testing.T) {
			root := t.TempDir()
			name := "dir"
			if kind == EntryDirectory {
				if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
					t.Fatal(err)
				}
			} else {
				name = "link"
				if err := os.Symlink("target", filepath.Join(root, name)); err != nil {
					t.Fatal(err)
				}
			}
			scanner, _ := NewScanner(root)
			scan, err := scanner.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			entry := scan.Entries["/"+name]
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "conflict", http.StatusConflict)
			}))
			defer server.Close()
			err = newTestApplyEngine(t, server, scanner, 1<<20).Apply(context.Background(), map[string]SourceEntry{entry.Path: entry}, nil)
			if !errors.Is(err, ErrApplyRescan) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestApplyPostUploadRereadFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		remote   *applyRemote
		wantKind error
	}{
		{name: "reread unavailable", remote: &applyRemote{postHeadErr: http.StatusServiceUnavailable}},
		{name: "checksum mismatch", remote: &applyRemote{checksum: strings.Repeat("f", 64)}, wantKind: ErrApplyVerification},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := tc.remote
			server := httptest.NewServer(http.HandlerFunc(remote.handler))
			defer server.Close()
			scanner, source := newApplyFixture(t, "content")
			engine := newTestApplyEngine(t, server, scanner, 1<<20)
			err := engine.Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
			if tc.wantKind != nil && !errors.Is(err, tc.wantKind) {
				t.Fatalf("error=%v", err)
			}
			if tc.wantKind == nil {
				var statusErr *client.StatusError
				if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("error=%T %v", err, err)
				}
				if err := engine.Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil); !errors.Is(err, ErrApplyRescan) {
					t.Fatalf("retry error=%v", err)
				}
			}
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if len(remote.expected) != 1 {
				t.Fatalf("unsafe retry writes=%v", remote.expected)
			}
		})
	}
}

type multipartApplyRemote struct {
	t              *testing.T
	mu             sync.Mutex
	fail           string
	initiates      int
	committed      bool
	expected       []int64
	parts          map[string]map[int]int
	checksum       string
	resumeHits     int
	nlink          uint32
	linkOnComplete bool
	completeHits   int
}

func (r *multipartApplyRemote) handler(w http.ResponseWriter, request *http.Request) {
	pathParts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	switch {
	case request.Method == http.MethodHead && request.URL.Path == "/v1/fs/data/file":
		r.mu.Lock()
		committed := r.committed
		nlink := r.nlink
		r.mu.Unlock()
		if !committed {
			http.NotFound(w, request)
			return
		}
		if nlink == 0 {
			nlink = 1
		}
		w.Header().Set("Content-Length", "8")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "1")
		w.Header().Set("X-Dat9-Resource-ID", "big-file")
		w.Header().Set("X-Dat9-Mode", fmt.Sprint(uint32(0o100644)))
		w.Header().Set("X-Dat9-Nlink", fmt.Sprint(nlink))
		w.Header().Set("X-Dat9-Checksum-SHA256", r.checksum)
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/initiate":
		var body struct {
			Expected *int64 `json:"expected_revision"`
		}
		_ = json.NewDecoder(request.Body).Decode(&body)
		if body.Expected == nil {
			r.t.Error("multipart initiate omitted expected revision")
			body.Expected = new(int64)
		}
		r.mu.Lock()
		r.initiates++
		id := fmt.Sprintf("upload-%d", r.initiates)
		r.expected = append(r.expected, *body.Expected)
		r.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": id, "part_size": 4, "total_parts": 2})
	case request.Method == http.MethodPost && len(pathParts) == 4 && pathParts[0] == "v2" && pathParts[3] == "presign-batch":
		id := pathParts[2]
		_ = json.NewEncoder(w).Encode(map[string]any{"parts": []map[string]any{
			{"number": 1, "url": "http://" + request.Host + "/parts/" + id + "/1", "size": 4, "expires_at": time.Now().Add(time.Minute)},
			{"number": 2, "url": "http://" + request.Host + "/parts/" + id + "/2", "size": 4, "expires_at": time.Now().Add(time.Minute)},
		}})
	case request.Method == http.MethodPut && len(pathParts) == 3 && pathParts[0] == "parts":
		id := pathParts[1]
		var part int
		_, _ = fmt.Sscanf(pathParts[2], "%d", &part)
		_, _ = io.Copy(io.Discard, request.Body)
		r.mu.Lock()
		if r.parts[id] == nil {
			r.parts[id] = make(map[int]int)
		}
		r.parts[id][part]++
		fail := r.fail == "part" && id == "upload-1" && part == 1
		r.mu.Unlock()
		if fail {
			http.Error(w, "injected part failure", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, part))
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && len(pathParts) == 4 && pathParts[0] == "v2" && pathParts[3] == "complete":
		id := pathParts[2]
		if r.fail == "complete" && id == "upload-1" {
			http.Error(w, "injected complete failure", http.StatusServiceUnavailable)
			return
		}
		r.mu.Lock()
		r.committed = true
		r.completeHits++
		if r.linkOnComplete {
			r.nlink = 2
		}
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && len(pathParts) == 4 && pathParts[0] == "v2" && pathParts[3] == "abort":
		w.WriteHeader(http.StatusNoContent)
	case request.Method == http.MethodPost && request.URL.Path == "/v1/fs/data/file" && request.URL.Query().Has("chmod"):
		w.WriteHeader(http.StatusOK)
	case strings.HasPrefix(request.URL.Path, "/v1/uploads"):
		r.mu.Lock()
		r.resumeHits++
		r.mu.Unlock()
		http.Error(w, "resume forbidden", http.StatusInternalServerError)
	default:
		http.NotFound(w, request)
	}
}

func TestApplyMultipartFailureStartsFreshAttempt(t *testing.T) {
	for _, failure := range []string{"part", "complete"} {
		t.Run(failure, func(t *testing.T) {
			scanner, source := newApplyFixture(t, "12345678")
			checksum := sha256.Sum256([]byte("12345678"))
			remote := &multipartApplyRemote{
				t: t, fail: failure, parts: make(map[string]map[int]int), checksum: hex.EncodeToString(checksum[:]),
			}
			server := httptest.NewServer(http.HandlerFunc(remote.handler))
			defer server.Close()
			engine := newTestApplyEngine(t, server, scanner, 1)
			sourceMap := map[string]SourceEntry{"/file": source}
			if err := engine.Apply(context.Background(), sourceMap, nil); err == nil {
				t.Fatal("injected multipart failure succeeded")
			}
			if err := engine.Apply(context.Background(), sourceMap, nil); err != nil {
				t.Fatalf("fresh retry: %v", err)
			}
			remote.mu.Lock()
			defer remote.mu.Unlock()
			if remote.initiates != 2 || len(remote.expected) != 2 || remote.expected[0] != 0 || remote.expected[1] != 0 || remote.resumeHits != 0 {
				t.Fatalf("initiates=%d expected=%v resume=%d", remote.initiates, remote.expected, remote.resumeHits)
			}
			if remote.parts["upload-2"][1] != 1 || remote.parts["upload-2"][2] != 1 {
				t.Fatalf("fresh upload parts=%v", remote.parts)
			}
		})
	}
}

func TestApplyMultipartRejectsTargetLinkRaceDuringCommit(t *testing.T) {
	scanner, source := newApplyFixture(t, "12345678")
	checksum := sha256.Sum256([]byte("12345678"))
	remote := &multipartApplyRemote{
		t: t, parts: make(map[string]map[int]int), checksum: hex.EncodeToString(checksum[:]), linkOnComplete: true,
	}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()

	err := newTestApplyEngine(t, server, scanner, 1).Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
	if !errors.Is(err, ErrApplyRescan) {
		t.Fatalf("multipart in-commit target link race error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if remote.completeHits != 1 || !remote.committed {
		t.Fatalf("multipart commit hits=%d committed=%v", remote.completeHits, remote.committed)
	}
}

func TestApplyMultipartSourceMutationCannotCommitOrFalseConverge(t *testing.T) {
	const (
		partSize  = 4
		partCount = 17
	)
	original := []byte(strings.Repeat("A", partSize*partCount))
	changed := []byte(strings.Repeat("B", partSize*partCount))
	scanner, source := newApplyFixture(t, string(original))
	filePath := filepath.Join(scanner.root, "file")
	originalSum := sha256.Sum256(original)

	var mu sync.Mutex
	parts := make(map[int][]byte)
	started := 0
	committed := false
	completeHits := 0
	abortHits := 0
	targetChecksum := ""
	release := make(chan struct{})
	var releaseOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		segments := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		switch {
		case request.Method == http.MethodHead && request.URL.Path == "/v1/fs/data/file":
			mu.Lock()
			exists := committed
			checksum := targetChecksum
			mu.Unlock()
			if !exists {
				http.NotFound(w, request)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(original)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Resource-ID", "unstable-file")
			w.Header().Set("X-Dat9-Mode", fmt.Sprint(uint32(0o100644)))
			w.Header().Set("X-Dat9-Checksum-SHA256", checksum)
			w.WriteHeader(http.StatusOK)
		case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "unstable", "part_size": partSize, "total_parts": partCount})
		case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/unstable/presign-batch":
			var requested struct {
				Parts []struct {
					PartNumber int `json:"part_number"`
				} `json:"parts"`
			}
			if err := json.NewDecoder(request.Body).Decode(&requested); err != nil {
				http.Error(w, "bad presign request", http.StatusBadRequest)
				return
			}
			presigned := make([]map[string]any, 0, len(requested.Parts))
			for _, requestedPart := range requested.Parts {
				part := requestedPart.PartNumber
				presigned = append(presigned, map[string]any{
					"number": part, "url": fmt.Sprintf("http://%s/parts/%d", request.Host, part),
					"size": partSize, "expires_at": time.Now().Add(time.Minute),
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": presigned})
		case request.Method == http.MethodPut && len(segments) == 2 && segments[0] == "parts":
			var part int
			_, _ = fmt.Sscanf(segments[1], "%d", &part)
			body, _ := io.ReadAll(request.Body)
			mu.Lock()
			parts[part] = append([]byte(nil), body...)
			started++
			shouldMutate := started == 16
			mu.Unlock()
			if shouldMutate {
				if err := os.WriteFile(filePath, changed, 0o644); err != nil {
					t.Error(err)
				}
				if err := os.Chmod(filePath, 0o600); err != nil {
					t.Error(err)
				}
				releaseOnce.Do(func() { close(release) })
			}
			<-release
			w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, part))
			w.WriteHeader(http.StatusOK)
		case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/unstable/complete":
			var body struct {
				Checksum string `json:"checksum_sha256"`
			}
			_ = json.NewDecoder(request.Body).Decode(&body)
			mu.Lock()
			completeHits++
			committed = true
			targetChecksum = body.Checksum
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case request.Method == http.MethodPost && request.URL.Path == "/v2/uploads/unstable/abort":
			mu.Lock()
			abortHits++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, request)
		}
	}))
	defer srv.Close()

	engine := newTestApplyEngine(t, srv, scanner, 1)
	var casCalls atomic.Int32
	engine.onCAS = func(SourceEntry, *client.StatResult, int64, time.Time, error) {
		casCalls.Add(1)
	}
	err := engine.Apply(context.Background(), map[string]SourceEntry{"/file": source}, nil)
	if !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("error=%v, want ErrSourceChanged", err)
	}
	if err := os.WriteFile(filePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filePath, 0o644); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	wasCommitted := committed
	gotCompleteHits := completeHits
	gotAbortHits := abortHits
	uploadedParts := len(parts)
	uploaded := make([]byte, 0, len(original))
	for part := 1; part <= partCount; part++ {
		uploaded = append(uploaded, parts[part]...)
	}
	mu.Unlock()
	if uploadedParts != partCount || len(uploaded) != len(original) || bytes.Equal(uploaded, original) {
		t.Fatalf("fixture parts=%d bytes=%d mutated=%v", uploadedParts, len(uploaded), !bytes.Equal(uploaded, original))
	}
	if wasCommitted || gotCompleteHits != 0 || gotAbortHits != 1 || casCalls.Load() != 0 {
		t.Fatalf("committed=%v complete=%d abort=%d CAS reports=%d", wasCommitted, gotCompleteHits, gotAbortHits, casCalls.Load())
	}

	restored, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restoredEntry := restored.Entries["/file"]
	deep, err := scanner.ReadStableEntry(context.Background(), restoredEntry)
	if err != nil {
		t.Fatal(err)
	}
	if deep.ChecksumSHA256 != hex.EncodeToString(originalSum[:]) {
		t.Fatalf("restored checksum=%s", deep.ChecksumSHA256)
	}
	restoredEntry.ChecksumSHA256 = deep.ChecksumSHA256
	restored.Entries["/file"] = restoredEntry
	round, err := BuildRound("verification", RoundModeVerification, time.Now(), restored, TargetScan{Complete: true, Entries: map[string]TargetEntry{}})
	if err != nil {
		t.Fatal(err)
	}
	if round.Converged || !hasFindingAt(round.Findings, "/file", FindingSourceOnly) {
		t.Fatalf("unstable attempt falsely converged: %+v", round)
	}
}

func TestApplyOrdersNamespaceAndUsesOneLimiter(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "d"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "d", "a"), []byte("body"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "d", "a"), filepath.Join(root, "d", "b")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("d/a", filepath.Join(root, "z")); err != nil {
		t.Fatal(err)
	}
	scanner, _ := NewScanner(root)
	scan, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	remote := &applyRemote{}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	engine := newTestApplyEngine(t, server, scanner, 1<<20)
	target := map[string]TargetEntry{
		"/old":       {Path: "/old", Kind: EntryDirectory, ResourceID: "old", HasMode: true, Mode: 0o755},
		"/old/child": {Path: "/old/child", Kind: EntryRegular, Revision: 1, ResourceID: "child", HasMode: true, Mode: 0o644},
	}
	if err := engine.Apply(context.Background(), scan.Entries, target); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	operations := append([]string(nil), remote.operations...)
	remote.mu.Unlock()
	positions := map[string]int{}
	for i, operation := range operations {
		for _, prefix := range []string{"mkdir ", "write ", "hardlink ", "symlink ", "chmod /d", "delete "} {
			if strings.HasPrefix(operation, prefix) {
				if _, exists := positions[prefix]; !exists {
					positions[prefix] = i
				}
			}
		}
	}
	order := []string{"mkdir ", "write ", "hardlink ", "symlink ", "chmod /d", "delete "}
	if len(positions) != len(order) {
		t.Fatalf("missing operation: operations=%v positions=%v", operations, positions)
	}
	for i := 1; i < len(order); i++ {
		if positions[order[i-1]] >= positions[order[i]] {
			t.Fatalf("operation order=%v positions=%v", operations, positions)
		}
	}
	childDelete, parentDelete := -1, -1
	for i, operation := range operations {
		if operation == "delete /old/child" {
			childDelete = i
		}
		if operation == "delete /old/" {
			parentDelete = i
		}
	}
	if childDelete < 0 || parentDelete < 0 || childDelete >= parentDelete {
		t.Fatalf("delete order=%v", operations)
	}
	if engine.limiterForSize(1) != engine.limiterForSize(1<<20) {
		t.Fatal("small and large pools do not share one limiter")
	}
}

func TestApplyDualWriteSkipsDeleteAndBlocksUnsafeLinkReplacement(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("target", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	scanner, _ := NewScanner(root)
	scan, _ := scanner.Scan(context.Background())
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	api := client.New(server.URL, "key")
	engine, err := NewApplyEngine(api, scanner, ApplyConfig{
		Prefix: "/data", Phase: PhaseDualWriteRepairing, SmallFileThreshold: 10,
		SmallWorkers: 1, LargeWorkers: 1, MaxBytesPerSecond: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := map[string]TargetEntry{
		"/link":  {Path: "/link", Kind: EntrySymlink, Mode: 0o777, HasMode: true, Revision: 1, ResourceID: "link", ChecksumSHA256: strings.Repeat("f", 64)},
		"/extra": {Path: "/extra", Kind: EntryRegular, Mode: 0o644, HasMode: true, Revision: 1, ResourceID: "extra"},
	}
	if err := engine.Apply(context.Background(), scan.Entries, target); !errors.Is(err, ErrUnsafeApply) || hits != 0 {
		t.Fatalf("error=%v remote hits=%d", err, hits)
	}
	if err := engine.Apply(context.Background(), map[string]SourceEntry{}, map[string]TargetEntry{"/extra": target["/extra"]}); err != nil || hits != 0 {
		t.Fatalf("target-only residue error=%v hits=%d", err, hits)
	}
}

func TestApplyDualHardlinkAliasUsesPrimaryOutsideMutationSet(t *testing.T) {
	root := t.TempDir()
	primaryPath := filepath.Join(root, "a-primary")
	if err := os.WriteFile(primaryPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(primaryPath, filepath.Join(root, "z-alias")); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	primary, alias := manifest.Entries["/a-primary"], manifest.Entries["/z-alias"]
	deep, err := scanner.ReadStableEntry(context.Background(), primary)
	if err != nil {
		t.Fatal(err)
	}
	primary.ChecksumSHA256 = deep.ChecksumSHA256
	manifest.Entries[primary.Path] = primary
	remote := &applyRemote{exists: true, revision: 4, resourceID: "hardlink-resource", mode: 0o644, body: []byte("content")}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	api := client.New(server.URL, "key")
	api.SetSmallFileThresholdForTests(1 << 20)
	engine, err := NewApplyEngine(api, scanner, ApplyConfig{
		Prefix: "/data", Phase: PhaseDualWriteRepairing, SmallFileThreshold: 1 << 20,
		SmallWorkers: 1, LargeWorkers: 1, MaxBytesPerSecond: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := map[string]TargetEntry{
		primary.Path: {Path: primary.Path, Kind: EntryRegular, Size: deep.Size, Mode: 0o644, HasMode: true, Revision: 4, ResourceID: "hardlink-resource", Nlink: 1, ChecksumSHA256: deep.ChecksumSHA256},
	}
	if err := engine.ApplyWithManifest(context.Background(), map[string]SourceEntry{alias.Path: alias}, manifest.Entries, target); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if !containsOperation(remote.operations, "hardlink /z-alias") || containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("operations=%v", remote.operations)
	}
}

func TestApplyDualHardlinkAliasRejectsUnownedPrimary(t *testing.T) {
	root := t.TempDir()
	primaryPath := filepath.Join(root, "a-primary")
	if err := os.WriteFile(primaryPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(primaryPath, filepath.Join(root, "z-alias")); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	primary, alias := manifest.Entries["/a-primary"], manifest.Entries["/z-alias"]
	deep, err := scanner.ReadStableEntry(context.Background(), primary)
	if err != nil {
		t.Fatal(err)
	}
	primary.ChecksumSHA256 = deep.ChecksumSHA256
	manifest.Entries[primary.Path] = primary
	remote := &applyRemote{exists: true, revision: 4, resourceID: "hardlink-resource", mode: 0o644, body: []byte("content"), nlink: 2}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	api := client.New(server.URL, "key")
	api.SetSmallFileThresholdForTests(1 << 20)
	engine, err := NewApplyEngine(api, scanner, ApplyConfig{
		Prefix: "/data", Phase: PhaseDualWriteRepairing, SmallFileThreshold: 1 << 20,
		SmallWorkers: 1, LargeWorkers: 1, MaxBytesPerSecond: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := map[string]TargetEntry{
		primary.Path: {Path: primary.Path, Kind: EntryRegular, Size: deep.Size, Mode: 0o644, HasMode: true, Revision: 4, ResourceID: "hardlink-resource", Nlink: 2, ChecksumSHA256: deep.ChecksumSHA256},
	}

	err = engine.ApplyWithManifest(context.Background(), map[string]SourceEntry{alias.Path: alias}, manifest.Entries, target)
	if !errors.Is(err, ErrUnsafeApply) {
		t.Fatalf("unowned hardlink primary error=%v", err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if containsOperationPrefix(remote.operations, "hardlink ") {
		t.Fatalf("unowned hardlink primary reached link creation: %v", remote.operations)
	}
}

func TestApplyDoesNotReopenSourceThroughEscapedAncestor(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "directory")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "file"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := scanner.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(t.TempDir(), "moved")
	scanner.afterRead = func(string) {
		if err := os.Rename(directory, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(moved, directory); err != nil {
			t.Fatal(err)
		}
		scanner.afterRead = nil
	}
	remote := &applyRemote{}
	server := httptest.NewServer(http.HandlerFunc(remote.handler))
	defer server.Close()
	engine := newTestApplyEngine(t, server, scanner, 1<<20)

	err = engine.Apply(context.Background(), manifest.Entries, nil)
	if err == nil {
		t.Fatal("apply uploaded a source reopened through an escaped ancestor")
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if containsOperationPrefix(remote.operations, "write ") {
		t.Fatalf("operations=%v", remote.operations)
	}
}

func containsOperation(operations []string, want string) bool {
	for _, operation := range operations {
		if operation == want {
			return true
		}
	}
	return false
}

func containsOperationPrefix(operations []string, prefix string) bool {
	for _, operation := range operations {
		if strings.HasPrefix(operation, prefix) {
			return true
		}
	}
	return false
}

func TestByteTokenBucketAggregatesReservations(t *testing.T) {
	bucket := newByteTokenBucket(10)
	now := bucket.last
	if delay := bucket.reserveAt(now, 10); delay != 0 {
		t.Fatalf("initial delay=%s", delay)
	}
	if delay := bucket.reserveAt(now, 5); delay != 500*time.Millisecond {
		t.Fatalf("aggregate delay=%s", delay)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := newByteTokenBucket(1).WaitN(ctx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error=%v", err)
	}
}
