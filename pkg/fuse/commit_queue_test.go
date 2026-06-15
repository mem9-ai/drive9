package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommitQueueConditionalCommitSuccess(t *testing.T) {
	var gotExpected string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
		// Return committed revision in JSON (direct PUT response format).
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/ok.txt", []byte("data"), 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/ok.txt", 4, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}

	var successPath string
	var successRev int64

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		successPath = entry.Path
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/ok.txt",
		BaseRev: 7,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if gotExpected != "7" {
		t.Fatalf("expected revision header = %q, want 7", gotExpected)
	}
	if successPath != "/ok.txt" {
		t.Fatalf("OnSuccess path = %q, want /ok.txt", successPath)
	}
	if successRev != 8 {
		t.Fatalf("OnSuccess committedRev = %d, want 8", successRev)
	}
	if pending.HasPending("/ok.txt") {
		t.Fatal("pending entry should be removed after successful commit")
	}
	if shadow.Has("/ok.txt") {
		t.Fatal("shadow should be removed after successful commit")
	}
}

func TestCommitQueueLayerEntryShadowSpillUploadsObject(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 1024)
	var gotPath, gotSize, gotBaseRevision, gotMode string
	var gotBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/objects" {
			http.NotFound(w, r)
			return
		}
		gotPath = r.URL.Query().Get("path")
		gotSize = r.URL.Query().Get("size")
		gotBaseRevision = r.URL.Query().Get("base_revision")
		gotMode = r.URL.Query().Get("mode")
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"layer_id":   "layer-1",
			"path":       "/remote/spill.bin",
			"op":         "upsert",
			"kind":       "file",
			"size_bytes": len(payload),
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/spill.bin", payload, 7); err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, nil, nil, 1, 8)
	_, err = cq.uploadLayerEntry(context.Background(), "layer-1", &CommitEntry{
		Path:        "/spill.bin",
		BaseRev:     7,
		Size:        int64(len(payload)),
		Kind:        PendingOverwrite,
		ShadowSpill: true,
		Mode:        0o640,
		HasMode:     true,
	}, "/remote/spill.bin", 7)
	if err != nil {
		t.Fatalf("uploadLayerEntry: %v", err)
	}
	if gotPath != "/remote/spill.bin" || gotSize != "1024" || gotBaseRevision != "7" || gotMode != "640" {
		t.Fatalf("query path=%q size=%q base=%q mode=%q", gotPath, gotSize, gotBaseRevision, gotMode)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("body mismatch: got %d bytes want %d", len(gotBody), len(payload))
	}
}

func TestCommitQueueBeginInFlightSerializesSamePath(t *testing.T) {
	cq := &CommitQueue{inFlight: make(map[string]*CommitEntry)}
	first := &CommitEntry{Path: "/same.txt"}
	second := &CommitEntry{Path: "/same.txt"}
	other := &CommitEntry{Path: "/other.txt"}

	if !cq.beginInFlight(first) {
		t.Fatal("begin first in-flight returned false")
	}
	if !cq.beginInFlight(other) {
		t.Fatal("begin other-path in-flight returned false")
	}
	cq.endInFlight(other)

	done := make(chan struct{})
	go func() {
		if !cq.beginInFlight(second) {
			t.Errorf("begin second in-flight returned false")
		}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second same-path entry became in-flight before first ended")
	case <-time.After(25 * time.Millisecond):
	}

	cq.endInFlight(first)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second same-path entry did not become in-flight after first ended")
	}
	cq.endInFlight(second)
}

func TestCommitQueueBeginInFlightPreservesSamePathFIFO(t *testing.T) {
	first := &CommitEntry{Path: "/same.txt"}
	second := &CommitEntry{Path: "/same.txt"}
	cq := &CommitQueue{
		queue:        []*CommitEntry{first, second},
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
	}
	cq.rebuildQueuedIndexLocked()

	secondDone := make(chan struct{})
	go func() {
		if !cq.beginInFlight(second) {
			t.Errorf("begin second in-flight returned false")
		}
		close(secondDone)
	}()

	select {
	case <-secondDone:
		t.Fatal("second same-path entry became in-flight before older queued entry")
	case <-time.After(25 * time.Millisecond):
	}

	if !cq.beginInFlight(first) {
		t.Fatal("begin first in-flight returned false")
	}

	select {
	case <-secondDone:
		t.Fatal("second same-path entry became in-flight while first was in-flight")
	case <-time.After(25 * time.Millisecond):
	}

	cq.endInFlight(first)
	cq.removeFromQueue(first)

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second same-path entry did not become in-flight after older entry completed")
	}
	cq.endInFlight(second)
	cq.removeFromQueue(second)
}

func TestCommitQueueCommitNowHoldsPathLockThroughSuccessCleanup(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/locked.txt", []byte("data"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/locked.txt", 4, PendingOverwrite, 8); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	locked := false
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	cq.PathLock = func(path string) func() {
		mu.Lock()
		if path != "/locked.txt" {
			t.Errorf("PathLock path = %q, want /locked.txt", path)
		}
		locked = true
		mu.Unlock()
		return func() {
			mu.Lock()
			locked = false
			mu.Unlock()
		}
	}
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		mu.Lock()
		defer mu.Unlock()
		if !locked {
			t.Error("PathLock was not held during OnSuccess cleanup")
		}
	}

	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path:    "/locked.txt",
		BaseRev: 8,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatalf("CommitNow: %v", err)
	}
}

func TestCommitQueueAppliesModeAfterUpload(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	var chmodBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			chmodBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/exec.sh", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/exec.sh", 4, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/exec.sh",
		Size:    4,
		Kind:    PendingNew,
		Mode:    0o755,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if !bytes.Contains(chmodBody, []byte("493")) {
		t.Fatalf("chmod body = %s, want decimal mode 493", chmodBody)
	}
}

func TestCommitQueueLayerUploadWritesEntryAndKeepsPending(t *testing.T) {
	var gotReq clientLayerEntryRequest
	var gotPath string
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"layer_id":      "layer-1",
			"path":          gotReq.Path,
			"op":            gotReq.Op,
			"kind":          gotReq.Kind,
			"base_revision": gotReq.BaseRevision,
			"size_bytes":    gotReq.SizeBytes,
			"mode":          gotReq.Mode,
			"entry_seq":     1,
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/ok.txt", []byte("data"), 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/ok.txt", 4, PendingOverwrite, 7, 0o600, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8, "/remote")
	cq.SetLayerRef("layer-1")
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/ok.txt",
		BaseRev: 7,
		Size:    4,
		Kind:    PendingOverwrite,
		Mode:    0o600,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 0 {
		t.Fatalf("base PUT calls = %d, want 0", got)
	}
	if gotPath != "/v1/layers/layer-1/entries" {
		t.Fatalf("layer path = %q", gotPath)
	}
	if gotReq.Path != "/remote/ok.txt" || gotReq.Op != "upsert" || gotReq.Kind != "file" {
		t.Fatalf("layer request = %+v", gotReq)
	}
	if gotReq.BaseRevision != 7 || gotReq.SizeBytes != 4 || gotReq.Mode != 0o600 {
		t.Fatalf("layer request metadata = %+v", gotReq)
	}
	if !bytes.Equal(gotReq.Content, []byte("data")) {
		t.Fatalf("layer content = %q, want data", gotReq.Content)
	}
	if !pending.HasPending("/ok.txt") {
		t.Fatal("pending entry should remain after successful layer upload")
	}
	if !shadow.Has("/ok.txt") {
		t.Fatal("shadow should remain after successful layer upload")
	}
}

func TestCommitQueueLayerUploadAcceptsShadowSpillWithoutBaseWrite(t *testing.T) {
	var gotPath, gotSize, gotMode string
	var gotBody []byte
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			putCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/objects" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gotPath = r.URL.Query().Get("path")
		gotSize = r.URL.Query().Get("size")
		gotMode = r.URL.Query().Get("mode")
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"layer_id":   "layer-1",
			"path":       gotPath,
			"op":         "upsert",
			"kind":       "file",
			"size_bytes": 10,
			"entry_seq":  1,
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/spill.bin", []byte("spill-data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutShadowSpillWithMode("/spill.bin", 10, PendingNew, 0, 0o644, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8, "/remote")
	cq.SetLayerRef("layer-1")
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/spill.bin",
		Size:        10,
		Kind:        PendingNew,
		ShadowSpill: true,
		Mode:        0o644,
		HasMode:     true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 0 {
		t.Fatalf("base PUT calls = %d, want 0", got)
	}
	if gotPath != "/remote/spill.bin" || gotSize != "10" || gotMode != "644" {
		t.Fatalf("layer object query path=%q size=%q mode=%q", gotPath, gotSize, gotMode)
	}
	if !bytes.Equal(gotBody, []byte("spill-data")) {
		t.Fatalf("layer content = %q, want spill-data", gotBody)
	}
	if !pending.HasPending("/spill.bin") {
		t.Fatal("pending entry should remain after successful layer upload")
	}
}

type clientLayerEntryRequest struct {
	Path         string `json:"path"`
	Op           string `json:"op"`
	Kind         string `json:"kind"`
	BaseRevision int64  `json:"base_revision"`
	Content      []byte `json:"content"`
	ContentText  string `json:"content_text"`
	SizeBytes    int64  `json:"size_bytes"`
	Mode         uint32 `json:"mode"`
}

func TestCommitQueueSkipsDefaultModeForPendingNew(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			http.Error(w, "unexpected chmod", http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/plain.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/plain.txt", 4, PendingNew, 0, defaultRegularFileMode, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/plain.txt",
		Size:    4,
		Kind:    PendingNew,
		Mode:    defaultRegularFileMode,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 0 {
		t.Fatalf("chmod calls = %d, want 0", got)
	}
	if pending.HasPending("/plain.txt") {
		t.Fatal("pending entry should be removed after successful default-mode commit")
	}
	if shadow.Has("/plain.txt") {
		t.Fatal("shadow should be removed after successful default-mode commit")
	}
}

func TestShouldApplyRemoteModePreservesExplicitDefaultOverwrite(t *testing.T) {
	if !shouldApplyRemoteMode(PendingOverwrite, true, defaultRegularFileMode) {
		t.Fatal("PendingOverwrite with explicit 0644 mode must still apply remote chmod")
	}
}

func TestCommitQueueRetriesPostUploadChmodNotFound(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			if chmodCalls.Add(1) == 1 {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/exec-retry.sh", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/exec-retry.sh", 4, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/exec-retry.sh",
		Size:    4,
		Kind:    PendingNew,
		Mode:    0o755,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 2 {
		t.Fatalf("chmod calls = %d, want 2", got)
	}
	if pending.HasPending("/exec-retry.sh") {
		t.Fatal("pending entry should be removed after chmod retry succeeds")
	}
	if shadow.Has("/exec-retry.sh") {
		t.Fatal("shadow should be removed after chmod retry succeeds")
	}
}

func TestCommitQueueChmodFailureKeepsPendingState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			http.Error(w, "chmod failed", http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/exec-fail.sh", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/exec-fail.sh", 4, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.DrainAll()
	err = cq.CommitNow(context.Background(), &CommitEntry{
		Path:    "/exec-fail.sh",
		Size:    4,
		Kind:    PendingNew,
		Mode:    0o755,
		HasMode: true,
	})
	if err == nil {
		t.Fatal("CommitNow should fail when chmod fails")
	}
	if !pending.HasPending("/exec-fail.sh") {
		t.Fatal("pending entry should be retained when chmod fails")
	}
	if !shadow.Has("/exec-fail.sh") {
		t.Fatal("shadow should be retained when chmod fails")
	}
}

func TestCommitQueueConflictKeepsPendingState(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/conflict.txt", []byte("data"), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/conflict.txt", 4, PendingOverwrite, 3); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/conflict.txt",
		BaseRev: 3,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// After the initial 409, auto-resolve attempts a Stat (HEAD) which also
	// hits the blanket 409 handler, causing it to fall back to terminal failure.
	// We expect 2 calls: the original upload + the Stat attempt.
	if calls != 2 {
		t.Fatalf("expected 2 calls (upload + auto-resolve stat), got %d", calls)
	}
	// Terminal failure preserves shadow and pending data for manual recovery,
	// but marks the entry as PendingConflict so RecoverPending skips it.
	if !pending.HasPending("/conflict.txt") {
		t.Fatal("pending entry should be preserved after terminal conflict")
	}
	meta, ok := pending.GetMeta("/conflict.txt")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be marked as PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
	if !shadow.Has("/conflict.txt") {
		t.Fatal("shadow should be preserved after terminal conflict")
	}
	if got := cq.Pending(); got != 0 {
		t.Fatalf("queue pending count = %d, want 0 after terminal conflict", got)
	}
}

func TestCommitQueueCancelPathDoesNotPoisonFutureSamePath(t *testing.T) {
	const path = "/repo/.git/config.lock"
	newData := []byte("[remote \"origin\"]\n\turl = https://github.com/mem9-ai/drive9.git\n")

	var puts [][]byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read PUT body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		puts = append(puts, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	oldEntry := &CommitEntry{Path: path, Size: 3, Kind: PendingNew}
	cq := &CommitQueue{
		client:     newTestClient(ts.URL),
		shadows:    shadow,
		index:      pending,
		inFlight:   make(map[string]*CommitEntry),
		maxPending: 8,
		workCh:     make(chan *CommitEntry, 8),
	}
	cq.queue = append(cq.queue, oldEntry)
	cq.workCh <- oldEntry

	cq.CancelPath(path)
	if !cq.isEntryCanceled(oldEntry) {
		t.Fatal("old queued entry should be canceled")
	}

	if err := shadow.WriteFull(path, newData, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, int64(len(newData)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	newEntry := &CommitEntry{Path: path, Size: int64(len(newData)), Kind: PendingNew}
	if cq.isEntryCanceled(newEntry) {
		t.Fatal("new entry for same path must not inherit old cancellation")
	}
	if err := cq.Enqueue(newEntry); err != nil {
		t.Fatal(err)
	}

	close(cq.workCh)
	cq.wg.Add(1)
	go cq.worker()
	cq.wg.Wait()

	if len(puts) != 1 {
		t.Fatalf("PUT count = %d, want 1", len(puts))
	}
	if !bytes.Equal(puts[0], newData) {
		t.Fatalf("PUT body = %q, want %q", puts[0], newData)
	}
	if pending.HasPending(path) {
		t.Fatal("pending entry should be removed after new entry commits")
	}
	if shadow.Has(path) {
		t.Fatal("shadow should be removed after new entry commits")
	}
}

func TestCommitQueueCancelPathCancelsRetryBackoff(t *testing.T) {
	const path = "/retry.txt"
	var calls atomic.Int32
	firstAttemptDone := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			call := calls.Add(1)
			http.Error(w, `{"error":"temporary"}`, http.StatusServiceUnavailable)
			if call == 1 {
				close(firstAttemptDone)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 4, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	if err := cq.Enqueue(&CommitEntry{Path: path, Size: 4, Kind: PendingNew}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstAttemptDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first upload attempt did not finish")
	}

	start := time.Now()
	cq.CancelPath(path)
	done := make(chan struct{})
	go func() {
		cq.WaitPath(path)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("WaitPath did not unblock during retry backoff cancellation")
	}
	if elapsed := time.Since(start); elapsed >= 150*time.Millisecond {
		t.Fatalf("cancel during backoff took %s, want <150ms", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upload attempts after cancellation = %d, want 1", got)
	}
}

func TestCommitQueueQueuedPathIndexTracksCancelAndRemove(t *testing.T) {
	path := "/repo/file.txt"
	otherPath := "/repo/other.txt"
	e1 := &CommitEntry{Path: path}
	e2 := &CommitEntry{Path: path}
	other := &CommitEntry{Path: otherPath}
	cq := &CommitQueue{
		queue:        []*CommitEntry{e1, other, e2},
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
	}
	cq.addQueuedLocked(e1)
	cq.addQueuedLocked(other)
	cq.addQueuedLocked(e2)

	if !cq.HasPath(path) {
		t.Fatal("HasPath should find queued entries through index")
	}
	cq.CancelPath(path)
	if cq.HasPath(path) {
		t.Fatal("CancelPath should remove all queued entries for path from index")
	}
	if !cq.isEntryCanceled(e1) || !cq.isEntryCanceled(e2) {
		t.Fatal("CancelPath should cancel all queued entries for path")
	}
	if !cq.HasPath(otherPath) {
		t.Fatal("CancelPath should preserve unrelated queued paths")
	}

	cq.removeFromQueue(other)
	if cq.HasPath(otherPath) {
		t.Fatal("removeFromQueue should remove queued path index entry")
	}
}

// TestCommitQueueDirectPutRouting verifies that files under
// commitQueueDirectPutThreshold use direct PUT (WriteCtxConditionalWithRevision)
// which sends raw body, while files at or above the threshold use multipart
// upload (uploadBufferedRemoteFile → WriteStreamConditional).
func TestCommitQueueDirectPutRouting(t *testing.T) {
	var usedDirectPut bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Direct PUT sends Content-Type: application/octet-stream with raw body.
		// Multipart upload sends to /upload/initiate first.
		if r.Method == http.MethodPut && r.Header.Get("Content-Type") == "application/octet-stream" {
			usedDirectPut = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":10}`))
			return
		}
		// Fallback: accept anything else (multipart endpoints, etc.)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	t.Run("small file uses direct PUT", func(t *testing.T) {
		usedDirectPut = false
		shadow, err := NewShadowStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer shadow.Close()
		pending, err := NewPendingIndex(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}

		data := make([]byte, 30000) // 30KB — under 50KB server threshold
		if err := shadow.WriteFull("/small.bin", data, 5); err != nil {
			t.Fatal(err)
		}
		if _, err := pending.PutWithBaseRev("/small.bin", int64(len(data)), PendingOverwrite, 5); err != nil {
			t.Fatal(err)
		}

		cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
		if err := cq.Enqueue(&CommitEntry{
			Path:    "/small.bin",
			BaseRev: 5,
			Size:    int64(len(data)),
			Kind:    PendingOverwrite,
		}); err != nil {
			t.Fatal(err)
		}
		cq.DrainAll()

		if !usedDirectPut {
			t.Fatal("30KB file should use direct PUT, not multipart")
		}
		if shadow.Has("/small.bin") {
			t.Fatal("shadow should be removed after commit")
		}
	})
}

func TestCommitQueueLayerModeRetainsShadowAndPendingAfterUpload(t *testing.T) {
	var gotContent []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req struct {
			Path    string `json:"path"`
			Content []byte `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Path != "/layered.bin" {
			t.Errorf("request path = %q, want /layered.bin", req.Path)
		}
		gotContent = append([]byte(nil), req.Content...)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"layer_id":  "layer-1",
			"path":      req.Path,
			"op":        "upsert",
			"kind":      "file",
			"entry_seq": 1,
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("layer data")
	if err := shadow.WriteFull("/layered.bin", data, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/layered.bin", int64(len(data)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.SetLayerRef("layer-1")
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/layered.bin",
		BaseRev: 0,
		Size:    int64(len(data)),
		Kind:    PendingNew,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if !bytes.Equal(gotContent, data) {
		t.Fatalf("uploaded content = %q, want %q", gotContent, data)
	}
	if !shadow.Has("/layered.bin") {
		t.Fatal("layer shadow should be retained after upload")
	}
	if !pending.HasPending("/layered.bin") {
		t.Fatal("layer pending metadata should be retained after upload")
	}
	meta, ok := pending.GetMeta("/layered.bin")
	if !ok || meta.Kind != PendingOverwrite {
		t.Fatalf("layer pending metadata after upload = %+v, want PendingOverwrite", meta)
	}
	data2 := []byte("layer data v2")
	if err := shadow.WriteFull("/layered.bin", data2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/layered.bin", int64(len(data2)), PendingOverwrite, 0); err != nil {
		t.Fatal(err)
	}
	cq2 := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq2.SetLayerRef("layer-1")
	if err := cq2.Enqueue(&CommitEntry{
		Path:    "/layered.bin",
		BaseRev: 0,
		Size:    int64(len(data2)),
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq2.DrainAll()
	if !bytes.Equal(gotContent, data2) {
		t.Fatalf("second uploaded content = %q, want %q", gotContent, data2)
	}
}

func TestCommitQueueLayerUploadRejectsSizeMismatch(t *testing.T) {
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/mismatch.bin", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient("http://127.0.0.1"), shadow, pending, nil, 1, 8)
	_, err = cq.uploadLayerEntry(context.Background(), "layer-1", &CommitEntry{
		Path: "/mismatch.bin",
		Size: 99,
		Kind: PendingNew,
	}, "/mismatch.bin", 0)
	if err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("uploadLayerEntry err=%v, want size mismatch", err)
	}
	cq.DrainAll()
}

// --- Auto-resolve tests (LWW MVP) ---

// Test axis 1: 409 → fetch → LWW re-upload succeeds.
func TestCommitQueueAutoResolveLWW(t *testing.T) {
	var uploadCalls, statCalls, readCalls int
	var successRev int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			// Stat: return current revision.
			statCalls++
			w.Header().Set("X-Dat9-Revision", "10")
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// Read: return different content (triggers LWW).
			readCalls++
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("server-data!"))
		default:
			// PUT/POST: upload path.
			uploadCalls++
			rev := r.Header.Get("X-Dat9-Expected-Revision")
			if rev == "5" {
				// First upload: 409 conflict.
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			if rev == "10" {
				// LWW re-upload with new revision: success.
				w.WriteHeader(http.StatusOK)
				return
			}
			http.Error(w, `{"error":"unexpected revision"}`, http.StatusBadRequest)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/lww.txt", []byte("local-data!!"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/lww.txt", 12, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != "/lww.txt" {
			t.Fatalf("OnSuccess path = %q, want /lww.txt", entry.Path)
		}
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/lww.txt",
		BaseRev: 5,
		Size:    12,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if uploadCalls != 2 {
		t.Fatalf("upload calls = %d, want 2 (initial 409 + LWW retry)", uploadCalls)
	}
	if statCalls != 1 {
		t.Fatalf("stat calls = %d, want 1", statCalls)
	}
	if readCalls != 1 {
		t.Fatalf("read calls = %d, want 1", readCalls)
	}
	if successRev != 11 {
		t.Fatalf("OnSuccess committedRev = %d, want 11 from resolved server revision", successRev)
	}
	// Success: shadow and pending should be cleaned up.
	if pending.HasPending("/lww.txt") {
		t.Fatal("pending entry should be removed after successful LWW")
	}
	if shadow.Has("/lww.txt") {
		t.Fatal("shadow should be removed after successful LWW")
	}
}

// Test axis 2: 409 → fetch → content matches → idempotent success.
func TestCommitQueueAutoResolveIdempotent(t *testing.T) {
	var uploadCalls int
	var successRev int64
	sameContent := []byte("identical!")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "8")
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(sameContent)
		default:
			// First upload: 409.
			uploadCalls++
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/idem.txt", sameContent, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/idem.txt", int64(len(sameContent)), PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != "/idem.txt" {
			t.Fatalf("OnSuccess path = %q, want /idem.txt", entry.Path)
		}
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/idem.txt",
		BaseRev: 5,
		Size:    int64(len(sameContent)),
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// Only the initial upload should have been attempted (the 409 that triggered auto-resolve).
	// No LWW re-upload because content matched.
	if uploadCalls != 1 {
		t.Fatalf("upload calls = %d, want 1 (initial 409 only, no LWW re-upload)", uploadCalls)
	}
	if successRev != 8 {
		t.Fatalf("OnSuccess committedRev = %d, want matching server revision", successRev)
	}
	// Idempotent: should be cleaned up without a second upload.
	if pending.HasPending("/idem.txt") {
		t.Fatal("pending entry should be removed after idempotent resolve")
	}
	if shadow.Has("/idem.txt") {
		t.Fatal("shadow should be removed after idempotent resolve")
	}
}

// Test axis 3: 409 → fetch → LWW re-upload → second 409 → PendingConflict fallback.
func TestCommitQueueAutoResolveLWWSecond409Fallback(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "10")
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("server-data!"))
		default:
			// ALL uploads return 409 (simulates rapid concurrent writes).
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/double409.txt", []byte("local-data!!"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/double409.txt", 12, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/double409.txt",
		BaseRev: 5,
		Size:    12,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// Should fall back to PendingConflict — no regression from current behavior.
	if !pending.HasPending("/double409.txt") {
		t.Fatal("pending entry should be preserved after double-409 fallback")
	}
	meta, ok := pending.GetMeta("/double409.txt")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
	if !shadow.Has("/double409.txt") {
		t.Fatal("shadow should be preserved after double-409 fallback")
	}
}

// Test axis 4: Non-conflict errors (500) still follow existing retry + terminal failure.
func TestCommitQueueNonConflictErrorUnchanged(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/500.txt", []byte("data"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/500.txt", 4, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/500.txt",
		BaseRev: 5,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// 500 errors should retry (maxRetries=5), not trigger auto-resolve.
	if calls != 5 {
		t.Fatalf("500 error should retry 5 times, got %d calls", calls)
	}
	// Terminal failure: PendingConflict marker.
	if !pending.HasPending("/500.txt") {
		t.Fatal("pending entry should be preserved after 500 terminal failure")
	}
	meta, ok := pending.GetMeta("/500.txt")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
}

func TestCommitQueueRecoverPendingSkipsLegacyOverwriteWithoutBaseRev(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/legacy.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Put("/legacy.txt", 4, PendingOverwrite); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()

	if calls != 0 {
		t.Fatalf("legacy overwrite should not be auto-committed, got %d requests", calls)
	}
	if !pending.HasPending("/legacy.txt") {
		t.Fatal("legacy pending entry should remain for explicit recovery")
	}
	if !shadow.Has("/legacy.txt") {
		t.Fatal("legacy shadow should remain after skipped recovery")
	}
}

// --- ShadowSpill tests ---

// TestCommitQueueShadowSpillUpload verifies that ShadowSpill entries at or
// above the cached inline threshold are uploaded via streaming multipart and
// that the server receives the correct data and expected revision.
func TestCommitQueueShadowSpillUpload(t *testing.T) {
	data := bytes.Repeat([]byte("shadowspill-data-"), 100) // ~1700 bytes
	var gotExpected int64
	var gotBody []byte
	var statCalls atomic.Int32
	var successRev int64
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/big.bin":
			statCalls.Add(1)
			w.Header().Set("X-Dat9-Revision", "13")
			w.Header().Set("Content-Length", "1700")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != "/big.bin" {
				t.Fatalf("initiate path = %q, want /big.bin", req.Path)
			}
			if req.TotalSize != int64(len(data)) {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, len(data))
			}
			if req.ExpectedRevision == nil {
				t.Fatal("initiate expected_revision missing")
			}
			gotExpected = *req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u1",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u1/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u1/1":
			body, _ := io.ReadAll(r.Body)
			gotBody = body
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/big.bin", data, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/big.bin", int64(len(data)), PendingOverwrite, 12); err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != "/big.bin" {
			t.Fatalf("OnSuccess path = %q, want /big.bin", entry.Path)
		}
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/big.bin",
		BaseRev:     12,
		Size:        int64(len(data)),
		Kind:        PendingOverwrite,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if gotExpected != 12 {
		t.Fatalf("expected revision = %d, want 12", gotExpected)
	}
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if statCalls.Load() != 0 {
		t.Fatalf("stat calls = %d, want 0", statCalls.Load())
	}
	if successRev != 13 {
		t.Fatalf("OnSuccess committedRev = %d, want 13 from CAS base revision", successRev)
	}
	if pending.HasPending("/big.bin") {
		t.Fatal("pending entry should be removed after successful ShadowSpill commit")
	}
	if shadow.Has("/big.bin") {
		t.Fatal("shadow should be removed after successful ShadowSpill commit")
	}
}

func TestCommitQueueShadowSpillSmallFileDirectPUTCleansState(t *testing.T) {
	data := []byte("small shadow spill content")
	const path = "/small-spill.bin"
	var gotExpected int64 = -1
	var gotBody []byte
	var multipartCalls atomic.Int32
	var successRev int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/small-spill.bin":
			var err error
			gotBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read direct PUT body: %v", err)
			}
			gotExpected, err = strconv.ParseInt(r.Header.Get("X-Dat9-Expected-Revision"), 10, 64)
			if err != nil {
				t.Fatalf("expected revision header: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 13})
		case strings.HasPrefix(r.URL.Path, "/v2/uploads/"):
			multipartCalls.Add(1)
			t.Fatalf("small ShadowSpill should not use multipart: %s %s", r.Method, r.URL.String())
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, data, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutShadowSpill(path, int64(len(data)), PendingOverwrite, 12); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != path {
			t.Fatalf("OnSuccess path = %q, want %q", entry.Path, path)
		}
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:        path,
		BaseRev:     12,
		Size:        int64(len(data)),
		Kind:        PendingOverwrite,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if gotExpected != 12 {
		t.Fatalf("expected revision = %d, want 12", gotExpected)
	}
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("direct PUT body = %q, want %q", gotBody, data)
	}
	if multipartCalls.Load() != 0 {
		t.Fatalf("multipart calls = %d, want 0", multipartCalls.Load())
	}
	if successRev != 13 {
		t.Fatalf("OnSuccess committedRev = %d, want 13", successRev)
	}
	if pending.HasPending(path) {
		t.Fatal("pending entry should be removed after successful direct PUT ShadowSpill commit")
	}
	if shadow.Has(path) {
		t.Fatal("shadow should be removed after successful direct PUT ShadowSpill commit")
	}
}

func TestCommitQueueMultipartCreateInfersRevisionForOpenHandle(t *testing.T) {
	data := bytes.Repeat([]byte("d"), 1024)
	const path = "/workload.db"
	var gotExpected int64 = -1
	var gotBody []byte
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != path {
				t.Fatalf("initiate path = %q, want %q", req.Path, path)
			}
			if req.TotalSize != int64(len(data)) {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, len(data))
			}
			if req.ExpectedRevision == nil {
				t.Fatal("initiate expected_revision missing")
			}
			gotExpected = *req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u1",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u1/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u1/1":
			var err error
			gotBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read part body: %v", err)
			}
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	fs := NewDat9FS(c, opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.PathLock = fs.lockRemoteCommitPath
	cq.OnSuccess = fs.onCommitQueueSuccess
	cq.OnCleanup = fs.onCommitQueueCleanup
	fs.commitQueue = cq

	if err := shadow.WriteFull(path, data, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutShadowSpill(path, int64(len(data)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	ino := fs.inodes.Lookup(path, false, int64(len(data)), time.Now())
	fh := &FileHandle{
		Ino:         ino,
		Path:        path,
		Dirty:       fs.newWriteBuffer(path, maxPreloadSize, 0),
		IsNew:       true,
		ShadowReady: true,
		ShadowSpill: true,
	}
	fhID := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(fhID, fh)

	if err := cq.Enqueue(&CommitEntry{
		Path:        path,
		Inode:       ino,
		BaseRev:     0,
		Size:        int64(len(data)),
		Kind:        PendingNew,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if gotExpected != 0 {
		t.Fatalf("expected revision = %d, want create-if-absent 0", gotExpected)
	}
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if fh.IsNew {
		t.Fatal("open handle remained PendingNew after successful multipart create")
	}
	if fh.BaseRev != 1 {
		t.Fatalf("open handle BaseRev = %d, want inferred committed revision 1", fh.BaseRev)
	}
	if pending.HasPending(path) {
		t.Fatal("pending entry should be removed after commit")
	}
	if shadow.Has(path) {
		t.Fatal("shadow should be removed after commit")
	}
}

// TestCommitQueueShadowSpillConflictTerminal verifies that for ShadowSpill
// entries a genuine conflict (server size differs from local shadow) goes to
// terminal failure after a single HEAD probe — no content download, no LWW
// re-upload, no full-memory ReadAll.
func TestCommitQueueShadowSpillConflictTerminal(t *testing.T) {
	var putCalls, headCalls, getCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			headCalls++
			// Server size differs from local 4 bytes → genuine conflict.
			w.Header().Set("X-Dat9-Revision", "9")
			w.Header().Set("Content-Length", "999")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			getCalls++
			http.Error(w, `{"error":"unexpected read"}`, http.StatusInternalServerError)
		default:
			putCalls++
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/big-conflict.bin", []byte("data"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/big-conflict.bin", 4, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/big-conflict.bin",
		BaseRev:     5,
		Size:        4,
		Kind:        PendingOverwrite,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if headCalls != 1 {
		t.Fatalf("expected 1 HEAD probe for size check, got %d", headCalls)
	}
	if getCalls != 0 {
		t.Fatalf("expected no content reads when sizes differ, got %d", getCalls)
	}
	if !pending.HasPending("/big-conflict.bin") {
		t.Fatal("pending entry should be preserved after ShadowSpill terminal conflict")
	}
	meta, ok := pending.GetMeta("/big-conflict.bin")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
	if !shadow.Has("/big-conflict.bin") {
		t.Fatal("shadow should be preserved after ShadowSpill terminal conflict")
	}
}

// TestCommitQueueShadowSpillConflictIdempotent verifies that when the server
// content already equals the local shadow (crash after a completed upload,
// before local cleanup), a ShadowSpill 409 resolves as idempotent success via
// chunked compare instead of a phantom terminal conflict.
func TestCommitQueueShadowSpillConflictIdempotent(t *testing.T) {
	data := bytes.Repeat([]byte("spill-ok"), 512) // 4096 bytes
	var putCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "7")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			http.ServeContent(w, r, "big.bin", time.Time{}, bytes.NewReader(data))
		default:
			putCalls++
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/big.bin", data, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutShadowSpill("/big.bin", int64(len(data)), PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/big.bin",
		BaseRev:     5,
		Size:        int64(len(data)),
		Kind:        PendingOverwrite,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if putCalls != 1 {
		t.Fatalf("expected exactly 1 upload attempt (no LWW re-upload), got %d", putCalls)
	}
	if pending.HasPending("/big.bin") {
		t.Fatal("pending entry should be removed after idempotent resolve")
	}
	if shadow.Has("/big.bin") {
		t.Fatal("shadow should be removed after idempotent resolve")
	}
}

// TestCommitQueueAutoResolveNotFoundRetriesAsCreate verifies that a 409 whose
// auto-resolve stat finds no remote file (e.g. the server 409ed on an upload
// session orphaned by a crashed client, not on content) retries the upload as
// a create instead of terminal-failing and abandoning the data.
func TestCommitQueueAutoResolveNotFoundRetriesAsCreate(t *testing.T) {
	var uploadCalls, statCalls int
	var successRev int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			statCalls++
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		case http.MethodGet:
			t.Errorf("unexpected GET %s", r.URL.String())
			http.Error(w, `{"error":"unexpected read"}`, http.StatusInternalServerError)
		default:
			uploadCalls++
			if r.Header.Get("X-Dat9-Expected-Revision") != "0" {
				http.Error(w, `{"error":"unexpected revision"}`, http.StatusBadRequest)
				return
			}
			if uploadCalls == 1 {
				// Dangling upload session left by a crashed client.
				http.Error(w, `{"error":"active upload already exists for this path"}`, http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/recovered.txt", []byte("recovered-data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/recovered.txt", 14, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/recovered.txt",
		BaseRev: 0,
		Size:    14,
		Kind:    PendingNew,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if uploadCalls != 2 {
		t.Fatalf("upload calls = %d, want 2 (initial 409 + create retry)", uploadCalls)
	}
	if statCalls != 1 {
		t.Fatalf("stat calls = %d, want 1", statCalls)
	}
	if successRev != 1 {
		t.Fatalf("OnSuccess committedRev = %d, want 1 for a resolved create", successRev)
	}
	if pending.HasPending("/recovered.txt") {
		t.Fatal("pending entry should be removed after create retry succeeds")
	}
	if shadow.Has("/recovered.txt") {
		t.Fatal("shadow should be removed after create retry succeeds")
	}
}

// TestCommitQueueShadowSpillConflictNotFoundRetriesAsCreate reproduces the
// crash-recovery e2e failure: a SIGKILLed mount leaves a dangling server-side
// upload session, so the recovery commit's initiate 409s even though the file
// was never committed (stat 404). The resolve must re-upload as a create
// rather than parking the recovered data as a terminal conflict.
func TestCommitQueueShadowSpillConflictNotFoundRetriesAsCreate(t *testing.T) {
	data := bytes.Repeat([]byte("recovered-spill-"), 256)
	var initiateCalls, statCalls int
	var gotBody []byte
	retryExpectedRev := int64(-1)
	var successRev int64
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			statCalls++
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			initiateCalls++
			if initiateCalls == 1 {
				// Dangling upload session left by the crashed client.
				http.Error(w, `{"error":"active upload already exists for this path"}`, http.StatusConflict)
				return
			}
			var req struct {
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode initiate request: %v", err)
			}
			if req.ExpectedRevision != nil {
				retryExpectedRev = *req.ExpectedRevision
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u-retry",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u-retry/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u-retry/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u-retry/1":
			body, _ := io.ReadAll(r.Body)
			gotBody = body
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u-retry/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			http.Error(w, `{"error":"unexpected request"}`, http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/recovered-spill.bin", data, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutShadowSpill("/recovered-spill.bin", int64(len(data)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		successRev = committedRev
	}
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/recovered-spill.bin",
		BaseRev:     0,
		Size:        int64(len(data)),
		Kind:        PendingNew,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if initiateCalls != 2 {
		t.Fatalf("initiate calls = %d, want 2 (initial 409 + create retry)", initiateCalls)
	}
	if statCalls != 1 {
		t.Fatalf("stat calls = %d, want 1", statCalls)
	}
	if retryExpectedRev != 0 {
		t.Fatalf("retry expected_revision = %d, want 0 (create)", retryExpectedRev)
	}
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if successRev != 1 {
		t.Fatalf("OnSuccess committedRev = %d, want 1 for a resolved create", successRev)
	}
	if pending.HasPending("/recovered-spill.bin") {
		t.Fatal("pending entry should be removed after create retry succeeds")
	}
	if shadow.Has("/recovered-spill.bin") {
		t.Fatal("shadow should be removed after create retry succeeds")
	}
}

// TestCommitQueueRecoverPendingShadowSpill verifies that crash recovery
// preserves the ShadowSpill flag so recovered entries use streaming upload
// (not ReadAll which would OOM for large files).
func TestCommitQueueRecoverPendingShadowSpill(t *testing.T) {
	data := bytes.Repeat([]byte("recover-"), 200)
	var gotBody []byte
	var statCalls atomic.Int32
	var successRev int64
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/recover.bin":
			statCalls.Add(1)
			w.Header().Set("X-Dat9-Revision", "9")
			w.Header().Set("Content-Length", "1600")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != "/recover.bin" {
				t.Fatalf("initiate path = %q, want /recover.bin", req.Path)
			}
			if req.TotalSize != int64(len(data)) {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, len(data))
			}
			if req.ExpectedRevision == nil || *req.ExpectedRevision != 8 {
				t.Fatalf("initiate expected_revision = %v, want 8", req.ExpectedRevision)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u1",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u1/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u1/1":
			body, _ := io.ReadAll(r.Body)
			gotBody = body
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()

	// Phase 1: create pending entry with ShadowSpill=true, then "crash" (close everything).
	shadow1, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending1, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow1.WriteFull("/recover.bin", data, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := pending1.PutShadowSpill("/recover.bin", int64(len(data)), PendingOverwrite, 8); err != nil {
		t.Fatal(err)
	}
	shadow1.Close()

	// Phase 2: "restart" — reopen stores and recover.
	shadow2, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow2.Close()
	if err := shadow2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	pending2, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}

	// Verify the recovered meta has ShadowSpill=true.
	meta, ok := pending2.GetMeta("/recover.bin")
	if !ok {
		t.Fatal("pending entry should survive restart")
	}
	if !meta.ShadowSpill {
		t.Fatal("ShadowSpill flag must be persisted and recovered from disk")
	}

	// RecoverPending should reconstruct CommitEntry with ShadowSpill=true,
	// causing uploadEntry to use streaming (uploadFromShadow) not ReadAll.
	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(1)
	cq := NewCommitQueue(c, shadow2, pending2, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != "/recover.bin" {
			t.Fatalf("OnSuccess path = %q, want /recover.bin", entry.Path)
		}
		successRev = committedRev
	}
	cq.RecoverPending()
	cq.DrainAll()

	// Verify data arrived correctly at the server (streaming upload worked).
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if statCalls.Load() != 0 {
		t.Fatalf("stat calls = %d, want 0", statCalls.Load())
	}
	if successRev != 9 {
		t.Fatalf("OnSuccess committedRev = %d, want 9 from recovered CAS base revision", successRev)
	}
	if pending2.HasPending("/recover.bin") {
		t.Fatal("pending entry should be removed after successful recovery upload")
	}
	if shadow2.Has("/recover.bin") {
		t.Fatal("shadow should be removed after successful recovery upload")
	}
}

// TestCommitQueueRecoverPendingUsesShadowSize verifies that recovery routes
// the upload by the actual shadow file size, not stale pending metadata. A
// WAL-resurrected meta can carry an old size; routing on it would direct-PUT
// a file the server requires to be multipart (or vice versa).
func TestCommitQueueRecoverPendingUsesShadowSize(t *testing.T) {
	data := []byte("0123456789abcdef") // 16 bytes, well under the 50KB threshold
	var putBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/uploads/"):
			t.Errorf("multipart initiate used for a %d-byte file — stale meta size routed the upload", len(data))
			http.Error(w, `{"error":"unexpected multipart"}`, http.StatusBadRequest)
		default:
			http.Error(w, `{"error":"unexpected"}`, http.StatusBadRequest)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull("/stale.bin", data, 5); err != nil {
		t.Fatal(err)
	}
	// Stale meta: claims 60000 bytes (> 50KB threshold → would multipart).
	if _, err := pending.PutWithBaseRev("/stale.bin", 60000, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()

	if !bytes.Equal(putBody, data) {
		t.Fatalf("direct PUT body = %d bytes, want %d", len(putBody), len(data))
	}
	if pending.HasPending("/stale.bin") {
		t.Error("pending entry not cleaned up after recovered commit")
	}
}

// TestCommitQueueCancelPathJournalsCommitMarker verifies that unlinking a
// path with pending local state writes a WAL "done" marker, so a later crash
// recovery does not resurrect the deleted file from older fsync frames.
func TestCommitQueueCancelPathJournalsCommitMarker(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"unexpected"}`, http.StatusBadRequest)
	}))
	defer ts.Close()

	dir := t.TempDir()
	journal, err := NewJournal(dir + "/journal.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the Fsync flow: shadow + meta + WAL fsync frame.
	if err := shadow.WriteFull("/gone.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/gone.txt", 4, PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(JournalEntry{Op: JournalFsync, Path: "/gone.txt", Length: 4}); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, journal, 1, 8)
	cq.CancelPath("/gone.txt") // Unlink flow
	cq.DrainAll()

	if shadow.Has("/gone.txt") || pending.HasPending("/gone.txt") {
		t.Fatal("cancel did not clean local state")
	}

	// Crash recovery must not resurrect the unlinked path.
	recovered, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(journal, recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.HasPending("/gone.txt") {
		t.Error("unlinked path resurrected from WAL — cancel marker missing")
	}
}

func TestCommitQueueCancelQueuedZeroTruncatePreservesLocalOnlyWhenNotInFlight(t *testing.T) {
	zero := &CommitEntry{Path: "/file.txt", Size: 0, Kind: PendingOverwrite}
	nonzero := &CommitEntry{Path: "/other.txt", Size: 4, Kind: PendingOverwrite}
	cq := &CommitQueue{
		queue:        []*CommitEntry{zero, nonzero},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
	}
	cq.rebuildQueuedIndexLocked()

	if !cq.CancelQueuedZeroTruncatePreserveLocal("/file.txt") {
		t.Fatal("cancel queued zero truncate returned false")
	}
	if !zero.canceled {
		t.Fatal("zero truncate entry was not marked canceled")
	}
	if cq.HasPath("/file.txt") {
		t.Fatal("zero truncate path still queued")
	}
	if !cq.HasPath("/other.txt") {
		t.Fatal("non-zero queued entry was affected")
	}

	zero = &CommitEntry{Path: "/same.txt", Size: 0, Kind: PendingOverwrite}
	samePathNonzero := &CommitEntry{Path: "/same.txt", Size: 4, Kind: PendingOverwrite}
	cq = &CommitQueue{
		queue:        []*CommitEntry{zero, samePathNonzero},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{},
	}
	cq.rebuildQueuedIndexLocked()

	if cq.CancelQueuedZeroTruncatePreserveLocal("/same.txt") {
		t.Fatal("cancel returned true while non-zero same-path entry remained queued")
	}
	if !zero.canceled {
		t.Fatal("zero truncate entry was not marked canceled with same-path non-zero entry")
	}
	if samePathNonzero.canceled {
		t.Fatal("non-zero same-path entry was canceled")
	}
	if !cq.HasPath("/same.txt") {
		t.Fatal("same-path non-zero entry disappeared")
	}

	inFlight := &CommitEntry{Path: "/busy.txt", Size: 0, Kind: PendingOverwrite}
	cq = &CommitQueue{
		queue:        []*CommitEntry{inFlight},
		queuedByPath: map[string]map[*CommitEntry]struct{}{},
		inFlight:     map[string]*CommitEntry{"/busy.txt": inFlight},
	}
	cq.rebuildQueuedIndexLocked()

	if cq.CancelQueuedZeroTruncatePreserveLocal("/busy.txt") {
		t.Fatal("cancel returned true for in-flight zero truncate")
	}
	if inFlight.canceled {
		t.Fatal("in-flight zero truncate was canceled")
	}
	if !cq.HasPath("/busy.txt") {
		t.Fatal("in-flight path disappeared")
	}
}
