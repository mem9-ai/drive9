package writeback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// extentSpillServer captures the part bytes an extent session actually
// uploads, so a test can assert on CONTENT rather than on the fact that an
// upload happened. That distinction is the whole point: the bug it guards
// against publishes a full-size all-zero image and reports success.
type extentSpillServer struct {
	*httptest.Server
	partSize int64

	mu    sync.Mutex
	parts map[int][]byte // part number -> uploaded bytes
	// completeEchoID is what the completion echoes (completion_identity_v1).
	completeEchoID string
	missing        map[int]bool // parts the server said were already present
	initLen        int
}

func newExtentSpillServer(t *testing.T, partSize int64) *extentSpillServer {
	t.Helper()
	s := &extentSpillServer{partSize: partSize, parts: make(map[int][]byte), missing: make(map[int]bool)}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"inline_threshold":1,"storage_capabilities":{"completion_identity_v1":true}}`))
	})
	mux.HandleFunc("/v1/fs/", func(w http.ResponseWriter, r *http.Request) {
		// No remote object: the entry is a fresh create.
		http.Error(w, "not found", http.StatusNotFound)
	})
	mux.HandleFunc("/v2/uploads/initiate", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			TotalSize int64  `json:"total_size"`
			Layout    string `json:"layout"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		total := (body.TotalSize + s.partSize - 1) / s.partSize
		s.mu.Lock()
		s.initLen++
		s.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upload_id": "s1", "key": "", "part_size": s.partSize, "total_parts": total,
			"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
		})
	})
	mux.HandleFunc("/v2/uploads/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/presign-batch") {
			// completion_identity_v1: the real server echoes the identity of
			// the object it published, and consumers depend on it.
			s.mu.Lock()
			s.completeEchoID = "echoed-res-1"
			id := s.completeEchoID
			s.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "resource_id": id})
			return
		}
		var body struct {
			Parts []struct {
				Number int `json:"part_number"`
			} `json:"parts"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		out := make([]map[string]any, 0, len(body.Parts))
		for _, p := range body.Parts {
			out = append(out, map[string]any{
				"number": p.Number,
				"url":    fmt.Sprintf("http://%s/obj/%d", r.Host, p.Number),
				"size":   s.partSize,
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
	})
	mux.HandleFunc("/obj/", func(w http.ResponseWriter, r *http.Request) {
		var n int
		_, _ = fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/obj/"), "%d", &n)
		data, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.parts[n] = data
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

// image reassembles every part the server received, in order.
func (s *extentSpillServer) image() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []byte
	for n := 1; ; n++ {
		d, ok := s.parts[n]
		if !ok {
			break
		}
		out = append(out, d...)
	}
	return out
}

func (s *extentSpillServer) uploadedParts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.parts)
}

func sha256HexOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// newSpilledExtentQueue builds a spill-shaped entry for an image that lives
// ONLY in the shadow store — exactly the state the close path produces for an
// image at or above commitInlineStreamThreshold (10 MiB): the write buffer was
// evicted part by part, so the shadow is the sole source of the bytes.
func newSpilledExtentQueue(t *testing.T, content []byte) (*CommitQueue, *staging.ShadowStore, *CommitEntry, *extentSpillServer) {
	t.Helper()
	srv := newExtentSpillServer(t, 4<<20)
	dir := t.TempDir()
	shadows, err := staging.NewShadowStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/big.bin"
	if err := shadows.WriteFull(path, content, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutShadowSpillWithMode(path, int64(len(content)), staging.PendingNew, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(client.New(srv.URL, ""), shadows, pending, nil, 1, 8)
	cq.SetExtentTakeover(true)
	entry := &CommitEntry{
		Path: path, Size: int64(len(content)), Kind: staging.PendingNew,
		ShadowSpill:           true,
		ShadowGen:             shadows.ActiveGeneration(path),
		PendingIndexGen:       gen,
		DisableAutoResolveLWW: true,
	}
	return cq, shadows, entry, srv
}

// A spilled image at or above the stream threshold must upload ITS OWN bytes.
// Regression: ShadowGen was taken from the content-generation space while the
// reader resolved it in the pin-generation space, so every spilled extent
// upload failed to find its shadow — and the "truncate-only" fallback turned
// that failure into a successful publication of pure zeros.
func TestSpilledExtentEntryUploadsItsOwnBytes(t *testing.T) {
	// Two distinct 4 MiB parts plus a tail: identical parts would let a
	// zero-filled image masquerade as correct after dedup.
	part := make([]byte, 4<<20)
	for i := range part {
		part[i] = byte(i%251) + 1 // never zero, so a zero image cannot match
	}
	content := append(append([]byte{}, part...), part...)
	content = append(content, []byte("tail-bytes-not-zero")...)

	cq, _, entry, srv := newSpilledExtentQueue(t, content)
	defer cq.DrainAll()

	if _, err := cq.uploadEntry(context.Background(), entry); err != nil {
		t.Fatalf("upload: %v", err)
	}
	got := srv.image()
	if len(got) != len(content) {
		t.Fatalf("uploaded %d bytes, want %d", len(got), len(content))
	}
	if sha256HexOf(got) != sha256HexOf(content) {
		t.Fatalf("published image is not the staged image\n got sha256 %s (%d bytes)\nwant sha256 %s",
			sha256HexOf(got), len(got), sha256HexOf(content))
	}
	if bytes.Equal(got, make([]byte, len(content))) {
		t.Fatal("published image is all zeros: the shadow was never read")
	}
}

// A spilled entry whose shadow is genuinely GONE must fail closed. Serving
// zeros to a non-truncate file is silent data loss; the entry has to error.
func TestSpilledExtentEntryFailsWhenShadowIsGone(t *testing.T) {
	content := bytes.Repeat([]byte("real-bytes"), 1<<20) // ~10 MiB, non-zero
	cq, shadows, entry, srv := newSpilledExtentQueue(t, content)
	defer cq.DrainAll()

	// Destroy the shadow behind the entry's back, as a concurrent cleanup
	// or an over-eager retention would.
	shadows.Remove(entry.Path)

	if _, err := cq.uploadEntry(context.Background(), entry); err == nil {
		uploaded := srv.image()
		t.Fatalf("upload of a vanished shadow returned success (published %d bytes, %d uploaded parts); want an error",
			len(uploaded), srv.uploadedParts())
	}
}

// The truncate-only shortcut must still work: a fresh file that holds only a
// logical size and never staged bytes is legitimately all zeros.
func TestTruncateOnlySpillStillPublishesZeros(t *testing.T) {
	const size = 12 << 20
	srv := newExtentSpillServer(t, 4<<20)
	shadows, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/sparse.bin"
	gen, err := pending.PutShadowSpillWithMode(path, size, staging.PendingNew, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(client.New(srv.URL, ""), shadows, pending, nil, 1, 8)
	cq.SetExtentTakeover(true)
	entry := &CommitEntry{
		Path: path, Size: size, Kind: staging.PendingNew,
		ShadowSpill:     true,
		ShadowGen:       0, // never staged: no content generation exists
		PendingIndexGen: gen,
	}
	if _, err := cq.uploadEntry(context.Background(), entry); err != nil {
		t.Fatalf("truncate-only upload: %v", err)
	}
	got := srv.image()
	if len(got) != size {
		t.Fatalf("uploaded %d bytes, want %d", len(got), size)
	}
	if !bytes.Equal(got, make([]byte, size)) {
		t.Fatal("a truncate-only image must be zeros")
	}
}
