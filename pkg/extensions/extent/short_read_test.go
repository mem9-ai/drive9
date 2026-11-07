package extent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
)

// A classic read must not accept a body SHORTER than the size this mount
// already published.
//
// The classic surface publishes with a CAS PUT and reads with a range GET. A
// GET answered by a replica that has not caught up returns the pre-write
// object — for a file this mount just grew, a shorter body. Accepting it hands
// the caller a pre-write image, which is what made SQLite read page 1 with a
// stale schema cookie and lose the table its previous statement created.
func TestClassicReadRetriesAShortLaggingBody(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		// First two answers are the lagging replica (only 4 KiB present);
		// the third is caught up.
		if n < 3 {
			_, _ = w.Write([]byte(strings.Repeat("A", 4096)))
			return
		}
		_, _ = w.Write([]byte(strings.Repeat("B", 8192)))
	}))
	defer srv.Close()

	br := NewBridge(Config{Client: client.New(srv.URL, ""), RemoteRoot: "/"})
	const ino = meta.Ino(61)
	// This mount believes the file is 8 KiB: it wrote that much itself.
	br.mu.Lock()
	br.sizes[ino] = 8192
	br.paths[ino] = "/t.db"
	br.mu.Unlock()

	var slices []meta.Slice
	if errno := br.readClassic(context.Background(), ino, "/t.db", 0, 8191, &slices); errno != 0 {
		t.Fatalf("read: %v", errno)
	}
	if calls.Load() < 3 {
		t.Fatalf("fetched %d times; the short (lagging) bodies should have been retried", calls.Load())
	}
	// The returned slice must describe an 8 KiB image: a 4 KiB lagging body
	// would have produced a 4 KiB slice.
	if len(slices) != 1 || slices[0].Len != 8192 {
		t.Fatalf("slice length = %v, want one 8192-byte slice (a short body was accepted)",
			func() []uint32 {
				out := make([]uint32, 0, len(slices))
				for _, s := range slices {
					out = append(out, s.Len)
				}
				return out
			}())
	}
}

// With a pending write the local overlay is newer than the remote, so a short
// body is legitimate and must NOT trigger retries: the overlay fills the rest.
func TestClassicReadDoesNotRetryWhenWritesArePending(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte(strings.Repeat("A", 4096)))
	}))
	defer srv.Close()

	br := NewBridge(Config{Client: client.New(srv.URL, ""), RemoteRoot: "/"})
	const ino = meta.Ino(62)
	// Stage a REAL local block so the overlay can serve it: the point of this
	// test is only that a short remote body is legitimate when local writes
	// are unpublished.
	payload := []byte(strings.Repeat("Z", 4096))
	sum := sha256Hex(payload)
	br.blocks.stagePinned(sum, payload, true)
	br.mu.Lock()
	br.sizes[ino] = 8192
	br.paths[ino] = "/t.db"
	br.pending[ino] = append(br.pending[ino], pendingWrite{chunk: 0, off: 4096, size: 4096, checksum: sum})
	br.mu.Unlock()

	var slices []meta.Slice
	if errno := br.readClassic(context.Background(), ino, "/t.db", 0, 8191, &slices); errno != 0 {
		t.Fatalf("read: %v", errno)
	}
	if calls.Load() != 1 {
		t.Fatalf("fetched %d times with pending local writes; a short remote body is expected there", calls.Load())
	}
}
