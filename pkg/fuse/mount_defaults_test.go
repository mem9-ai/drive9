package fuse

// Default-value rows lost when v1 pkg/fuse/shadow_test.go moved to
// pkg/fusecomp: the WriteCacheSizeMB knob trio, the profile-dependent
// kernel cache TTLs, the parallel-read block default (1MiB), and the
// per-file readdir-prefetch cap (50KB). The TTL and write-cache rows pin
// the mount-level resolvers; the 1MiB row observes the cache extension's
// default through the exported prefetcher (a 1MiB range request after the
// window grows past the block size); the 50KB row pins the shared
// writebuffer threshold the cache cap clamps to.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// v1 setDefaults rows: 60s attr/entry and 10s listings by default,
// coding-agent drops attr to 30s, interactive drops attr/entry to 1s and
// listings to 2s; explicit positive values always win.
func TestMountDefaultKernelTTLs(t *testing.T) {
	rows := []struct {
		name                string
		profile             string
		attr, entry, dir    time.Duration
		wantAttr, wantEntry time.Duration
		wantDir             time.Duration
	}{
		{"default", "", 0, 0, 0, 60 * time.Second, 60 * time.Second, 10 * time.Second},
		{"coding-agent", "coding-agent", 0, 0, 0, 30 * time.Second, 60 * time.Second, 10 * time.Second},
		{"interactive", "interactive", 0, 0, 0, 1 * time.Second, 1 * time.Second, 2 * time.Second},
		{"explicit wins", "", 5 * time.Second, 6 * time.Second, 7 * time.Second, 5 * time.Second, 6 * time.Second, 7 * time.Second},
		{"explicit wins on profile", "interactive", 5 * time.Second, 6 * time.Second, 7 * time.Second, 5 * time.Second, 6 * time.Second, 7 * time.Second},
		{"negative uses default", "", -1, -1, -1, 60 * time.Second, 60 * time.Second, 10 * time.Second},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			attr, entry, dir := resolveKernelTTLDefaults(row.profile, row.attr, row.entry, row.dir)
			if attr != row.wantAttr || entry != row.wantEntry || dir != row.wantDir {
				t.Fatalf("TTLs = %s/%s/%s, want %s/%s/%s", attr, entry, dir, row.wantAttr, row.wantEntry, row.wantDir)
			}
		})
	}
}

// v1 TestSetDefaultsWriteCacheSizeMB* rows: zero uses the 1024 MiB
// default, an explicit value is kept, negative disables the byte quota.
func TestMountDefaultWriteCacheSizeMB(t *testing.T) {
	if got := resolveWriteCacheSizeMB(0); got != 1024 {
		t.Fatalf("WriteCacheSizeMB default = %d, want 1024", got)
	}
	if got := resolveWriteCacheSizeMB(2048); got != 2048 {
		t.Fatalf("WriteCacheSizeMB explicit = %d, want 2048", got)
	}
	if got := resolveWriteCacheSizeMB(-1); got != 0 {
		t.Fatalf("WriteCacheSizeMB negative = %d, want 0 (disabled)", got)
	}
}

// The cache extension's per-file readdir-prefetch cap defaults to (and
// never exceeds) writebuffer.SmallFileThreshold — 50KB, not a 512KB guess.
// The clamp itself lives in pkg/extensions/cache; this pins the shared
// constant the mount contract depends on.
func TestMountDefaultPrefetchPerFileCapIsSmallFileThreshold(t *testing.T) {
	if writebuffer.SmallFileThreshold != 50_000 {
		t.Fatalf("SmallFileThreshold = %d, want 50000 (50KB per-file prefetch cap)", writebuffer.SmallFileThreshold)
	}
}

// rangeRecorder captures the lengths of Range requests hitting the backend.
type rangeRecorder struct {
	inner  *testbackend.Backend
	mu     sync.Mutex
	length []int64
}

func (r *rangeRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if rng := req.Header.Get("Range"); rng != "" && req.Method == http.MethodGet {
		var lo, hi int64
		if n, _ := fmt.Sscanf(rng, "bytes=%d-%d", &lo, &hi); n == 2 && hi >= lo {
			r.mu.Lock()
			r.length = append(r.length, hi-lo+1)
			r.mu.Unlock()
		}
	}
	r.inner.ServeHTTP(w, req)
}

func (r *rangeRecorder) hasLength(want int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.length {
		if l == want {
			return true
		}
	}
	return false
}

// The parallel-read block default is 1MiB: a prefetcher armed with the
// zero mount knobs (SetParallelRead(0,0) keeps the defaults) fetches the
// grown window as exactly-1MiB range requests.
func TestMountDefaultParallelReadBlockSize(t *testing.T) {
	rec := &rangeRecorder{inner: testbackend.NewBackend()}
	rec.inner.Seed("/big", make([]byte, 4<<20))
	server := httptest.NewServer(rec)
	t.Cleanup(server.Close)
	cl := client.New(server.URL, "")
	cl.Warm(context.Background())

	p := cache.NewPrefetcher(cl, "/big", 4<<20)
	t.Cleanup(p.Close)
	p.SetParallelRead(0, 0) // zeros keep the defaults (4 x 1MiB)

	// Two sequential 512KB reads grow the adaptive window to 1MiB; the
	// prefetch of the next region then fetches exactly one 1MiB block.
	p.OnRead(0, 512*1024)
	p.OnRead(512*1024, 512*1024)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec.hasLength(1 << 20) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	t.Fatalf("no 1MiB range request observed; saw lengths %v", rec.length)
}
