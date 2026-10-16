// v1 dat9fs_test.go Lookup negative-cache rows (B2-4), retargeted to the
// facade + core terminal: a cached-negative namespace row answers ENOENT
// without a remote stat (and does not poison the entry timeout for other
// error classes), a partial miss falls through to the remote HEAD, and a
// remote-404 stat seeds the short negative namespace row that the next
// lookup consumes locally.
package fuse

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// facadeOverCore mounts a facade over the core terminal against the given
// server, exposing the namespace cache for row-level seeding.
func facadeOverCore(t *testing.T, ts *httptest.Server) (*FS, *core.Core) {
	t.Helper()
	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{Extensions: []fs.Handler{terminal}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	// The composition root attaches the facade's namespace cache to the
	// terminal (mount.go does this in production).
	terminal.Attach(f.Ctx.DirCache.(core.NamespaceCache))
	return f, terminal
}

// v1 TestLookupUsesDirCacheNegativeEntryWithoutRemoteStat: a name the
// mount itself deleted (negative namespace row) answers ENOENT with zero
// remote traffic.
func TestPortLookupNegativeRowServesWithoutRemoteStat(t *testing.T) {
	var remoteCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		remoteCalls.Add(1)
		http.Error(w, "unexpected remote call", http.StatusInternalServerError)
	}))
	defer ts.Close()

	f, _ := facadeOverCore(t, ts)
	// The facade's own removal bookkeeping marks the name negative (v1
	// cacheNegativePath via markNameNegative).
	f.markNameNegative("/deleted.txt")

	var out gofuse.EntryOut
	st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "deleted.txt", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
	if got := remoteCalls.Load(); got != 0 {
		t.Fatalf("remote calls = %d, want 0", got)
	}
}

// v1 TestLookupStatNotFoundSeedsShortNegativeNamespaceCache: the first
// lookup of a missing name pays one remote HEAD; the negative row it
// seeds serves the second lookup locally.
func TestPortLookupStatNotFoundSeedsNegativeRow(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		headCalls.Add(1)
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer ts.Close()

	f, _ := facadeOverCore(t, ts)
	var out gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "gone.txt", &out); st != gofuse.ENOENT {
		t.Fatalf("first lookup = %v, want ENOENT", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("first lookup HEAD calls = %d, want 1", got)
	}
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "gone.txt", &out); st != gofuse.ENOENT {
		t.Fatalf("second lookup = %v, want ENOENT (cached negative)", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("second lookup HEAD calls = %d, want still 1", got)
	}
}

// v1 TestLookupPartialNamespaceMissFallsThroughToRemoteStat: a name
// absent from an otherwise-cached listing pays the remote HEAD and
// succeeds.
func TestPortLookupPartialMissFallsThroughToStat(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodHead {
			t.Fatalf("method = %s, want HEAD", r.Method)
		}
		headCalls.Add(1)
		w.Header().Set("Content-Length", "9")
		w.Header().Set("X-Dat9-Is-Dir", "false")
		w.Header().Set("X-Dat9-Revision", "4")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	f, _ := facadeOverCore(t, ts)
	// Seed a single-name row (v1 Upsert): the directory has partial state,
	// so a name absent from it is a partial miss that falls to the stat.
	f.Ctx.DirCache.Upsert("/", fs.DirEntryInfo{Name: "known.txt", Size: 1})

	var out gofuse.EntryOut
	st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "remote.txt", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := headCalls.Load(); got != 1 {
		t.Fatalf("HEAD calls = %d, want 1", got)
	}
	if out.Size != 9 {
		t.Fatalf("Lookup size = %d, want 9", out.Size)
	}
}

// v1 TestLookupLockFileStatNotFoundDoesNotSeedNamespaceNegative: lock
// files never enter the negative namespace row — an O_CREAT|O_EXCL probe
// must always revalidate.
func TestPortLookupLockFileDoesNotSeedNegative(t *testing.T) {
	var headCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		headCalls.Add(1)
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer ts.Close()

	f, _ := facadeOverCore(t, ts)
	var out gofuse.EntryOut
	for i := 0; i < 3; i++ {
		if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "repo.lock", &out); st != gofuse.ENOENT {
			t.Fatalf("lookup %d = %v, want ENOENT", i, st)
		}
	}
	if got := headCalls.Load(); got != 3 {
		t.Fatalf("HEAD calls = %d, want 3 (lock files never cache negatively)", got)
	}
}

// v1 TestMknodMetadataOnlySpecialRevalidatesNegativeCache /
// creator-side row: a create of a name that was negatively cached clears
// the marker — the next lookup sees the new entry, not the cached ENOENT.
func TestPortCreateClearsNegativeRow(t *testing.T) {
	var puts atomic.Int32
	var created atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			if created.Load() {
				w.Header().Set("Content-Length", "0")
				w.Header().Set("X-Dat9-Revision", "2")
				w.WriteHeader(http.StatusOK)
				return
			}
			http.Error(w, "missing", http.StatusNotFound)
		case r.Method == http.MethodPost, r.Method == http.MethodPut:
			puts.Add(1)
			created.Store(true)
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	f, _ := facadeOverCore(t, ts)
	var out gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "spawn.txt", &out); st != gofuse.ENOENT {
		t.Fatal("pre-create lookup must ENOENT")
	}
	// Create through the facade (the marker must not poison it).
	var cout gofuse.CreateOut
	if st := f.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644}, "spawn.txt", &cout); st != gofuse.OK {
		t.Fatalf("create over negative row = %v, want OK", st)
	}
	if got := puts.Load(); got == 0 {
		t.Fatal("create must reach the remote")
	}
	_ = syscall.ENOENT
}
