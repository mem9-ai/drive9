package fuse

// qiffang twenty-second round: chmod advances ctime (not mtime), a data
// write retires explicit utimens times, plain READDIR keeps hardlink
// identity, and the handle fast paths feed the fuse perf counters.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newPerfFS(t *testing.T, b *testbackend.Backend) *FS {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{Extensions: []fs.Handler{terminal}, Uid: 1000, Gid: 1000, Perf: true})
	if err != nil {
		t.Fatal(err)
	}
	terminal.Attach(f.Ctx.DirCache.(core.NamespaceCache))
	return f
}

func lookupFacade(t *testing.T, f *FS, name string) uint64 {
	t.Helper()
	var out gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, name, &out); st != gofuse.OK {
		t.Fatalf("lookup %s: %v", name, st)
	}
	return out.NodeId
}

// A pure chmod must not advance the content mtime (a backup/build tool
// reads it as a content change) and must not clobber an explicit mtime;
// ctime is what advances.
func TestPortChmodAdvancesCtimeNotMtime(t *testing.T) {
	f := newGetAttrFS(t, "/f.txt", "data")
	ino := lookupFacade(t, f, "f.txt")
	stamp := time.Unix(1234567890, 0)

	sin := &gofuse.SetAttrIn{}
	sin.InHeader = gofuse.InHeader{NodeId: ino}
	sin.Valid = gofuse.FATTR_MTIME
	sin.Mtime = uint64(stamp.Unix())
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, sin, &aout); st != gofuse.OK {
		t.Fatalf("utimens setattr = %v", st)
	}

	chmod := &gofuse.SetAttrIn{}
	chmod.InHeader = gofuse.InHeader{NodeId: ino}
	chmod.Valid = gofuse.FATTR_MODE
	chmod.Mode = 0o600
	if st := f.SetAttr(nil, chmod, &aout); st != gofuse.OK {
		t.Fatalf("chmod setattr = %v", st)
	}

	var gout gofuse.AttrOut
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &gout); st != gofuse.OK {
		t.Fatalf("getattr = %v", st)
	}
	if gout.Mtime != uint64(stamp.Unix()) {
		t.Fatalf("mtime = %d, want the explicit %d (chmod must not touch it)", gout.Mtime, stamp.Unix())
	}
	if gout.Ctime == gout.Mtime {
		t.Fatalf("ctime = %d still equal to mtime, want the chmod clock", gout.Ctime)
	}
}

// A same-length overwrite after utimens must advance the served mtime: the
// explicit override is retired at the write boundary and the landed stat
// governs.
func TestPortWriteClearsExplicitMtime(t *testing.T) {
	f := newGetAttrFS(t, "/w.txt", "OLD!")
	ino := lookupFacade(t, f, "w.txt")
	stamp := time.Unix(1234567890, 0)

	sin := &gofuse.SetAttrIn{}
	sin.InHeader = gofuse.InHeader{NodeId: ino}
	sin.Valid = gofuse.FATTR_MTIME
	sin.Mtime = uint64(stamp.Unix())
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, sin, &aout); st != gofuse.OK {
		t.Fatalf("utimens setattr = %v", st)
	}

	var oout gofuse.OpenOut
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &oout); st != gofuse.OK {
		t.Fatalf("open = %v", st)
	}
	if _, st := f.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: oout.Fh}, []byte("NEW!")); st != gofuse.OK {
		t.Fatalf("write = %v", st)
	}
	if st := f.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: oout.Fh}); st != gofuse.OK {
		t.Fatalf("fsync = %v", st)
	}

	var gout gofuse.AttrOut
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &gout); st != gofuse.OK {
		t.Fatalf("getattr = %v", st)
	}
	if gout.Mtime == uint64(stamp.Unix()) {
		t.Fatalf("mtime = %d, want a refreshed clock after the write", gout.Mtime)
	}
}

// Plain READDIR must allocate one FUSE inode for two names sharing the
// server's resource identity (hardlinks), or st_ino disagrees between
// aliases before the first lookup.
func TestPortReadDirKeepsHardlinkIdentity(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("hard"))
	b.SeedHardlink("/a", "/b")
	ts := httptest.NewServer(withHardlinkIdentity(b))
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)
	f.inodes.EnsureInode("/", true, 0, time.Now())

	if got := readdirWalk(t, f, 1, false); len(got) == 0 {
		t.Fatal("empty listing")
	}
	a, okA := f.inodes.GetInode("/a")
	bb, okB := f.inodes.GetInode("/b")
	if !okA || !okB {
		t.Fatalf("/a ino ok=%v, /b ino ok=%v", okA, okB)
	}
	if a != bb {
		t.Fatalf("hardlink inodes differ after READDIR: /a=%d /b=%d", a, bb)
	}
}

// The handle fast path bypasses dispatch: a buffered WRITE/FSYNC must still
// feed the fuse counters, not only the remote rows.
func TestPortHandleFastPathRecordsPerf(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/p.txt", []byte("abc"))
	f := newPerfFS(t, b)
	ino := lookupFacade(t, f, "p.txt")

	var oout gofuse.OpenOut
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &oout); st != gofuse.OK {
		t.Fatalf("open = %v", st)
	}
	if _, st := f.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: oout.Fh}, []byte("xyz")); st != gofuse.OK {
		t.Fatalf("write = %v", st)
	}
	if st := f.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: oout.Fh}); st != gofuse.OK {
		t.Fatalf("fsync = %v", st)
	}
	lines := strings.Join(f.perf.PerfLines(), "\n")
	if !strings.Contains(lines, "perf fuse write count=1") {
		t.Fatalf("fuse write counter missing from handle fast path:\n%s", lines)
	}
	if !strings.Contains(lines, "perf fuse fsync count=1") {
		t.Fatalf("fuse fsync counter missing from handle fast path:\n%s", lines)
	}
}

// withHardlinkIdentity stamps the server's hardlink identity onto the two
// listing rows (the in-memory backend carries no resource ids).
func withHardlinkIdentity(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !r.URL.Query().Has("list") {
			inner.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, r)
		var body struct {
			Entries []map[string]any `json:"entries"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		for _, e := range body.Entries {
			if e["name"] == "a" || e["name"] == "b" {
				e["resource_id"] = "res-shared"
				e["nlink"] = 2
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.Code)
		_ = json.NewEncoder(w).Encode(body)
	})
}
