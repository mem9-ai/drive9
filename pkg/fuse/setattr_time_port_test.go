// v1 setattr_time_override_test.go, retargeted: the inode-table rows are a
// direct port (the table kept its v1 API); the pending-commit-wait rows
// move to the writeback extension's SetAttr interception; the facade row
// pins utimensat visibility through GetAttr.
package fuse

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestLocalTimeOverrideLifecycle: override arming, derived-update
// immunity, listing-reseed preservation, user-mutation clearing,
// delete+recreate freshness, identity-replacement adoption, and
// kind-change handling. Every row keeps the v1 semantics except the
// kind-change one: a kind change now allocates a fresh inode instead of
// retyping the live entry (reusableInoAtLocked).
func TestPortLocalTimeOverrideLifecycle(t *testing.T) {
	inodes := NewInodeToPath()

	ino := inodes.Lookup("/a.txt", false, 5, time.Unix(1000, 0))
	requested := time.Unix(1234567890, 0)
	inodes.SetLocalMtime(ino, requested)
	inodes.SetLocalAtime(ino, requested.Add(time.Second))

	if !inodes.HasMtimeOverride(ino) {
		t.Fatal("override not armed after SetLocalMtime")
	}
	// Derived updates lose to the override.
	inodes.UpdateMtimeDerived(ino, time.Unix(9999999999, 0))
	entry, ok := inodes.GetEntry(ino)
	if !ok || !entry.Mtime.Equal(requested) || !entry.Atime.Equal(requested.Add(time.Second)) {
		t.Fatalf("derived update clobbered the override: mtime=%v atime=%v", entry.Mtime, entry.Atime)
	}
	// Listing reseeds preserve the override even when the listing time is
	// newer.
	inodes.Lookup("/a.txt", false, 5, time.Unix(9999999999, 0))
	inodes.LookupWithIdentity("/a.txt", "res-1", 1, false, 5, time.Unix(9999999999, 0))
	entry, _ = inodes.GetEntry(ino)
	if !entry.Mtime.Equal(requested) {
		t.Fatalf("listing reseed clobbered the override: %v", entry.Mtime)
	}

	// A user mutation clears the override; derived updates apply again.
	writeTime := time.Unix(2000000000, 0)
	inodes.UpdateMtime(ino, writeTime)
	if inodes.HasMtimeOverride(ino) {
		t.Fatal("override survived a user mutation")
	}
	inodes.UpdateMtimeDerived(ino, time.Unix(9999999999, 0))
	entry, _ = inodes.GetEntry(ino)
	if !entry.Mtime.Equal(time.Unix(9999999999, 0)) {
		t.Fatalf("derived update did not apply after override cleared: %v", entry.Mtime)
	}

	// Delete + recreate gets a fresh inode without the override.
	inodes.SetLocalMtime(ino, requested)
	inodes.Remove("/a.txt")
	ino2 := inodes.Lookup("/a.txt", false, 5, time.Unix(1000, 0))
	if ino2 == ino {
		t.Fatal("recreated path reused the old inode number")
	}
	if inodes.HasMtimeOverride(ino2) {
		t.Fatal("recreated path inherited the time override")
	}
	if _, ok := inodes.MtimeOverride(ino); ok {
		t.Fatal("stale inode still reports an override")
	}

	// A remote replacement (different resource id) adopts the replacement's
	// listing metadata and drops both overrides; binding the FIRST identity
	// is create-shaped and keeps them.
	inodes.LookupWithIdentity("/a.txt", "res-1", 1, false, 5, time.Unix(2000, 0))
	inodes.SetLocalMtime(ino2, requested)
	inodes.SetLocalAtime(ino2, requested.Add(time.Minute))
	if !inodes.HasMtimeOverride(ino2) || !inodes.HasAtimeOverride(ino2) {
		t.Fatal("overrides not armed before identity replacement")
	}
	replacementMtime := time.Unix(8888888888, 0)
	inodes.LookupWithIdentity("/a.txt", "res-2", 1, false, 5, replacementMtime)
	if inodes.HasMtimeOverride(ino2) {
		t.Fatal("identity replacement inherited the mtime override")
	}
	if inodes.HasAtimeOverride(ino2) {
		t.Fatal("identity replacement inherited the atime override")
	}
	entry, _ = inodes.GetEntry(ino2)
	if !entry.Mtime.Equal(replacementMtime) {
		t.Fatalf("replacement mtime = %v, want listing mtime %v", entry.Mtime, replacementMtime)
	}
	inodes.UpdateAtime(ino2, time.Unix(1, 0))
	entry, _ = inodes.GetEntry(ino2)
	if !entry.Atime.Equal(time.Unix(1, 0)) {
		t.Fatal("atime override survived identity replacement")
	}

	// A kind change at the same path is a different object: the name moves
	// to a fresh inode (the kernel still holds the old nodeid alive and
	// rejects a reply that retypes it — the create-after-rmdir EIO), while
	// the replaced entry keeps its override and hardlink identity for its
	// remaining references (open fds, aliases at other paths).
	inodes.SetLocalMtime(ino2, requested)
	dirIno := inodes.Lookup("/a.txt", true, 0, time.Unix(7777777777, 0))
	if dirIno == ino2 {
		t.Fatal("kind change reused the replaced object's inode")
	}
	if inodes.HasMtimeOverride(dirIno) {
		t.Fatal("fresh inode inherited the mtime override")
	}
	entry, _ = inodes.GetEntry(dirIno)
	if !entry.Mtime.Equal(time.Unix(7777777777, 0)) {
		t.Fatalf("kind-change mtime = %v, want listing mtime", entry.Mtime)
	}
	if !inodes.HasMtimeOverride(ino2) {
		t.Fatal("detached entry lost the override its remaining references still serve")
	}
	if rejoined := inodes.LookupWithIdentity("/elsewhere.txt", "res-2", 1, false, 5, time.Unix(1000, 0)); rejoined != ino2 {
		t.Fatal("replaced object lost its byID identity for surviving aliases")
	}
}

// v1 TestStatIdentityReplacementAdoptsReplacementMtime (facade half):
// GetAttr serves the armed utimensat time; after the identity changes
// remotely, the replacement's server mtime wins.
func TestPortGetAttrServesTimeOverride(t *testing.T) {
	var mtime atomic.Int64
	mtime.Store(time.Now().Unix())
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/":
			// The facade's ancestor search stats the root: report a real
			// traversable directory (the fixture previously looked like a
			// 0644 file, which no caller can search). The mode header is
			// decimal, as the server writes it.
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Mode", "493") // 0o755
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.Header().Set("X-Dat9-Mtime", time.Unix(mtime.Load(), 0).Format(time.RFC3339))
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{Extensions: []fs.Handler{terminal}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	terminal.Attach(f.Ctx.DirCache.(core.NamespaceCache))

	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "timed.txt", &lout); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	ino := lout.NodeId

	// utimensat: the facade arms the local override before dispatch
	// (SetAttrIn embeds its header; set it through the embedded struct).
	requested := time.Unix(1234567890, 0)
	sin := &gofuse.SetAttrIn{}
	sin.InHeader = gofuse.InHeader{NodeId: ino}
	sin.Mtime = uint64(requested.Unix())
	sin.Mtimensec = uint32(requested.Nanosecond())
	sin.Valid = gofuse.FATTR_MTIME
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, sin, &aout); st != gofuse.OK {
		t.Fatalf("setattr time: %v", st)
	}
	if !time.Unix(int64(aout.Mtime), 0).Equal(requested) {
		t.Fatalf("reply mtime = %v, want the requested %v", time.Unix(int64(aout.Mtime), 0), requested)
	}
	// The override keeps serving across remote stat refreshes (the server
	// mtime is "now", newer than the requested time).
	mtime.Store(time.Now().Add(time.Hour).Unix())
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &aout); st != gofuse.OK {
		t.Fatalf("getattr: %v", st)
	}
	if !time.Unix(int64(aout.Mtime), 0).Equal(requested) {
		t.Fatalf("override lost after refresh: %v, want %v", time.Unix(int64(aout.Mtime), 0), requested)
	}
}
