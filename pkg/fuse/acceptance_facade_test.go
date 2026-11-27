package fuse

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/httperr"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func newFacadeWritebackTestStack(t *testing.T, policy writeback.Policy) (*FS, *writeback.Writeback, *testbackend.Backend) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)

	bus := fs.NewEventBus()
	wb, err := writeback.New(writeback.Options{
		Policy:     policy,
		Client:     cl,
		WriteCache: wbcache,
		Staging:    ledger,
		Events:     bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })

	sp, err := specials.New(specials.Options{Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	term, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}

	f, err := New(Options{
		Extensions:         []fs.Handler{sp, wb, term},
		Events:             bus,
		DefaultPermissions: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wb.SetInodeTable(f.Ctx.Inodes)
	wb.SetSurvivorLookup(f.SurvivorAlias)
	return f, wb, b
}

// TestFacadeReopenUnflushedCreateWritableAndFlock verifies the full FUSE
// facade flow for Issue 1 (Rust std::fs::File::create + OpenOptions::write +
// shared/exclusive flock across two open file handles without closing f1).
func TestFacadeReopenUnflushedCreateWritableAndFlock(t *testing.T) {
	for _, policy := range []writeback.Policy{writeback.PolicyWriteBack, writeback.PolicyCloseSync} {
		name := "writeback"
		if policy == writeback.PolicyCloseSync {
			name = "close-sync"
		}
		t.Run(name, func(t *testing.T) {
			f, wb, b := newFacadeWritebackTestStack(t, policy)

			// 1. Create /locktarget (O_CREAT|O_WRONLY|O_TRUNC) and keep fh1 open & unflushed.
			var cOut gofuse.CreateOut
			st := f.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC),
				Mode:     0o644,
			}, "locktarget", &cOut)
			if st != gofuse.OK {
				t.Fatalf("Create locktarget: %v", st)
			}

			// 2. Lookup + Open(O_WRONLY) and Open(O_RDWR) without O_CREAT.
			var eOut gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "locktarget", &eOut); st != gofuse.OK {
				t.Fatalf("Lookup locktarget while fh1 open: %v", st)
			}
			if eOut.NodeId != cOut.NodeId {
				t.Fatalf("Lookup NodeId=%d, Create NodeId=%d", eOut.NodeId, cOut.NodeId)
			}

			var oOut gofuse.OpenOut
			if st := f.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: cOut.NodeId},
				Flags:    uint32(syscall.O_RDWR),
			}, &oOut); st != gofuse.OK {
				t.Fatalf("Open(O_RDWR) on unflushed Create: %v (want OK, not ENOENT)", st)
			}

			// 3. Shared flock on fh1 and fh2 both succeed; exclusive flock on fh2 conflicts with EAGAIN.
			flockShared := func(fh, owner uint64) *gofuse.LkIn {
				return &gofuse.LkIn{
					InHeader: gofuse.InHeader{NodeId: cOut.NodeId, Caller: gofuse.Caller{Pid: 100}},
					Fh:       fh,
					Owner:    owner,
					LkFlags:  gofuse.FUSE_LK_FLOCK,
					Lk:       gofuse.FileLock{Start: 0, End: (1 << 63) - 1, Typ: syscall.F_RDLCK, Pid: 100},
				}
			}
			flockExcl := func(fh, owner uint64) *gofuse.LkIn {
				in := flockShared(fh, owner)
				in.Lk.Typ = syscall.F_WRLCK
				return in
			}
			flockUn := func(fh, owner uint64) *gofuse.LkIn {
				in := flockShared(fh, owner)
				in.Lk.Typ = syscall.F_UNLCK
				return in
			}

			if st := f.SetLk(nil, flockShared(cOut.Fh, 1001)); st != gofuse.OK {
				t.Fatalf("fh1 shared flock: %v", st)
			}
			if st := f.SetLk(nil, flockShared(oOut.Fh, 1002)); st != gofuse.OK {
				t.Fatalf("fh2 shared flock: %v", st)
			}
			if st := f.SetLk(nil, flockExcl(oOut.Fh, 1002)); st != gofuse.EAGAIN {
				t.Fatalf("fh2 exclusive flock over fh1 shared = %v, want EAGAIN", st)
			}
			if st := f.SetLk(nil, flockUn(cOut.Fh, 1001)); st != gofuse.OK {
				t.Fatalf("fh1 unlock: %v", st)
			}
			if st := f.SetLk(nil, flockExcl(oOut.Fh, 1002)); st != gofuse.OK {
				t.Fatalf("fh2 upgrade to exclusive flock after fh1 unlock: %v", st)
			}

			// 4. Write through fh1, read back through fh2 before flush/close!
			written, wst := f.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: cOut.NodeId},
				Fh:       cOut.Fh, Offset: 0, Size: 5,
			}, []byte("locks"))
			if wst != gofuse.OK || written != 5 {
				t.Fatalf("Write fh1 = (%d, %v)", written, wst)
			}
			rr, rst := f.Read(nil, &gofuse.ReadIn{
				InHeader: gofuse.InHeader{NodeId: cOut.NodeId},
				Fh:       oOut.Fh, Offset: 0, Size: 16,
			}, make([]byte, 16))
			if rst != gofuse.OK {
				t.Fatalf("Read fh2: %v", rst)
			}
			rb, _ := rr.Bytes(make([]byte, 16))
			if string(rb) != "locks" {
				t.Fatalf("Read fh2 = %q, want locks", string(rb))
			}

			// 5. Close both handles and verify remote durability & clean drain.
			if st := f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: cOut.NodeId}, Fh: oOut.Fh, LockOwner: 1002}); st != gofuse.OK {
				t.Fatalf("Flush fh2: %v", st)
			}
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: cOut.NodeId}, Fh: oOut.Fh, LockOwner: 1002})
			if st := f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: cOut.NodeId}, Fh: cOut.Fh, LockOwner: 1001}); st != gofuse.OK {
				t.Fatalf("Flush fh1: %v", st)
			}
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: cOut.NodeId}, Fh: cOut.Fh, LockOwner: 1001})

			if err := wb.Drain(context.Background()); err != nil {
				t.Fatalf("Drain: %v", err)
			}
			if reason, count := wb.DrainPending(); reason != "" || count != 0 {
				t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
			}
			if n, ok := b.Node("/locktarget"); !ok || string(n.Data) != "locks" {
				t.Fatalf("remote /locktarget = (%v, %q), want locks", ok, n.Data)
			}
		})
	}
}

// TestFacadeRejectsInvalidNamespaceNamesWithoutPollutingDrain verifies Issue 3
// at the FUSE facade boundary across both writeback and close-sync policies:
// control characters (0x04), backslashes (D:\abspath), and invalid UTF-8
// (\xff\xfe) are rejected synchronously with EINVAL at Create, Mkdir, Mknod,
// Symlink (link name only; relative symlink target ../ok is allowed), Link,
// and Rename, allocating zero inodes and leaving DrainPending() clean.
func TestFacadeRejectsInvalidNamespaceNamesWithoutPollutingDrain(t *testing.T) {
	for _, policy := range []writeback.Policy{writeback.PolicyWriteBack, writeback.PolicyCloseSync} {
		name := "writeback"
		if policy == writeback.PolicyCloseSync {
			name = "close-sync"
		}
		t.Run(name, func(t *testing.T) {
			f, wb, _ := newFacadeWritebackTestStack(t, policy)

			// Seed one valid file so Link and Rename have a valid source inode.
			var validOut gofuse.CreateOut
			if st := f.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
				Mode:     0o644,
			}, "valid.txt", &validOut); st != gofuse.OK {
				t.Fatalf("Create valid.txt: %v", st)
			}
			_, _ = f.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: validOut.NodeId}, Fh: validOut.Fh, Size: 2}, []byte("ok"))
			_ = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: validOut.NodeId}, Fh: validOut.Fh})
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: validOut.NodeId}, Fh: validOut.Fh})
			if err := wb.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}

			// Valid symlink with a relative "../valid.txt" target MUST still succeed.
			var symOut gofuse.EntryOut
			if st := f.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "../valid.txt", "rel.link", &symOut); st != gofuse.OK {
				t.Fatalf("Symlink with relative pointedTo failed: %v", st)
			}

			badNames := []string{
				"=\xd8\x04\xdc",
				`D:\abspath`,
				"bad\xff\xfe",
			}
			wantEINVAL := gofuse.Status(syscall.EINVAL)
			for _, bad := range badNames {
				var cOut gofuse.CreateOut
				if st := f.Create(nil, &gofuse.CreateIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
					Mode:     0o644,
				}, bad, &cOut); st != wantEINVAL {
					t.Fatalf("Create(%q) = %v, want EINVAL", bad, st)
				}

				var dirOut gofuse.EntryOut
				if st := f.Mkdir(nil, &gofuse.MkdirIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Mode:     0o755,
				}, bad, &dirOut); st != wantEINVAL {
					t.Fatalf("Mkdir(%q) = %v, want EINVAL", bad, st)
				}

				var nodOut gofuse.EntryOut
				if st := f.Mknod(nil, &gofuse.MknodIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Mode:     syscall.S_IFIFO | 0o644,
				}, bad, &nodOut); st != wantEINVAL {
					t.Fatalf("Mknod(%q) = %v, want EINVAL", bad, st)
				}

				var badSymOut gofuse.EntryOut
				if st := f.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "valid.txt", bad, &badSymOut); st != wantEINVAL {
					t.Fatalf("Symlink(%q) = %v, want EINVAL", bad, st)
				}

				var linkOut gofuse.EntryOut
				if st := f.Link(nil, &gofuse.LinkIn{
					InHeader:  gofuse.InHeader{NodeId: 1},
					Oldnodeid: validOut.NodeId,
				}, bad, &linkOut); st != wantEINVAL {
					t.Fatalf("Link(%q) = %v, want EINVAL", bad, st)
				}

				if st := f.Rename(nil, &gofuse.RenameIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Newdir:   1,
				}, "valid.txt", bad); st != wantEINVAL {
					t.Fatalf("Rename(valid.txt -> %q) = %v, want EINVAL", bad, st)
				}

				if _, exists := f.inodes.GetInode("/" + bad); exists {
					t.Fatalf("invalid path /%q allocated an inode", bad)
				}
			}

			// Decomposed Unicode (NFD) must be canonicalized to NFC across Create,
			// Lookup, Open, Rename, and Unlink so NFD callers never hit ENOENT or
			// leave stranded staging behind on Unlink.
			var nfdOut gofuse.CreateOut
			if st := f.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
				Mode:     0o644,
			}, "cafe\u0301.txt", &nfdOut); st != gofuse.OK {
				t.Fatalf("Create NFD name: %v", st)
			}
			// Unlanded Lookup with NFD spelling must find the NFC-registered inode.
			var nfdLookup gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "cafe\u0301.txt", &nfdLookup); st != gofuse.OK {
				t.Fatalf("Lookup NFD while unlanded: %v, want OK", st)
			}
			if nfdLookup.NodeId != nfdOut.NodeId {
				t.Fatalf("Lookup NFD NodeId = %d, want %d", nfdLookup.NodeId, nfdOut.NodeId)
			}
			// Secondary Open on that inode while still unlanded must also succeed.
			var nfdOpen2 gofuse.OpenOut
			if st := f.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: nfdOut.NodeId},
				Flags:    uint32(syscall.O_RDWR),
			}, &nfdOpen2); st != gofuse.OK {
				t.Fatalf("secondary Open NFD while unlanded: %v", st)
			}
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: nfdOut.NodeId}, Fh: nfdOpen2.Fh})

			if _, st := f.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: nfdOut.NodeId},
				Fh:       nfdOut.Fh,
				Offset:   0,
				Size:     4,
			}, []byte("cafe")); st != gofuse.OK {
				t.Fatalf("Write NFD: %v", st)
			}
			_ = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: nfdOut.NodeId}, Fh: nfdOut.Fh})
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: nfdOut.NodeId}, Fh: nfdOut.Fh})
			if _, ok := f.inodes.GetInode("/caf\u00e9.txt"); !ok {
				t.Fatal("expected NFC canonical inode path /café.txt")
			}
			if _, ok := f.inodes.GetInode("/cafe\u0301.txt"); ok {
				t.Fatal("unexpected un-normalized NFD inode key /cafe\\u0301.txt")
			}
			// Self-rename with NFD oldName -> NFC newName must be a clean no-op
			// and keep the canonical inode reachable.
			if st := f.Rename(nil, &gofuse.RenameIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Newdir:   1,
			}, "cafe\u0301.txt", "caf\u00e9.txt"); st != gofuse.OK {
				t.Fatalf("self-Rename NFD->NFC: %v", st)
			}
			var nfcAfterRename gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "caf\u00e9.txt", &nfcAfterRename); st != gofuse.OK {
				t.Fatalf("Lookup NFC after self-rename: %v, want OK", st)
			}
			if nfcAfterRename.NodeId != nfdOut.NodeId {
				t.Fatalf("Lookup NFC NodeId after self-rename = %d, want %d", nfcAfterRename.NodeId, nfdOut.NodeId)
			}
			// Unlink with NFD spelling must remove the canonical inode, make NFC
			// Lookup fail with ENOENT, and purge staging cleanly.
			if st := f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "cafe\u0301.txt"); st != gofuse.OK {
				t.Fatalf("Unlink NFD: %v", st)
			}
			if _, ok := f.inodes.GetInode("/caf\u00e9.txt"); ok {
				t.Fatal("expected canonical inode /café.txt to be removed after NFD Unlink")
			}
			var nfcAfterUnlink gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "caf\u00e9.txt", &nfcAfterUnlink); st != gofuse.ENOENT {
				t.Fatalf("Lookup NFC after NFD Unlink = %v, want ENOENT", st)
			}
			if reason, count := wb.DrainPending(); reason != "" || count != 0 {
				t.Fatalf("DrainPending after NFD Unlink = (%q, %d), want (\"\", 0)", reason, count)
			}

			// Mixed Unicode spelling ("ée\u0301.txt" -> canonical "/éé.txt"):
			// positive Lookup of the mixed spelling followed by NFC Unlink must
			// invalidate both the mixed spelling and full NFD ("e\u0301e\u0301.txt").
			const (
				mixedName = "\u00e9e\u0301.txt"
				nfcDouble = "\u00e9\u00e9.txt"
				nfdDouble = "e\u0301e\u0301.txt"
			)
			var dblCreate gofuse.CreateOut
			if st := f.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Flags:    uint32(syscall.O_CREAT | syscall.O_RDWR),
				Mode:     0o644,
			}, nfcDouble, &dblCreate); st != gofuse.OK {
				t.Fatalf("Create %q: %v", nfcDouble, st)
			}
			_ = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: dblCreate.NodeId}, Fh: dblCreate.Fh})
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: dblCreate.NodeId}, Fh: dblCreate.Fh})

			var mixedOut gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, mixedName, &mixedOut); st != gofuse.OK {
				t.Fatalf("Lookup mixed spelling %q: %v, want OK", mixedName, st)
			}
			if mixedOut.NodeId != dblCreate.NodeId {
				t.Fatalf("Lookup mixed spelling NodeId = %d, want %d", mixedOut.NodeId, dblCreate.NodeId)
			}
			var invalidatedMu sync.Mutex
			invalidated := map[string]bool{}
			f.onInvalidateEntry = func(_ uint64, invName string) {
				invalidatedMu.Lock()
				invalidated[invName] = true
				invalidatedMu.Unlock()
			}
			if st := f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, nfcDouble); st != gofuse.OK {
				t.Fatalf("Unlink NFC %q: %v", nfcDouble, st)
			}
			f.onInvalidateEntry = nil
			invalidatedMu.Lock()
			gotMixedInv := invalidated[mixedName]
			gotNFDInv := invalidated[nfdDouble]
			invalidatedMu.Unlock()
			if !gotMixedInv || !gotNFDInv {
				t.Fatalf("Unlink NFC %q invalidated mixed=%v nfd=%v, want both true", nfcDouble, gotMixedInv, gotNFDInv)
			}
			var mixedAfterUnlink gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, mixedName, &mixedAfterUnlink); st != gofuse.ENOENT {
				t.Fatalf("Lookup mixed spelling after Unlink = %v, want ENOENT", st)
			}

			// Rename with cached alternate Unicode spellings on BOTH source and
			// destination must invalidate alternate source spellings (without
			// dropping oldName itself) and all destination spellings.
			const (
				srcNFC = "ren-\u00e9.txt"
				srcNFD = "ren-e\u0301.txt"
				dstNFC = "dst-\u00e9\u00e9.txt"
				dstMix = "dst-\u00e9e\u0301.txt"
			)
			for _, createName := range []string{srcNFC, dstNFC} {
				var co gofuse.CreateOut
				if st := f.Create(nil, &gofuse.CreateIn{
					InHeader: gofuse.InHeader{NodeId: 1},
					Flags:    uint32(syscall.O_CREAT | syscall.O_RDWR),
					Mode:     0o644,
				}, createName, &co); st != gofuse.OK {
					t.Fatalf("Create %q: %v", createName, st)
				}
				_ = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: co.NodeId}, Fh: co.Fh})
				f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: co.NodeId}, Fh: co.Fh})
			}
			var lo gofuse.EntryOut
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, srcNFD, &lo); st != gofuse.OK {
				t.Fatalf("Lookup %q: %v", srcNFD, st)
			}
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, dstMix, &lo); st != gofuse.OK {
				t.Fatalf("Lookup %q: %v", dstMix, st)
			}
			renInv := map[string]bool{}
			f.onInvalidateEntry = func(_ uint64, invName string) {
				invalidatedMu.Lock()
				renInv[invName] = true
				invalidatedMu.Unlock()
			}
			if st := f.Rename(nil, &gofuse.RenameIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Newdir:   1,
			}, srcNFD, dstNFC); st != gofuse.OK {
				t.Fatalf("Rename(%q -> %q): %v", srcNFD, dstNFC, st)
			}
			f.onInvalidateEntry = nil
			invalidatedMu.Lock()
			gotSrcNFC := renInv[srcNFC]
			gotSrcNFD := renInv[srcNFD]
			gotDstNFC := renInv[dstNFC]
			gotDstMix := renInv[dstMix]
			invalidatedMu.Unlock()
			if !gotSrcNFC || gotSrcNFD || !gotDstNFC || !gotDstMix {
				t.Fatalf("Rename invalidations: srcNFC=%v (want true) srcNFD=%v (want false) dstNFC=%v (want true) dstMix=%v (want true)",
					gotSrcNFC, gotSrcNFD, gotDstNFC, gotDstMix)
			}
			if st := f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, dstNFC); st != gofuse.OK {
				t.Fatalf("cleanup Unlink %q: %v", dstNFC, st)
			}

			// Symlink created with mixed spelling (without intervening Lookup)
			// and descendant inside a renamed directory (/udir -> /umoved) must
			// both invalidate their recorded mixed spellings when replaced.
			const (
				symMix = "sym-\u00e9e\u0301"
				symNFC = "sym-\u00e9\u00e9"
			)
			var symMixOut gofuse.EntryOut
			if st := f.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "/target", symMix, &symMixOut); st != gofuse.OK {
				t.Fatalf("Symlink %q: %v", symMix, st)
			}
			var replCreate gofuse.CreateOut
			if st := f.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Flags:    uint32(syscall.O_CREAT | syscall.O_RDWR),
				Mode:     0o644,
			}, "repl.txt", &replCreate); st != gofuse.OK {
				t.Fatalf("Create repl.txt: %v", st)
			}
			_ = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: replCreate.NodeId}, Fh: replCreate.Fh})
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: replCreate.NodeId}, Fh: replCreate.Fh})
			symInv := map[string]bool{}
			f.onInvalidateEntry = func(_ uint64, invName string) {
				invalidatedMu.Lock()
				symInv[invName] = true
				invalidatedMu.Unlock()
			}
			if st := f.Rename(nil, &gofuse.RenameIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Newdir:   1,
			}, "repl.txt", symNFC); st != gofuse.OK {
				t.Fatalf("Rename over symlink %q: %v", symNFC, st)
			}
			f.onInvalidateEntry = nil
			invalidatedMu.Lock()
			gotSymMix := symInv[symMix]
			invalidatedMu.Unlock()
			if !gotSymMix {
				t.Fatalf("Rename over %q did not invalidate mixed Symlink spelling %q", symNFC, symMix)
			}
			_ = f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, symNFC)

			// Directory rename /udir -> /umoved keeps child spelling records
			// keyed by stable parentIno.
			var dirOut gofuse.EntryOut
			if st := f.Mkdir(nil, &gofuse.MkdirIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Mode:     0o755,
			}, "udir", &dirOut); st != gofuse.OK {
				t.Fatalf("Mkdir udir: %v", st)
			}
			for _, cn := range []string{nfcDouble, "src.txt"} {
				var co gofuse.CreateOut
				if st := f.Create(nil, &gofuse.CreateIn{
					InHeader: gofuse.InHeader{NodeId: dirOut.NodeId},
					Flags:    uint32(syscall.O_CREAT | syscall.O_RDWR),
					Mode:     0o644,
				}, cn, &co); st != gofuse.OK {
					t.Fatalf("Create /udir/%s: %v", cn, st)
				}
				_ = f.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: co.NodeId}, Fh: co.Fh})
				f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: co.NodeId}, Fh: co.Fh})
			}
			if st := f.Lookup(nil, &gofuse.InHeader{NodeId: dirOut.NodeId}, mixedName, &lo); st != gofuse.OK {
				t.Fatalf("Lookup /udir/%q: %v", mixedName, st)
			}
			if st := f.Rename(nil, &gofuse.RenameIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Newdir:   1,
			}, "udir", "umoved"); st != gofuse.OK {
				t.Fatalf("Rename udir -> umoved: %v", st)
			}
			movedInv := map[string]bool{}
			f.onInvalidateEntry = func(pino uint64, invName string) {
				if pino == dirOut.NodeId {
					invalidatedMu.Lock()
					movedInv[invName] = true
					invalidatedMu.Unlock()
				}
			}
			if st := f.Rename(nil, &gofuse.RenameIn{
				InHeader: gofuse.InHeader{NodeId: dirOut.NodeId},
				Newdir:   dirOut.NodeId,
			}, "src.txt", nfcDouble); st != gofuse.OK {
				t.Fatalf("Rename /umoved/src.txt -> %q: %v", nfcDouble, st)
			}
			f.onInvalidateEntry = nil
			invalidatedMu.Lock()
			gotMovedMix := movedInv[mixedName]
			invalidatedMu.Unlock()
			if !gotMovedMix {
				t.Fatalf("Rename-over inside renamed directory /umoved missed mixed spelling %q", mixedName)
			}
			_ = f.Unlink(nil, &gofuse.InHeader{NodeId: dirOut.NodeId}, nfcDouble)
			_ = f.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "umoved")

			// Sentinel-based client.ErrInvalidPath classification must return EINVAL
			// even when the invalid path text contains words like "not found".
			_, invalidErr := client.New("http://127.0.0.1:1", "").StatCtx(context.Background(), `/not found\bad`)
			if got := httperr.HTTPStatusOf(invalidErr); got != wantEINVAL {
				t.Fatalf("HTTPStatusOf(%v) = %v, want EINVAL", invalidErr, got)
			}

			if err := wb.Drain(context.Background()); err != nil {
				t.Fatalf("Drain after invalid names: %v", err)
			}
			if reason, count := wb.DrainPending(); reason != "" || count != 0 {
				t.Fatalf("DrainPending = (%q, %d), want (\"\", 0)", reason, count)
			}
		})
	}
}
