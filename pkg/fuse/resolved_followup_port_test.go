package fuse

// Follow-ups found in two previously-resolved threads (qiffang's last
// replies had been unanswered): the mount-exit classification must not
// trust the unconditional OnUnmount callback, and the open access gate
// must survive a real Lookup→Open with no recorded mode.

import (
	"errors"
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A remote chmod(000) followed by a REAL Lookup (no manual UpdateMode)
// and a fresh O_RDONLY Open: the inode carries no recorded mode (the
// chain's zero mode is ambiguous), so the gate settles it with a fresh
// stat — the read must be denied instead of sailing through and serving
// the private bytes.
func TestPortOpenAccessGateSurvivesRealLookupChmod000(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/secret", []byte("private"))
	b.SetNodeMode("/secret", 0)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)

	entryOut := gofuse.EntryOut{}
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "secret", &entryOut); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	hdr := gofuse.InHeader{NodeId: entryOut.NodeId}
	hdr.Uid = 1000
	hdr.Gid = 1000
	out := gofuse.OpenOut{}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: 0 /* O_RDONLY */}, &out); st != gofuse.EACCES {
		t.Fatalf("O_RDONLY of remote chmod(000) after real lookup = %v, want EACCES", st)
	}
}

// A nonzero mode IS recorded by the lookup itself now — the gate answers
// from the entry without a fresh stat.
func TestPortLookupRecordsRegularModeForGate(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/wo", []byte("data"))
	b.SetNodeMode("/wo", 0o200)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)

	entryOut := gofuse.EntryOut{}
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "wo", &entryOut); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	entry, ok := f.inodes.GetEntry(entryOut.NodeId)
	if !ok || !entry.HasMode {
		t.Fatalf("regular-file lookup did not record the mode: ok=%v HasMode=%v", ok, entry.HasMode)
	}
	if entry.Mode&0o777 != 0o200 {
		t.Fatalf("recorded mode = %o, want 200", entry.Mode&0o777)
	}
	hdr := gofuse.InHeader{NodeId: entryOut.NodeId}
	hdr.Uid = 1000
	hdr.Gid = 1000
	out := gofuse.OpenOut{}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: uint32(syscall.O_RDWR)}, &out); st != gofuse.EACCES {
		t.Fatalf("O_RDWR of mode 0200 = %v, want EACCES", st)
	}
}

// The serve-exit classification keys on the KERNEL mount row alone: a
// row that survives the settle window is abnormal — even though the
// go-fuse fork's OnUnmount callback fired (it fires unconditionally) —
// and a row that disappears classifies clean.
func TestPortServeExitClassificationIgnoresUnmountCallback(t *testing.T) {
	present := true
	err := classifyServeExit("/mnt/x", func(string) bool { return present })
	var merr *MountExitError
	if !errors.As(err, &merr) || merr.ExitCode() != ExitServeAbnormal {
		t.Fatalf("surviving mount row classified %v, want ExitServeAbnormal", err)
	}
	present = false
	if err := classifyServeExit("/mnt/x", func(string) bool { return present }); err != nil {
		t.Fatalf("disappeared mount row classified %v, want clean", err)
	}
}

// qiffang nineteenth round: root's access mask follows POSIX — pure
// R/W requests pass without an execute bit; X needs any execute bit.
func TestPortRootAccessMaskPOSIXRule(t *testing.T) {
	root := fs.Caller{Uid: 0, Gid: 0}
	if !hasPOSIXAccess(root, 1000, 1000, 0o600, 0b010) {
		t.Fatal("root W_OK on 0600 denied")
	}
	if !hasPOSIXAccess(root, 1000, 1000, 0o600, 0b100) {
		t.Fatal("root R_OK on 0600 denied")
	}
	if !hasPOSIXAccess(root, 1000, 1000, 0o600, 0b110) {
		t.Fatal("root R|W on 0600 denied")
	}
	if hasPOSIXAccess(root, 1000, 1000, 0o644, 0b001) {
		t.Fatal("root X_OK on 0644 granted without any execute bit")
	}
	if !hasPOSIXAccess(root, 1000, 1000, 0o644|0o100, 0b001) {
		t.Fatal("root X_OK on 0744 denied despite owner execute bit")
	}
	// End-to-end: a fresh root O_RDWR open of a 0600 file passes the gate.
	b := testbackend.NewBackend()
	b.Seed("/rw", []byte("data"))
	b.SetNodeMode("/rw", 0o600)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)
	entryOut := gofuse.EntryOut{}
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "rw", &entryOut); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	hdr := gofuse.InHeader{NodeId: entryOut.NodeId}
	hdr.Uid = 0
	hdr.Gid = 0
	out := gofuse.OpenOut{}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: uint32(syscall.O_RDWR)}, &out); st != gofuse.OK {
		t.Fatalf("root O_RDWR of 0600 = %v, want OK", st)
	}
}

// qiffang twentieth round: the facade honors an explicit drop-cache
// request instead of ORing KEEP_CACHE back on.
func TestPortFacadeHonorsDropCacheMarker(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)
	entryOut := gofuse.EntryOut{}
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &entryOut); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	// The core plane returns no flags: KEEP_CACHE applies (default).
	hdr := gofuse.InHeader{NodeId: entryOut.NodeId}
	out := gofuse.OpenOut{}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: 0}, &out); st != gofuse.OK {
		t.Fatalf("open: %v", st)
	}
	if out.OpenFlags&gofuse.FOPEN_KEEP_CACHE == 0 {
		t.Fatalf("plain open flags = %#x, want KEEP_CACHE default", out.OpenFlags)
	}
	if out.OpenFlags&fs.OpenFlagDropKernelCache != 0 {
		t.Fatalf("facade leaked the drop-cache marker to the kernel: %#x", out.OpenFlags)
	}
}
