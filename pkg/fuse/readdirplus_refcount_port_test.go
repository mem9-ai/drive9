package fuse

// qiffang C4/C5 facade probes: READDIRPLUS grants one Nlookup reference
// per successfully emitted entry (a re-read of the same handle must
// account again — identity pinning is separate from reference counting),
// and the userspace open gate derives its access mask from O_ACCMODE (a
// read open of a chmod 000 file must EACCES, not only write opens).

import (
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Two full READDIRPLUS enumerations of one handle grant two references;
// after the kernel FORGETs both, a third enumeration re-resolves the
// pinned identity instead of emitting a dead nodeid.
func TestPortReadDirPlusAccountsLookupPerEmit(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)

	dh := &dirHandle{path: "/", ino: 1, entries: []fs.Entry{{
		Name: "f",
		Attr: fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: 4},
	}}}
	fh := f.nextFh.Add(1)
	f.dirs.put(fh, dh)

	emit := func() uint64 {
		t.Helper()
		out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
		if st := f.ReadDirPlus(nil, &gofuse.ReadIn{Fh: fh, InHeader: gofuse.InHeader{NodeId: 1}}, out); st != gofuse.OK {
			t.Fatalf("ReadDirPlus: %v", st)
		}
		ino, ok := f.inodes.GetInode("/f")
		if !ok {
			t.Fatal("entry not mapped after enumeration")
		}
		return ino
	}

	ino := emit()
	if entry, ok := f.inodes.GetEntry(ino); !ok || entry.Nlookup != 1 {
		t.Fatalf("after first enumeration Nlookup = %+v ok=%v, want 1", entry, ok)
	}
	emit()
	if entry, ok := f.inodes.GetEntry(ino); !ok || entry.Nlookup != 2 {
		t.Fatalf("after re-read Nlookup = %+v ok=%v, want 2 (one reference per emitted reply)", entry, ok)
	}

	// The kernel drops both references: the mapping may retire, and the
	// next enumeration must not emit the dead nodeid.
	f.inodes.Forget(ino, 2)
	emit()
	reIno, ok := f.inodes.GetInode("/f")
	if !ok || reIno == 0 {
		t.Fatal("re-enumeration did not re-resolve a live mapping")
	}
}

// qiffang follow-up: the open gate checks the full O_ACCMODE mask — a
// read open of a chmod 000 file and a read-write open of a write-only
// mode both EACCES; the write-only open of the write-only mode passes.
func TestPortOpenAccessMaskCoversReadClass(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	f, _ := facadeOverCore(t, ts)

	entryOut := gofuse.EntryOut{}
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &entryOut); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	ino := entryOut.NodeId

	cases := []struct {
		name  string
		mode  uint32
		flags uint32
		want  gofuse.Status
	}{
		{"chmod000-read", 0o000, uint32(syscall.O_RDONLY), gofuse.EACCES},
		{"writeonly-rw", 0o200, uint32(syscall.O_RDWR), gofuse.EACCES},
		{"writeonly-write", 0o200, uint32(syscall.O_WRONLY), gofuse.OK},
		{"readonly-write", 0o400, uint32(syscall.O_WRONLY), gofuse.EACCES},
		{"readonly-read", 0o400, uint32(syscall.O_RDONLY), gofuse.OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.inodes.UpdateMode(ino, tc.mode)
			hdr := gofuse.InHeader{NodeId: ino}
			hdr.Uid = 1000
			hdr.Gid = 1000
			out := gofuse.OpenOut{}
			st := f.Open(nil, &gofuse.OpenIn{InHeader: hdr, Flags: tc.flags}, &out)
			if st != tc.want {
				t.Fatalf("open(mode=%04o, flags=%#x) = %v, want %v", tc.mode, tc.flags, st, tc.want)
			}
		})
	}
}
