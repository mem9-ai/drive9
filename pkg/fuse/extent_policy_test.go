package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extent"
)

func TestRegularMknodUsesExtentCreationPolicy(t *testing.T) {
	for _, test := range []struct {
		name        string
		file        string
		wantClassic bool
	}{
		{name: "matching extent path", file: "upper.extent"},
		{name: "ordinary path", file: "ordinary.txt", wantClassic: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				http.Error(w, "classic write must fail", http.StatusInternalServerError)
			}))
			defer server.Close()

			fs := NewDat9FS(newTestClient(server.URL), &MountOptions{ExtentPaths: []string{"*.extent"}})
			fs.extentTornDown = true
			var out gofuse.EntryOut
			status := fs.Mknod(nil, &gofuse.MknodIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Mode:     uint32(syscall.S_IFREG) | 0o640,
			}, test.file, &out)
			if status == gofuse.OK {
				t.Fatal("Mknod status=OK, want injected failure")
			}
			gotCalls := calls.Load()
			if test.wantClassic && gotCalls == 0 {
				t.Fatal("ordinary Mknod made no classic HTTP call")
			}
			if !test.wantClassic && gotCalls != 0 {
				t.Fatalf("extent Mknod made %d classic HTTP calls, want 0", gotCalls)
			}
		})
	}
}

func TestFindPathInDirIgnoresExtraSlash(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	dir := fs.inodes.Lookup("/work/n4/", true, 0, time.Now())
	file := fs.inodes.Lookup("/work/n4/src.txt", false, 1, time.Now())
	got, p, ok := fs.inodes.FindPathInDir("/work/n4/", "src.txt")
	if !ok || got != file || p != "/work/n4/src.txt" {
		t.Fatalf("FindPathInDir = %d %q %v, want ino=%d path=/work/n4/src.txt", got, p, ok, file)
	}
	if dir == 0 {
		t.Fatal("missing dir inode")
	}
}

func TestFinishLocalRenameFindsSourceByBasename(t *testing.T) {
	fs := &Dat9FS{
		inodes:      NewInodeToPath(),
		readCache:   NewReadCache(0, 0),
		dirCache:    NewDirCache(time.Second),
		openHandles: NewOpenHandleIndex(),
	}
	srcDir := fs.inodes.Lookup("/n0/", true, 0, time.Now())
	dstDir := fs.inodes.Lookup("/n1/", true, 0, time.Now())
	src := fs.inodes.Lookup("/n0/src.txt", false, 1, time.Now())
	fs.inodes.SetExtentIno(src, 42)
	input := &gofuse.RenameIn{}
	input.NodeId = srcDir
	input.Newdir = dstDir
	fs.finishLocalRename(input, "/n0/src.txt", "/n1/dst.txt")
	if ino, ok := fs.inodes.GetInode("/n1/dst.txt"); !ok || ino != src {
		t.Fatalf("dest inode = %d/%v, want source %d", ino, ok, src)
	}
	if _, ok := fs.inodes.GetInode("/n0/src.txt"); ok {
		t.Fatal("source path should be gone after rename")
	}
}

func TestFinishLocalRenameFindsUniqueBasename(t *testing.T) {
	fs := &Dat9FS{
		inodes:      NewInodeToPath(),
		readCache:   NewReadCache(0, 0),
		dirCache:    NewDirCache(time.Second),
		openHandles: NewOpenHandleIndex(),
	}
	_ = fs.inodes.Lookup("/n0/", true, 0, time.Now())
	dstDir := fs.inodes.Lookup("/n1/", true, 0, time.Now())
	src := fs.inodes.Lookup("/n0/pjdfstest_uniquesrc", false, 1, time.Now())
	fs.inodes.SetExtentIno(src, 7)
	input := &gofuse.RenameIn{}
	input.NodeId = dstDir
	input.Newdir = dstDir
	fs.finishLocalRename(input, "/wrong/pjdfstest_uniquesrc", "/n1/dst.txt")
	if ino, ok := fs.inodes.GetInode("/n1/dst.txt"); !ok || ino != src {
		t.Fatalf("dest inode = %d/%v, want unique-basename source %d", ino, ok, src)
	}
}

func TestFindByExtentInoReusesFuseInode(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/a.txt", false, 1, time.Now())
	fs.inodes.SetExtentIno(ino, 77)
	got, ok := fs.inodes.FindByExtentIno(77)
	if !ok || got != ino {
		t.Fatalf("FindByExtentIno = %d/%v, want %d", got, ok, ino)
	}
	if _, ok := fs.inodes.FindByExtentIno(78); ok {
		t.Fatal("missing juicefs ino must not match")
	}
}

func TestChildPathTrimsDirectorySlash(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	dir := fs.inodes.Lookup("/work/n4/", true, 0, time.Now())
	got, st := fs.childPath(dir, "file")
	if st != gofuse.OK || got != "/work/n4/file" {
		t.Fatalf("childPath = %q/%v, want /work/n4/file", got, st)
	}
}

func TestSetAttrModeAllowsNonOwnerClearSetid(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/suid.bin", false, 0, time.Now())
	fs.inodes.UpdateMode(ino, 04777)
	fs.inodes.UpdateOwner(ino, 0, 0, true, true)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing inode")
	}
	input := &gofuse.SetAttrIn{}
	input.InHeader.Uid = 65534
	input.InHeader.Gid = 65534
	input.Mode = 0777
	got, st := fs.setAttrModeForCaller(input, entry)
	if st != gofuse.OK || got&posixPermissionModeMask != 0777 {
		t.Fatalf("clear setid mode=%o st=%v, want 0777 OK", got, st)
	}
	input.Mode = 0222
	if _, st := fs.setAttrModeForCaller(input, entry); st != gofuse.EPERM {
		t.Fatalf("non-owner chmod 0222 st=%v, want EPERM", st)
	}
}

func TestApplyJuiceFSDirectoryMetadataUsesNativeOwnerModeOnly(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	mtime := time.Unix(123, 456)
	ino := fs.inodes.Lookup("/sticky/", true, 7, mtime)
	fs.inodes.UpdateMode(ino, 01777)
	fs.inodes.UpdateOwner(ino, 0, 0, true, true)
	fs.inodes.SetExtentIno(ino, 99)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing dir inode")
	}
	fs.applyJuiceFSDirectoryMetadata(entry, &jfsmeta.Attr{
		Typ:    jfsmeta.TypeDirectory,
		Mode:   0750,
		Uid:    3456,
		Gid:    4567,
		Length: 4096,
		Nlink:  99,
		Mtime:  999,
	})
	entry, ok = fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing refreshed dir inode")
	}
	if !entry.HasMode || entry.Mode&posixPermissionModeMask != 0750 {
		t.Fatalf("mode = %o, want 0750", entry.Mode)
	}
	if !entry.HasUID || !entry.HasGID || entry.Uid != 3456 || entry.Gid != 4567 {
		t.Fatalf("owner = %d:%d, want 3456:4567", entry.Uid, entry.Gid)
	}
	if entry.Size != 7 || entry.Nlink == 99 || !entry.Mtime.Equal(mtime) {
		t.Fatalf("directory namespace metadata changed: size=%d nlink=%d mtime=%s", entry.Size, entry.Nlink, entry.Mtime)
	}
}

func TestApplyJuiceFSDirectoryMetadataRejectsNonDirectoryAttr(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/dir/", true, 0, time.Now())
	fs.inodes.UpdateMode(ino, 0755)
	fs.inodes.UpdateOwner(ino, 1001, 1002, true, true)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing dir inode")
	}

	fs.applyJuiceFSDirectoryMetadata(entry, &jfsmeta.Attr{
		Typ:  jfsmeta.TypeFile,
		Mode: 0600,
		Uid:  3456,
		Gid:  4567,
	})
	entry, ok = fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing dir inode after rejected attr")
	}
	if entry.Mode&posixPermissionModeMask != 0755 || entry.Uid != 1001 || entry.Gid != 1002 {
		t.Fatalf("non-directory attr changed directory metadata: mode=%o owner=%d:%d", entry.Mode, entry.Uid, entry.Gid)
	}
}

func TestApplyCachedNativeOwnerModeUsesDirectoryMetadataOnly(t *testing.T) {
	mtime := time.Unix(123, 456)
	item := CachedFileInfo{
		Name: "root", Size: 7, IsDir: true, Mtime: mtime,
		Mode: 0755, HasMode: true, Uid: 1001, Gid: 1001, HasUID: true, HasGID: true,
		ExtentIno: 42,
	}
	entry := &InodeEntry{
		IsDir: true, Size: 4096, Mtime: time.Unix(999, 0),
		Mode: 0700, HasMode: true, Uid: 0, Gid: 0, HasUID: true, HasGID: true,
	}
	applyCachedNativeOwnerMode(&item, entry)
	if item.Mode != 0700 || !item.HasMode || item.Uid != 0 || item.Gid != 0 || !item.HasUID || !item.HasGID {
		t.Fatalf("cached native metadata = mode=%o owner=%d:%d flags=%v/%v/%v", item.Mode, item.Uid, item.Gid, item.HasMode, item.HasUID, item.HasGID)
	}
	if item.Size != 7 || !item.Mtime.Equal(mtime) || item.ExtentIno != 42 {
		t.Fatalf("directory namespace metadata changed: size=%d mtime=%s extent_ino=%d", item.Size, item.Mtime, item.ExtentIno)
	}
}

func TestApplyCachedNativeOwnerModeUsesFileMetadataOnly(t *testing.T) {
	mtime := time.Unix(123, 456)
	item := CachedFileInfo{
		Name: "drive9.conf", Size: 7, Mtime: mtime,
		Mode: 0644, HasMode: true, Uid: 1001, Gid: 1001, HasUID: true, HasGID: true,
		ExtentIno: 42,
	}
	entry := &InodeEntry{
		Size: 99, Mtime: time.Unix(999, 0),
		Mode: 0640, HasMode: true, Uid: 1234, Gid: 2345, HasUID: true, HasGID: true,
	}
	applyCachedNativeOwnerMode(&item, entry)
	if item.Mode != 0640 || !item.HasMode || item.Uid != 1234 || item.Gid != 2345 || !item.HasUID || !item.HasGID {
		t.Fatalf("cached native metadata = mode=%o owner=%d:%d flags=%v/%v/%v", item.Mode, item.Uid, item.Gid, item.HasMode, item.HasUID, item.HasGID)
	}
	if item.Size != 7 || !item.Mtime.Equal(mtime) || item.ExtentIno != 42 {
		t.Fatalf("file listing metadata changed: size=%d mtime=%s extent_ino=%d", item.Size, item.Mtime, item.ExtentIno)
	}
}

func TestExtentSetAttrResultUpdatesCachedReadDirPlusMetadata(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)

	parent := "/upper/etc"
	file := parent + "/drive9.conf"
	dirIno := fs.inodes.Lookup(parent, true, 0, time.Now())
	ino := fs.inodes.Lookup(file, false, 7, time.Now())
	fs.inodes.SetExtentIno(ino, 42)
	fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0644)
	fs.inodes.UpdateOwner(ino, 1001, 1001, true, true)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing extent inode")
	}
	fs.dirCache.Put(parent, []CachedFileInfo{cachedInfoFromEntry("drive9.conf", entry)})

	input := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino},
		Valid:    gofuse.FATTR_MODE | gofuse.FATTR_UID | gofuse.FATTR_GID,
		Mode:     0640,
		Owner:    gofuse.Owner{Uid: 1234, Gid: 2345},
	}}
	entry = fs.applyExtentSetAttrResult(input, entry, &jfsmeta.Entry{
		Inode: 42,
		Attr: &jfsmeta.Attr{
			Full:   true,
			Typ:    jfsmeta.TypeFile,
			Mode:   0640,
			Uid:    1234,
			Gid:    2345,
			Length: 7,
			Nlink:  1,
		},
	}, 0640)
	if entry == nil {
		t.Fatal("missing updated extent inode")
	}

	items, ok := fs.dirCache.Get(parent)
	if !ok || len(items) != 1 {
		t.Fatalf("cached listing = %+v ok=%v, want one item", items, ok)
	}
	item := items[0]
	if item.Mode&posixPermissionModeMask != 0640 || item.Uid != 1234 || item.Gid != 2345 {
		t.Fatalf("cached listing metadata = mode=%o owner=%d:%d, want 0640 1234:2345", item.Mode, item.Uid, item.Gid)
	}
	if item.ExtentIno != 42 {
		t.Fatalf("cached listing extent inode = %d, want 42", item.ExtentIno)
	}

	entries := fs.cachedToDirEntries(parent, items)
	if len(entries) != 1 {
		t.Fatalf("directory entries = %+v, want one entry", entries)
	}
	if entries[0].AttrMode&posixPermissionModeMask != 0640 || entries[0].Uid != 1234 || entries[0].Gid != 2345 {
		t.Fatalf("directory entry metadata = mode=%o owner=%d:%d, want 0640 1234:2345", entries[0].AttrMode, entries[0].Uid, entries[0].Gid)
	}

	dh := &DirHandle{
		Ino:               dirIno,
		Path:              parent,
		Entries:           entries,
		entriesGeneration: fs.mountViewGeneration.Load(),
	}
	fh := fs.dirHandles.Allocate(dh)
	out := gofuse.NewDirEntryList(make([]byte, 4096), 0)
	if st := fs.ReadDirPlus(nil, &gofuse.ReadIn{
		InHeader: gofuse.InHeader{NodeId: dirIno},
		Fh:       fh,
		Size:     4096,
	}, out); st != gofuse.OK {
		t.Fatalf("ReadDirPlus status = %v, want OK", st)
	}
	entry, ok = fs.inodes.GetEntry(ino)
	if !ok || entry.Mode&posixPermissionModeMask != 0640 || entry.Uid != 1234 || entry.Gid != 2345 {
		t.Fatalf("ReadDirPlus inode metadata = %+v ok=%v, want 0640 1234:2345", entry, ok)
	}
}

func TestApplyCachedNativeOwnerModeRejectsMismatchedKinds(t *testing.T) {
	for _, test := range []struct {
		name  string
		item  CachedFileInfo
		entry InodeEntry
	}{
		{name: "listing file", item: CachedFileInfo{IsDir: false, Mode: 0644, Uid: 1001, Gid: 1002}, entry: InodeEntry{IsDir: true, Mode: 0700, Uid: 0, Gid: 0}},
		{name: "native file", item: CachedFileInfo{IsDir: true, Mode: 0755, Uid: 1001, Gid: 1002}, entry: InodeEntry{IsDir: false, Mode: 0600, Uid: 0, Gid: 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := test.item
			applyCachedNativeOwnerMode(&test.item, &test.entry)
			if test.item != before {
				t.Fatalf("mismatched kinds changed cached metadata: got %+v want %+v", test.item, before)
			}
		})
	}
}

func TestCachedNativeOwnerModeDiffersOnlyForMatchingKinds(t *testing.T) {
	tests := []struct {
		name  string
		item  CachedFileInfo
		entry *InodeEntry
		want  bool
	}{
		{name: "nil entry", item: CachedFileInfo{Mode: 0644}},
		{name: "kind mismatch", item: CachedFileInfo{IsDir: true, Mode: 0755, HasMode: true}, entry: &InodeEntry{Mode: 0644, HasMode: true}},
		{name: "same metadata", item: CachedFileInfo{Mode: 0640, HasMode: true, Uid: 1, Gid: 2, HasUID: true, HasGID: true}, entry: &InodeEntry{Mode: syscall.S_IFREG | 0640, HasMode: true, Uid: 1, Gid: 2, HasUID: true, HasGID: true}},
		{name: "mode differs", item: CachedFileInfo{Mode: 0644, HasMode: true}, entry: &InodeEntry{Mode: syscall.S_IFREG | 0640, HasMode: true}, want: true},
		{name: "owner differs", item: CachedFileInfo{Uid: 1001, Gid: 1001, HasUID: true, HasGID: true}, entry: &InodeEntry{Uid: 1234, Gid: 2345, HasUID: true, HasGID: true}, want: true},
		{name: "listing omits native mode", item: CachedFileInfo{}, entry: &InodeEntry{Mode: syscall.S_IFREG | 0600, HasMode: true}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cachedNativeOwnerModeDiffers(test.item, test.entry); got != test.want {
				t.Fatalf("cachedNativeOwnerModeDiffers = %v, want %v", got, test.want)
			}
		})
	}
}

func TestShouldRefreshCachedNativeMetadataAcceptsExactExtentIdentity(t *testing.T) {
	item := CachedFileInfo{Mode: 0644, HasMode: true, ExtentIno: 42}
	entry := &InodeEntry{Mode: syscall.S_IFREG | 0640, HasMode: true, ExtentIno: 42}
	if !shouldRefreshCachedNativeMetadata(item, entry, false) {
		t.Fatal("matching extent inode with changed native mode must refresh without a resource id")
	}
	entry.ExtentIno = 43
	if shouldRefreshCachedNativeMetadata(item, entry, false) {
		t.Fatal("different extent identity must not refresh without matching resource evidence")
	}
	if !shouldRefreshCachedNativeMetadata(item, entry, true) {
		t.Fatal("matching resource evidence with changed native mode must refresh")
	}
}

func TestProjectedExtentInoAcceptsMirroredDirectoryOnly(t *testing.T) {
	for _, test := range []struct {
		name string
		stat *client.StatResult
		want uint64
	}{
		{name: "nil stat"},
		{name: "mirrored directory", stat: &client.StatResult{IsDir: true, ExtentIno: 41}, want: 41},
		{name: "extent file", stat: &client.StatResult{ContentLayout: client.ContentLayoutExtent, ExtentIno: 42}, want: 42},
		{name: "classic file with stale inode", stat: &client.StatResult{ExtentIno: 43}},
		{name: "directory without mirror", stat: &client.StatResult{IsDir: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := projectedExtentIno(test.stat)
			if got != test.want || ok != (test.want != 0) {
				t.Fatalf("projectedExtentIno = %d/%v, want %d/%v", got, ok, test.want, test.want != 0)
			}
		})
	}
}

func TestJuiceDirCreateModeUsesStickyOwner(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/work/sticky", true, 0, time.Now())
	fs.inodes.UpdateMode(ino, 01777)
	fs.inodes.UpdateOwner(ino, 65534, 65533, true, true)
	mode, uid, gid := fs.juiceDirCreateMode("/work/sticky/", 0755, 1000, 1000)
	if mode != 01777 || uid != 65534 || gid != 65533 {
		t.Fatalf("juiceDirCreateMode = %o %d/%d, want 01777 65534/65533", mode, uid, gid)
	}
}

func TestExtentEnabledRequiresPatterns(t *testing.T) {
	fs := &Dat9FS{}
	if fs.extentEnabled() {
		t.Fatal("nil opts must not enable juicefs mkdir")
	}
	fs.opts = &MountOptions{}
	if fs.extentEnabled() {
		t.Fatal("empty ExtentPaths must not enable juicefs mkdir")
	}
	fs.opts.ExtentPaths = []string{"*"}
	if !fs.extentEnabled() {
		t.Fatal("ExtentPaths=* must enable juicefs mkdir")
	}
}

func TestLocalOnlySpecialEntrySkipsExtentBackedFifo(t *testing.T) {
	e := &InodeEntry{HasMode: true, Mode: syscall.S_IFIFO | 0644, ExtentIno: 9}
	if localOnlySpecialEntry(e) {
		t.Fatal("extent fifo must use juicefs setattr/getattr")
	}
	e.ExtentIno = 0
	if !localOnlySpecialEntry(e) {
		t.Fatal("in-memory fifo must stay local-only")
	}
}

func TestFinishLocalRenameDecrementsReplacedNlink(t *testing.T) {
	fs := &Dat9FS{
		inodes:      NewInodeToPath(),
		readCache:   NewReadCache(0, 0),
		dirCache:    NewDirCache(time.Second),
		openHandles: NewOpenHandleIndex(),
	}
	srcDir := fs.inodes.Lookup("/d/", true, 0, time.Now())
	src := fs.inodes.Lookup("/d/src", false, 1, time.Now())
	dst := fs.inodes.LookupWithIdentity("/d/dst", "dst-id", 2, false, 1, time.Now())
	if !fs.inodes.AddAlias(dst, "/d/dstlnk", "dst-id", 2, false, 1, time.Now()) {
		t.Fatal("alias")
	}
	fs.inodes.SetExtentIno(src, 10)
	fs.inodes.SetExtentIno(dst, 11)
	input := &gofuse.RenameIn{}
	input.NodeId = srcDir
	input.Newdir = srcDir
	fs.finishLocalRename(input, "/d/src", "/d/dst")
	entry, ok := fs.inodes.GetEntry(dst)
	if !ok {
		t.Fatal("replaced dest inode should remain via remaining hardlink")
	}
	if entry.Nlink != 1 {
		t.Fatalf("remaining dest nlink=%d, want 1 after rename-replace", entry.Nlink)
	}
	if _, ok := fs.inodes.GetInode("/d/dstlnk"); !ok {
		t.Fatal("remaining hardlink path missing")
	}
	var out gofuse.Attr
	fs.fillAttr(entry, &out)
	if out.Nlink != 1 {
		t.Fatalf("fillAttr nlink=%d, want 1 from remaining Paths", out.Nlink)
	}
}

func TestApplyJuiceFSAttrCopiesNlink(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/linked.bin", false, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing inode")
	}
	entry.Nlink = 2
	fs.inodes.UpdateLinkCount(ino, 2)
	fs.applyJuiceFSAttr(entry, &jfsmeta.Attr{Full: true, Typ: jfsmeta.TypeFile, Mode: 0644, Nlink: 1})
	if entry.Nlink != 2 {
		t.Fatalf("nlink=%d, want Dat9FS 2 (juicefs getattr must not clobber hardlink nlink)", entry.Nlink)
	}
	live, ok := fs.inodes.GetEntry(ino)
	if !ok || live.Nlink != 2 {
		t.Fatalf("live nlink=%v/%d, want 2", ok, live.Nlink)
	}
}

func TestJuiceTypeToStatMode(t *testing.T) {
	if got := juiceTypeToStatMode(jfsmeta.TypeFIFO, 0644); got&syscall.S_IFMT != syscall.S_IFIFO {
		t.Fatalf("fifo mode=%o, want S_IFIFO", got)
	}
	if got := juiceTypeToStatMode(jfsmeta.TypeSymlink, 0777); got&syscall.S_IFMT != syscall.S_IFLNK {
		t.Fatalf("symlink mode=%o, want S_IFLNK", got)
	}
	if got := juiceTypeToStatMode(jfsmeta.TypeFile, 0642); got&0777 != 0642 {
		t.Fatalf("file perm=%o, want 0642", got)
	}
}

func TestApplyJuiceFSAttrSetsFifoType(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/pipe", false, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing inode")
	}
	fs.applyJuiceFSAttr(entry, &jfsmeta.Attr{Full: true, Typ: jfsmeta.TypeFIFO, Mode: 0644, Uid: 65534, Gid: 65533, Nlink: 1})
	if !entry.HasMode || entry.Mode&syscall.S_IFMT != syscall.S_IFIFO {
		t.Fatalf("fifo mode=%o, want S_IFIFO", entry.Mode)
	}
}

func TestLookupFromDirCacheDoesNotReuseStaleDestExtentIno(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	src := fs.inodes.Lookup("/src.txt", false, 1, time.Now())
	dst := fs.inodes.Lookup("/dst.txt", false, 1, time.Now())
	fs.inodes.SetExtentIno(src, 10)
	fs.inodes.SetExtentIno(dst, 11)
	fs.inodes.Rename("/src.txt", "/dst.txt")
	fs.dirCache.Upsert("/", CachedFileInfo{
		Name:      "dst.txt",
		Size:      1,
		Mtime:     time.Now(),
		ExtentIno: 11,
	})
	var out gofuse.EntryOut
	handled, st := fs.lookupFromDirCache("/", "/dst.txt", "dst.txt", &out)
	if !handled || st != gofuse.OK {
		t.Fatalf("lookupFromDirCache handled/status = %v/%v, want true/OK", handled, st)
	}
	if out.NodeId != src {
		t.Fatalf("stale dest ExtentIno reused node=%d, want source %d (not dest %d)", out.NodeId, src, dst)
	}
}

func TestApplyJuiceFSAttrCopiesOwnerSizeModeValues(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/owned.bin", false, 0, time.Now())
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing inode")
	}
	fs.applyJuiceFSAttr(entry, &jfsmeta.Attr{Full: true, Typ: jfsmeta.TypeFile, Mode: 0642, Uid: 65534, Gid: 65533, Length: 123, Nlink: 1})
	if !entry.HasUID || entry.Uid != 65534 || !entry.HasGID || entry.Gid != 65533 {
		t.Fatalf("owner = %d/%d has=%v/%v, want 65534/65533", entry.Uid, entry.Gid, entry.HasUID, entry.HasGID)
	}
	if !entry.HasMode || entry.Mode&0777 != 0642 {
		t.Fatalf("mode = %o, want 0642", entry.Mode)
	}
	if entry.Size != 123 {
		t.Fatalf("size = %d, want 123", entry.Size)
	}
}

func TestRenamePreflightSymlinkParentIsELOOP(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	dir := fs.inodes.Lookup("/", true, 0, time.Now())
	link := fs.inodes.Lookup("/loop", false, 4, time.Now())
	fs.inodes.UpdateMode(link, uint32(syscall.S_IFLNK)|0777)
	file := fs.inodes.Lookup("/file", false, 1, time.Now())
	if dir == 0 || file == 0 {
		t.Fatal("missing inodes")
	}
	input := &gofuse.RenameIn{}
	input.NodeId = dir
	input.Newdir = link
	_, _, st := fs.renamePreflight(context.TODO(), input, "/file", "/loop/test")
	if st != gofuse.Status(syscall.ELOOP) {
		t.Fatalf("rename through symlink parent st=%v, want ELOOP", st)
	}
}

func TestExtentAttrTimeoutZeroWhenHardlinked(t *testing.T) {
	e := &InodeEntry{Nlink: 2, ExtentIno: 9}
	if got := extentAttrTimeout(e); got != 0 {
		t.Fatalf("hardlink AttrTimeout=%s, want 0 so lstat after rename-replace revalidates nlink", got)
	}
	e.Nlink = 1
	if got := extentAttrTimeout(e); got != juiceFSAttrTimeout {
		t.Fatalf("nlink=1 AttrTimeout=%s, want %s", got, juiceFSAttrTimeout)
	}
}

func TestExtentFillEntryOutUsesJuiceFSAttrCache(t *testing.T) {
	fs := &Dat9FS{
		opts:   &MountOptions{AttrTTL: 60 * time.Second, EntryTTL: 60 * time.Second, ExtentPaths: []string{"*"}},
		inodes: NewInodeToPath(),
	}
	ino := fs.inodes.Lookup("/sqlite/mptest.db", false, 0, time.Now())
	fs.inodes.SetExtentIno(ino, 99)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing inode entry")
	}
	var out gofuse.EntryOut
	fs.fillEntryOut(entry, &out)
	if got := out.AttrTimeout(); got != juiceFSAttrTimeout {
		t.Fatalf("extent AttrTimeout = %s, want JuiceFS --attr-cache %s", got, juiceFSAttrTimeout)
	}
	if got := out.EntryTimeout(); got != juiceFSAttrTimeout {
		t.Fatalf("extent EntryTimeout = %s, want JuiceFS --entry-cache %s", got, juiceFSAttrTimeout)
	}
}

func TestJuiceParentForOpUsesFuseNodeExtentIno(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	dir := fs.inodes.Lookup("/sqlite/", true, 0, time.Now())
	fs.inodes.SetExtentIno(dir, 55)
	// The path is deliberately not the registered one, so only the by-nodeid
	// resolution can answer: otherwise the test passes through the path
	// fallback and stops proving the kernel-parent fast path works.
	got, st := fs.juiceParentForOp(nil, dir, "/not-registered/")
	if st != 0 || got != 55 {
		t.Fatalf("juiceParentForOp = %d/%v, want 55/0 (JuiceFS fuse uses kernel parent ino)", got, st)
	}
}

func TestMatchExtentPattern(t *testing.T) {
	cases := []struct {
		pat, path string
		want      bool
	}{
		{"*", "/foo.db", true},
		{"*.db", "/a/foo.db", true},
		{"*.db", "/a/foo.txt", false},
		{"*-wal", "/x.db-wal", true},
		{"foo.db", "/foo.db", true},
	}
	for _, tc := range cases {
		if got := matchExtentPattern(tc.pat, tc.path); got != tc.want {
			t.Fatalf("matchExtentPattern(%q,%q)=%v want %v", tc.pat, tc.path, got, tc.want)
		}
	}
}

func TestRemotePathPrefixesExtentProjection(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{RemoteRoot: "/drive9-fuse-supervise-1"}}
	got := fs.remotePath("/cache-invalidation-x/rename-src.txt")
	want := "/drive9-fuse-supervise-1/cache-invalidation-x/rename-src.txt"
	if got != want {
		t.Fatalf("remotePath = %q, want %q", got, want)
	}
	fs.opts.RemoteRoot = "/"
	if got := fs.remotePath("/cache-invalidation-x/rename-src.txt"); got != "/cache-invalidation-x/rename-src.txt" {
		t.Fatalf("root mount remotePath = %q", got)
	}
}

func TestExtentOpenFlags(t *testing.T) {
	if got := extentOpenFlags(false); got != 0 {
		t.Fatalf("without KeepCache OpenFlags=%d, want 0 (JuiceFS notify)", got)
	}
	if got := extentOpenFlags(true); got != gofuse.FOPEN_KEEP_CACHE {
		t.Fatalf("KeepCache OpenFlags=%d, want FOPEN_KEEP_CACHE", got)
	}
}

func TestExtentKeepOpenWALIndexSize(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath(), openHandles: NewOpenHandleIndex()}
	p := "/sqlite/mptest-wal-multiwrite01.db-shm"
	e := &InodeEntry{Path: p, Size: 0}
	fs.extentKeepOpenWALIndexSize(e)
	if e.Size != 0 {
		t.Fatalf("closed shm size=%d, want 0", e.Size)
	}
	fs.openHandles.Add(&FileHandle{Path: p})
	fs.extentKeepOpenWALIndexSize(e)
	if e.Size != sqliteWALIndexMinSize {
		t.Fatalf("open empty shm size=%d, want %d", e.Size, sqliteWALIndexMinSize)
	}
}

func TestWriteMissingHandleReturnsENOENT(t *testing.T) {
	// Without the kernel writeback cache there is no post-RELEASE writeback:
	// the kernel never addresses an inode whose handle is gone, so a
	// missing-handle write is an error for every inode, extent or classic.
	// (Silently acknowledging it is how the old cap-on path lost bytes.)
	fs := &Dat9FS{
		opts:        &MountOptions{ExtentPaths: []string{"*"}},
		inodes:      NewInodeToPath(),
		fileHandles: NewHandleTable[*FileHandle](),
	}
	fs.inodes.SetExtentIno(99, 42)
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: 99}, Fh: 7}, []byte("x")); st != gofuse.ENOENT || n != 0 {
		t.Fatalf("Write missing handle: n=%d st=%v, want ENOENT", n, st)
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: 99}, Fh: 7}, []byte("x")); st != gofuse.ENOENT || n != 0 {
		t.Fatalf("Write missing handle on extent node: n=%d st=%v, want ENOENT", n, st)
	}
}

func TestExtentWriteIsJuiceFSVFSOnly(t *testing.T) {
	fs := &Dat9FS{
		opts:        &MountOptions{},
		fileHandles: NewHandleTable[*FileHandle](),
	}
	fh := &FileHandle{Path: "/sqlite/mptest-wal-crash01.db-wal", extentIno: 9, extentFh: 1}
	id := fs.fileHandles.Allocate(fh)
	_, st := fs.Write(nil, &gofuse.WriteIn{Fh: id}, []byte("wal"))
	if st != gofuse.EBADF {
		t.Fatalf("Write status=%v, want EBADF (no VFS); JuiceFS fuse Write is VFS.Write without Dat9FS fuseCtx", st)
	}
}

func TestShouldUseExtentPath(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*.db", "*-wal"}}}
	if !fs.shouldUseExtentPath("/tmp/x.db") {
		t.Fatal("expected *.db match")
	}
	if fs.shouldUseExtentPath("/tmp/x.txt") {
		t.Fatal("did not expect txt match")
	}
	fs.opts.ExtentPaths = []string{"*"}
	if !fs.shouldUseExtentPath("/anything") {
		t.Fatal("expected * match")
	}
}

func TestShouldUseExtentPathSkipsDirectories(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*"}}}
	if fs.shouldUseExtentPath("/dir/") {
		t.Fatal("create glob must not treat directories as extent files")
	}
}

func TestResolveExtentInoUsesCachedInode(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath(), openHandles: NewOpenHandleIndex()}
	ino := fs.inodes.Lookup("/alpha/text.txt", false, 27, time.Now())
	if fs.shouldExtentTruncate(ino, "/alpha/text.txt") {
		t.Fatal("empty cache must not claim an extent inode")
	}
	fs.inodes.SetExtentIno(ino, 42)
	got, ok := fs.resolveExtentIno(ino, "/alpha/text.txt")
	if !ok || got != 42 {
		t.Fatalf("resolveExtentIno = %d/%v, want 42/true", got, ok)
	}
	if !fs.shouldExtentTruncate(ino, "/alpha/text-hardlink.txt") {
		t.Fatal("cached extent ino must route SetAttr truncate without a HEAD")
	}
}

func TestResolveExtentInoUsesOpenHandle(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath(), openHandles: NewOpenHandleIndex()}
	ino := fs.inodes.Lookup("/alpha/text.txt", false, 27, time.Now())
	fh := &FileHandle{Ino: ino, Path: "/alpha/text.txt", extentIno: 9, extentFh: 3}
	fs.openHandles.Add(fh)
	got, ok := fs.resolveExtentIno(ino, "/alpha/text.txt")
	if !ok || got != 9 {
		t.Fatalf("resolveExtentIno from handle = %d/%v, want 9/true", got, ok)
	}
	if !fh.isExtent() {
		t.Fatal("handle with extentIno/extentFh must be isExtent")
	}
	cached, ok := fs.cachedExtentIno(ino)
	if !ok || cached != 9 {
		t.Fatalf("cachedExtentIno = %d/%v, want 9/true", cached, ok)
	}
}

func TestCachedExtentInoSkipsUnmarkedFiles(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath(), openHandles: NewOpenHandleIndex()}
	ino := fs.inodes.Lookup("/plain.txt", false, 1, time.Now())
	if _, ok := fs.cachedExtentIno(ino); ok {
		t.Fatal("plain file must not take the VFS getattr path")
	}
}

func TestSetExtentInoSharedAcrossHardlinkAliases(t *testing.T) {
	m := NewInodeToPath()
	now := time.Now()
	ino := m.LookupWithIdentity("/a.txt", "file-1", 2, false, 10, now)
	_ = m.LookupWithIdentity("/b.txt", "file-1", 2, false, 10, now)
	m.SetExtentIno(ino, 77)
	entry, ok := m.GetEntry(ino)
	if !ok || entry.ExtentIno != 77 {
		t.Fatalf("ExtentIno = %+v/%v, want 77", entry, ok)
	}
}

func TestJuiceParentInoUsesBoundDir(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	dir := fs.inodes.Lookup("/final/w0", true, 0, time.Now())
	fs.bindJuiceDirIno("/final/w0", 55)
	got, ok := fs.juiceParentIno("/final/w0/")
	if !ok || got != 55 {
		t.Fatalf("juiceParentIno = %d/%v, want 55/true (dir ino %d)", got, ok, dir)
	}
	root, ok := fs.juiceParentIno("/")
	if !ok || root != 1 {
		t.Fatalf("root juice parent = %d/%v, want 1/true", root, ok)
	}
}

func TestExtentApplyWriterLengthNoopWithoutVFS(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/a.txt", false, 0, time.Now())
	fs.inodes.SetExtentIno(ino, 7)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing entry")
	}
	fs.extentApplyWriterLength(entry)
	if entry.Size != 0 {
		t.Fatalf("size changed without VFS: %d", entry.Size)
	}
}

func TestJuiceWriteOffset(t *testing.T) {
	if got := juiceWriteOffset(0, 216, true); got != 216 {
		t.Fatalf("O_APPEND syscall write off=%d, want EOF 216", got)
	}
	if got := juiceWriteOffset(100, 216, true); got != 100 {
		t.Fatalf("O_APPEND with a kernel offset=%d, want 100", got)
	}
	if got := juiceWriteOffset(0, 216, false); got != 0 {
		t.Fatalf("non-append write off=%d, want 0", got)
	}
}

func TestJuiceAppendOffUsesWriterLength(t *testing.T) {
	if got := juiceAppendOff(0, 20); got != 20 {
		t.Fatalf("O_APPEND off=0 with length 20: got %d", got)
	}
	if got := juiceAppendOff(20, 20); got != 20 {
		t.Fatalf("O_APPEND at EOF: got %d", got)
	}
	if got := juiceAppendOff(5, 4); got != 5 {
		t.Fatalf("kernel off past attr: got %d", got)
	}
}

func TestJfsCtxCancelHonorsFuseCancel(t *testing.T) {
	fs := &Dat9FS{}
	cancel := make(chan struct{})
	ctx, stop := fs.jfsCtxCancel(cancel, 1, 0, 0)
	defer stop()
	if ctx.Canceled() {
		t.Fatal("context canceled before FUSE cancel")
	}
	close(cancel)
	// JuiceFS fuseContext ignores interrupt for the first second.
	if ctx.Canceled() {
		t.Fatal("JuiceFS Canceled() must ignore FUSE cancel for the first second")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !ctx.Canceled() {
		if time.Now().After(deadline) {
			t.Fatal("JuiceFS Canceled() never saw FUSE cancel after 1s")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestJfsCtxCancelLockBlockValue(t *testing.T) {
	fs := &Dat9FS{}
	ctx, stop := fs.jfsCtxCancelLock(nil, 1, 0, 0, true)
	defer stop()
	if v, ok := ctx.Value(extent.LockBlockCtxKey).(bool); !ok || !v {
		t.Fatalf("blocking lock context missing LockBlockCtxKey: %v %v", v, ok)
	}
	ctx2, stop2 := fs.jfsCtxCancel(nil, 1, 0, 0)
	defer stop2()
	if _, ok := ctx2.Value(extent.LockBlockCtxKey).(bool); ok {
		t.Fatal("non-blocking lock context must not set LockBlockCtxKey")
	}
}

func TestJfsCtxCancelDoesNotCancelHTTPContext(t *testing.T) {
	fs := &Dat9FS{}
	cancel := make(chan struct{})
	ctx, stop := fs.jfsCtxCancel(cancel, 1, 0, 0)
	defer stop()
	close(cancel)
	select {
	case <-ctx.Done():
		t.Fatal("FUSE cancel must not close context.Done (JuiceFS polls Canceled only)")
	default:
	}
}

func TestExtentApplyWriterLengthSkipsSyntheticZero(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/large.bin", false, 0, time.Now())
	fs.inodes.SetExtentIno(ino, 7)
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing entry")
	}
	// Size 0 must not call VFS.UpdateLength (would reader.Truncate(0)).
	fs.extentApplyWriterLength(entry)
	if entry.Size != 0 {
		t.Fatalf("synthetic zero changed size: %d", entry.Size)
	}
}

func TestCachedInfoFromEntryCopiesExtentIno(t *testing.T) {
	entry := &InodeEntry{
		Ino:       3,
		Path:      "/final/w0/file-000.txt",
		Size:      32768,
		ExtentIno: 42,
	}
	item := cachedInfoFromEntry("file-000.txt", entry)
	if item.ExtentIno != 42 || item.Size != 32768 {
		t.Fatalf("cached info = %+v, want ExtentIno=42 Size=32768", item)
	}
	dirent := dirEntryFromCachedInfo(item, 3)
	if dirent.ExtentIno != 42 || dirent.Size != 32768 {
		t.Fatalf("dirent = %+v, want ExtentIno=42 Size=32768", dirent)
	}
}

func TestRecreateDirEntryInodeBindsExtentIno(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.recreateDirEntryInode("/final/w0", DirEntry{
		Name:        "file-000.txt",
		Size:        32768,
		ExtentIno:   42,
		HasMetadata: true,
	})
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing recreated inode")
	}
	if entry.ExtentIno != 42 {
		t.Fatalf("ExtentIno=%d, want 42", entry.ExtentIno)
	}
	if entry.Size != 32768 {
		t.Fatalf("size=%d, want 32768", entry.Size)
	}
}

func TestCachedExtentInoSkipsDirectories(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	dir := fs.inodes.Lookup("/final/w0", true, 0, time.Now())
	fs.inodes.SetExtentIno(dir, 55)
	if _, ok := fs.cachedExtentIno(dir); ok {
		t.Fatal("directory juicefs ino must not take the file GetAttr/Open path")
	}
}

func TestCachedToDirEntriesRestoresLiveSize(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/final/w0/file-000.txt", false, 32820, time.Now())
	fs.inodes.SetExtentIno(ino, 9)
	entries := fs.cachedToDirEntries("/final/w0", []CachedFileInfo{{Name: "file-000.txt", Size: 0}})
	if len(entries) != 1 {
		t.Fatalf("entries=%d", len(entries))
	}
	if entries[0].Size != 32820 {
		t.Fatalf("dirent size=%d, want 32820 (listing was 0)", entries[0].Size)
	}
	// The routing, unlike the size, is not carried over: the cached entry has
	// no resource id, so nothing proves it still describes this path, and the
	// authoritative listing says it is not an extent object. The next open
	// re-probes and re-stamps (a recoverable cost), whereas inheriting a stale
	// JuiceFS inode would route every later open at a drained inode and fail
	// with no classic fallback (not recoverable).
	if entries[0].ExtentIno != 0 {
		t.Fatalf("dirent ExtentIno=%d, want 0: an id-less cached entry cannot prove identity", entries[0].ExtentIno)
	}
	if got, ok := fs.cachedExtentIno(ino); ok {
		t.Fatalf("cachedExtentIno = %d, want cleared", got)
	}
}

// A listing row is the only layout signal readdir gets. When it carries the
// JuiceFS inode, the FUSE inode must be stamped from it, and when it does not
// (single-layout child, or an old server) nothing may be invented: the former
// name-based jfs overlay is gone precisely because a bare name is not evidence.
func TestCachedToDirEntriesTakesExtentInoFromListing(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	entries := fs.cachedToDirEntries("/final/w0", []CachedFileInfo{
		{Name: "extent.db", Size: 4096, ExtentIno: 41},
		{Name: "single.txt", Size: 12},
	})
	if len(entries) != 2 {
		t.Fatalf("entries=%d", len(entries))
	}
	if entries[0].ExtentIno != 41 {
		t.Fatalf("extent dirent ExtentIno=%d, want 41 from listing", entries[0].ExtentIno)
	}
	if entries[1].ExtentIno != 0 {
		t.Fatalf("single-layout dirent ExtentIno=%d, want 0", entries[1].ExtentIno)
	}
	ino, ok := fs.inodes.GetInode("/final/w0/extent.db")
	if !ok {
		t.Fatal("extent child has no FUSE inode")
	}
	if got, ok := fs.cachedExtentIno(ino); !ok || got != 41 {
		t.Fatalf("cachedExtentIno=%d ok=%v, want 41 true", got, ok)
	}
	plain, ok := fs.inodes.GetInode("/final/w0/single.txt")
	if !ok {
		t.Fatal("single child has no FUSE inode")
	}
	if got, ok := fs.cachedExtentIno(plain); ok {
		t.Fatalf("single-layout child must not resolve an extent inode, got %d", got)
	}
}

func TestNotifyRenameTargetSkipsExtentInode(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	ino := fs.inodes.Lookup("/renamed.txt", false, 27, time.Now())
	fs.inodes.SetExtentIno(ino, 11)
	fs.notifyRenameTarget(1, "renamed.txt", "/renamed.txt")
}

func TestIsExtentFileUsesContentLayoutNotGlob(t *testing.T) {
	fs := &Dat9FS{opts: &MountOptions{ExtentPaths: []string{"*"}}}
	if fs.isExtentFile(context.TODO(), "/x.db") {
		t.Fatal("existing files dispatch on ContentLayout, not the create glob")
	}
	if fs.isExtentFile(context.TODO(), "/dir/") {
		t.Fatal("directories are not extent files")
	}
	fs.inodes = NewInodeToPath()
	ino := fs.inodes.Lookup("/moved", true, 0, time.Time{})
	if fs.pathIsDir("/moved") != true {
		t.Fatalf("expected dir inode %d", ino)
	}
	if _, ok := fs.existingExtentIno(context.TODO(), "/moved"); ok {
		t.Fatal("directory inode must not be treated as an extent file")
	}
}

// A path whose drive9 file identity changed must not keep the previous file's
// JuiceFS inode. The listing is authoritative: when it reports a different
// resource for the same path (another client deleted the extent file and
// created a classic one), inheriting the cached ExtentIno would route every
// later Open to a drained extent inode, and extentOpen has no classic fallback,
// so the open would fail ENOENT for the life of the mount.
func TestCachedToDirEntriesDropsStaleExtentInoOnIdentityChange(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	// First listing: an extent file at this path.
	first := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "a.db", Size: 4096, ExtentIno: 41, ResourceID: "inode-extent"},
	})
	if len(first) != 1 || first[0].ExtentIno != 41 {
		t.Fatalf("first listing = %+v, want ExtentIno 41", first)
	}
	ino, ok := fs.inodes.GetInode("/final/a.db")
	if !ok {
		t.Fatal("no FUSE inode after the first listing")
	}

	// Second listing: the same path is now a different (classic) file.
	second := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "a.db", Size: 12, ResourceID: "inode-classic"},
	})
	if len(second) != 1 {
		t.Fatalf("second listing = %+v", second)
	}
	if second[0].ExtentIno != 0 {
		t.Fatalf("second listing ExtentIno = %d, want 0 (stale extent inode)", second[0].ExtentIno)
	}
	if got, ok := fs.cachedExtentIno(ino); ok {
		t.Fatalf("cachedExtentIno = %d, want cleared", got)
	}

	// Same identity keeps the inode: a listing that still describes the extent
	// file must not lose its routing.
	third := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "a.db", Size: 4096, ExtentIno: 41, ResourceID: "inode-extent"},
	})
	if len(third) != 1 || third[0].ExtentIno != 41 {
		t.Fatalf("third listing = %+v, want ExtentIno 41", third)
	}
	fourth := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "a.db", Size: 4096, ResourceID: "inode-extent"},
	})
	if len(fourth) != 1 || fourth[0].ExtentIno != 41 {
		t.Fatalf("same-identity listing lost the extent inode: %+v", fourth)
	}
}

// A cached entry that carries no resource id — a directory, or one created from
// a layer/pending merge — proves nothing about the object now at the path, so it
// must not lend its JuiceFS inode to the listing. A directory replaced by a
// classic file is the routine shape of this: directory entries never store a
// resource id, so an "both ids known" guard would inherit the drained extent
// inode and every later open would fail ENOENT with no classic fallback.
func TestCachedToDirEntriesClearsRoutingFromIdLessPredecessor(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	// A directory entry that somehow carries an extent inode (mkdir on an
	// extent mount stamps one) and no resource id.
	dir := fs.inodes.Lookup("/final/p", true, 0, time.Now())
	fs.inodes.SetExtentIno(dir, 5)

	got := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "p", Size: 12, ResourceID: "file-2"},
	})
	if len(got) != 1 {
		t.Fatalf("listing = %+v", got)
	}
	if got[0].ExtentIno != 0 {
		t.Fatalf("ExtentIno = %d, want 0: an id-less predecessor must not lend its routing", got[0].ExtentIno)
	}
	if got, ok := fs.cachedExtentIno(dir); ok {
		t.Fatalf("cachedExtentIno = %d, want cleared", got)
	}
}

// A replaced path must not inherit the previous file's size. That carry-over
// exists so a stale listing cannot shrink an extent inode, but applying it
// across an identity change inflates a different file instead: the classic file
// that replaced an extent file would report the old extent size.
func TestCachedToDirEntriesDoesNotInflateSizeAcrossIdentityChange(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	first := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "a.db", Size: 1 << 20, ExtentIno: 41, ResourceID: "inode-extent"},
	})
	if len(first) != 1 || first[0].Size != 1<<20 {
		t.Fatalf("first listing = %+v", first)
	}
	// The path is now a small classic file.
	second := fs.cachedToDirEntries("/final", []CachedFileInfo{
		{Name: "a.db", Size: 12, ResourceID: "inode-classic"},
	})
	if len(second) != 1 {
		t.Fatalf("second listing = %+v", second)
	}
	if second[0].Size != 12 {
		t.Fatalf("size = %d, want 12 (the replaced file must not inherit the old extent size)", second[0].Size)
	}
}

// A path that changes kind (extent file replaced by a directory) must lose the
// old file's JuiceFS routing, and no resolver may hand a file inode to the meta
// engine as a parent. updateEntryLocked cannot rely on setIdentityLocked for
// this: a directory's identity key is empty, so that function returns before it
// can clear anything.
func TestKindChangeClearsExtentRouting(t *testing.T) {
	fs := &Dat9FS{inodes: NewInodeToPath()}
	// A directory that carries a routing answers the parent resolver.
	dirIno := fs.inodes.Lookup("/p", true, 4096, time.Now())
	fs.inodes.SetExtentIno(dirIno, 41)
	if got, ok := fs.juiceDirInoFromFuseNode(dirIno); !ok || got != 41 {
		t.Fatalf("juiceDirInoFromFuseNode = %d,%v, want 41,true", got, ok)
	}

	// The same inode is now a file: a file inode must never answer a parent
	// lookup, because the meta engine only re-resolves a parent that does not
	// exist and would put children under it.
	fs.inodes.EnsureInodeWithIdentity("/p", "file-node", 1, false, 4096, time.Now())
	if got, ok := fs.inodes.GetEntry(dirIno); ok && got.ExtentIno != 0 {
		t.Fatalf("ExtentIno = %d after the kind change, want 0", got.ExtentIno)
	}
	if got, ok := fs.juiceDirInoFromFuseNode(dirIno); ok {
		t.Fatalf("juiceDirInoFromFuseNode = %d, want no routing for a file", got)
	}
	if got, ok := fs.juiceParentIno("/p"); ok {
		t.Fatalf("juiceParentIno = %d, want false once the routing is gone", got)
	}

	// And the reverse: an entry that is not a directory is rejected even while
	// it still carries a routing.
	fileIno := fs.inodes.Lookup("/q", false, 4096, time.Now())
	fs.inodes.SetExtentIno(fileIno, 77)
	if got, ok := fs.juiceDirInoFromFuseNode(fileIno); ok {
		t.Fatalf("juiceDirInoFromFuseNode = %d for a file entry, want false", got)
	}
}
