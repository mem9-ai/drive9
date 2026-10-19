package fuse

import (
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestErrnoAlignmentLocalDirectoryOperations(test *testing.T) {
	if runtime.GOOS != "linux" {
		test.Skip("Linux syscall errno regression")
	}
	var remoteCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		remoteCalls.Add(1)
		http.Error(writer, "local-only operation reached remote", http.StatusInternalServerError)
	}))
	test.Cleanup(server.Close)
	options := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: test.TempDir()}
	options.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), options)
	const parentPath = "/repo/node_modules"
	parentInode := fs.inodes.Lookup(parentPath, true, 0, time.Now())
	for _, name := range []string{"source", "target"} {
		if err := fs.localOverlay.Mkdir(parentPath+"/"+name, 0o755); err != nil {
			test.Fatal(err)
		}
		fs.inodes.Lookup(parentPath+"/"+name, true, 0, time.Now())
	}
	file, err := fs.localOverlay.OpenFile(parentPath+"/target/child", uint32(os.O_CREATE|os.O_RDWR), 0o644)
	if err != nil {
		test.Fatal(err)
	}
	if err := file.Close(); err != nil {
		test.Fatal(err)
	}
	if got := fs.Rmdir(nil, &gofuse.InHeader{NodeId: parentInode}, "target"); got != gofuse.Status(syscall.ENOTEMPTY) {
		test.Errorf("Rmdir nonempty local directory = %v, want ENOTEMPTY", got)
	}
	if got := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: parentInode}, Newdir: parentInode}, "source", "target"); got != gofuse.Status(syscall.ENOTEMPTY) {
		test.Errorf("Rename onto nonempty local directory = %v, want ENOTEMPTY", got)
	}
	for _, path := range []string{parentPath + "/source", parentPath + "/target/child"} {
		if _, err := fs.localOverlay.Lstat(path); err != nil {
			test.Errorf("failed operation changed %s: %v", path, err)
		}
	}
	if got := remoteCalls.Load(); got != 0 {
		test.Fatalf("remote calls = %d, want 0", got)
	}
}

func TestErrnoAlignmentXAttrNamespaces(test *testing.T) {
	for _, path := range []string{"/remote/file", "/repo/node_modules/file"} {
		for _, attr := range []string{"unknown.test", "user", "", "User.test", "prefix.user.test", "com.apple.test", "user.test", "security.test", "trusted.test", "system.test"} {
			test.Run(path+"/"+attr, func(test *testing.T) {
				fs := &Dat9FS{inodes: NewInodeToPath(), xattrs: NewXAttrStore()}
				inode := fs.inodes.Lookup(path, false, 0, time.Now())
				header := &gofuse.InHeader{NodeId: inode}
				known := attr == "user.test" || attr == "security.test" || attr == "trusted.test" || attr == "system.test"
				reject := runtime.GOOS == "linux" && !known
				want := gofuse.OK
				if reject {
					want = gofuse.Status(syscall.EOPNOTSUPP)
					if errno := fs.xattrs.SetWithFlags(path, attr, []byte("existing"), 0); errno != 0 {
						test.Fatal(errno)
					}
				}
				if got := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *header}, attr, []byte("value")); got != want {
					test.Errorf("SetXAttr = %v, want %v", got, want)
				}
				if _, got := fs.GetXAttr(nil, header, attr, nil); got != want {
					test.Errorf("GetXAttr = %v, want %v", got, want)
				}
				if got := fs.RemoveXAttr(nil, header, attr); got != want {
					test.Errorf("RemoveXAttr = %v, want %v", got, want)
				}
				if reject {
					value, found := fs.xattrs.Get(path, attr)
					if !found || string(value) != "existing" {
						test.Fatal("rejected xattr operation mutated the store")
					}
					if got := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *header, Flags: xattrCreateFlag}, attr, nil); got != want {
						test.Errorf("unknown namespace with create flag = %v, want %v", got, want)
					}
					if errno := fs.xattrs.SetWithFlags(path, attr, nil, 0); errno != 0 {
						test.Fatal(errno)
					}
					fs.xattrs.Remove(path, attr)
					if got := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *header, Flags: xattrReplaceFlag}, attr, nil); got != want {
						test.Errorf("unknown namespace with replace flag = %v, want %v", got, want)
					}
					if _, got := fs.GetXAttr(nil, header, attr, nil); got != want {
						test.Errorf("missing unknown xattr = %v, want %v", got, want)
					}
					if got := fs.RemoveXAttr(nil, header, attr); got != want {
						test.Errorf("remove missing unknown xattr = %v, want %v", got, want)
					}
				} else if _, got := fs.GetXAttr(nil, header, attr, nil); got != gofuse.ENOATTR {
					test.Errorf("removed known xattr = %v, want ENOATTR", got)
				}
				missing := &gofuse.InHeader{NodeId: inode + 1000}
				if got := fs.SetXAttr(nil, &gofuse.SetXAttrIn{InHeader: *missing}, attr, nil); got != gofuse.ENOENT {
					test.Errorf("missing inode SetXAttr = %v, want ENOENT", got)
				}
				if _, got := fs.GetXAttr(nil, missing, attr, nil); got != gofuse.ENOENT {
					test.Errorf("missing inode GetXAttr = %v, want ENOENT", got)
				}
				if got := fs.RemoveXAttr(nil, missing, attr); got != gofuse.ENOENT {
					test.Errorf("missing inode RemoveXAttr = %v, want ENOENT", got)
				}
			})
		}
	}
}
