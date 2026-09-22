//go:build !windows

package fuse

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/extent"
)

func TestPromotionOutcomeUnknownReleaseDropsExtentHandleWithoutCommit(t *testing.T) {
	jfs, ctx := newPromotionExtentTestVFS(t)
	entry, extentFH, errn := jfs.Create(ctx, 1, "blocked-release.db", 0o644, 0, syscall.O_RDWR)
	if errn != 0 {
		t.Fatalf("create extent file: %s", errn)
	}
	payload := bytes.Repeat([]byte{0xA5}, 4096)
	if errn = jfs.Write(ctx, entry.Inode, payload, 0, extentFH); errn != 0 {
		t.Fatalf("buffer extent write: %s", errn)
	}

	fs := newPromotionTestFS(t, "http://127.0.0.1")
	fs.extentRT = &extentRuntime{rt: &extent.Runtime{VFS: jfs}}
	fuseIno := fs.inodes.Lookup("/blocked-release.db", false, 0, time.Now())
	fs.inodes.SetExtentIno(fuseIno, uint64(entry.Inode))
	fh := &FileHandle{
		Ino:       fuseIno,
		Path:      "/blocked-release.db",
		extentIno: entry.Inode,
		extentFh:  extentFH,
	}
	fhID := fs.allocateFileHandle(fh)
	fs.promotionBlocked.Store(true)

	if status := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: fuseIno}, Fh: fhID}); status != gofuse.EIO {
		t.Fatalf("blocked extent Flush status = %v, want EIO", status)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fuseIno}, Fh: fhID})

	if _, ok := fs.fileHandles.Get(fhID); ok {
		t.Fatal("blocked Release left the extent handle registered")
	}
	if fs.openHandles.Has(fuseIno, fh.Path) {
		t.Fatal("blocked Release left the extent handle in the open-handle index")
	}
	got, errn := jfs.GetAttr(ctx, entry.Inode, 0)
	if errn != 0 {
		t.Fatalf("get extent attr after blocked Release: %s", errn)
	}
	if got.Attr.Length != 0 {
		t.Fatalf("extent length after blocked Release = %d, want 0", got.Attr.Length)
	}
}

func newPromotionExtentTestVFS(t *testing.T) (*vfs.VFS, vfs.LogContext) {
	t.Helper()
	metaConf := jfsmeta.DefaultConf()
	metaConf.MountPoint = "/promotion-extent-test"
	m := jfsmeta.NewClient("memkv://", metaConf)
	format := &jfsmeta.Format{
		Name:        "promotion-extent-test",
		UUID:        uuid.NewString(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "none",
		DirStats:    true,
	}
	if err := m.Init(format, true); err != nil {
		t.Fatalf("init extent meta: %v", err)
	}
	conf := &vfs.Config{
		Meta:    metaConf,
		Format:  *format,
		Version: "Drive9 promotion test",
		Chunk: &chunk.Config{
			BlockSize:   format.BlockSize * 1024,
			Compress:    format.Compression,
			MaxUpload:   2,
			MaxDownload: 2,
			BufferSize:  30 << 20,
			CacheSize:   10 << 20,
			CacheDir:    "memory",
		},
		FuseOpts: &vfs.FuseOptions{},
	}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("create extent storage: %v", err)
	}
	store := chunk.NewCachedStore(blob, *conf.Chunk, nil)
	return vfs.NewVFS(conf, m, store, nil, nil), vfs.NewLogContext(jfsmeta.Background())
}
