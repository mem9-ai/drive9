package extent

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/client"
)

// TestBridgeEngineRoundTripLive drives the COMMUNITY vfs + chunk store
// through the bridge against a live drive9-server built from the v2
// redesign. Skipped unless DRIVE9_EXTENT_JOINT_BASE is set.
//
//	DRIVE9_EXTENT_JOINT_BASE=http://127.0.0.1:9410 \
//	DRIVE9_EXTENT_JOINT_KEY=... \
//	  go test ./pkg/jfsbridge/ -run TestBridgeEngineRoundTripLive -v -count=1
func TestBridgeEngineRoundTripLive(t *testing.T) {
	base := os.Getenv("DRIVE9_EXTENT_JOINT_BASE")
	if base == "" {
		t.Skip("DRIVE9_EXTENT_JOINT_BASE not set; live engine round-trip runs only against a v2 server")
	}
	ctx0 := context.Background()
	cl := client.New(base, os.Getenv("DRIVE9_EXTENT_JOINT_KEY"))
	// Provisioning is asynchronous when a fresh key was provided.
	for i := 0; i < 60; i++ {
		if _, err := cl.StatCtx(ctx0, "/"); err == nil {
			break
		} else if i == 59 {
			t.Fatalf("tenant never became active: %v", err)
		}
		time.Sleep(time.Second)
	}

	root := "/jfsbridge-live"
	if err := cl.MkdirCtx(ctx0, root, 0o755); err != nil && !client.IsNotFound(err) && !strings.Contains(err.Error(), "exist") {
		t.Fatalf("mkdir root: %v", err)
	}

	b := NewBridge(Config{Client: cl, RemoteRoot: root})
	chunkConf := chunk.Config{
		BlockSize:   blockSizeMiB << 20,
		CacheDir:    t.TempDir(),
		CacheSize:   1 << 30,
		CacheMode:   0o600,
		AutoCreate:  true,
		Compress:    "none",
		MaxUpload:   4,
		MaxDownload: 4,
		BufferSize:  32 << 30, // large: sidestep the AllocMemory-based throttle while validating the bridge
		Readahead:   0,
		GetTimeout:  time.Minute,
		PutTimeout:  time.Minute,
	}
	store := chunk.NewCachedStore(b.Storage(), chunkConf, nil)
	defer func() { _ = store.UsedMemory() }()

	conf := &vfs.Config{
		Meta:            &meta.Config{Retries: 1},
		Format:          b.VolumeFormat(),
		Chunk:           &chunkConf,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		DirEntryTimeout: time.Second,
	}
	v := vfs.NewVFS(conf, b, store, nil, nil)

	ctx := vfs.NewLogContext(meta.NewContext(1, 0, []uint32{0}))

	// 1. Create a fresh file and write 5 MiB of random data past the
	//    block size, in 1 MiB pwrite chunks.
	name := time.Now().Format("big-150405.000.bin")
	fe, fh, errno := v.Create(ctx, rootIno, name, 0o644, 0, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("vfs create: %v", errno)
	}
	ino := fe.Inode
	var want bytes.Buffer
	_, _ = want.Write(make([]byte, 0, 5*(1<<20)))
	chunk1 := 1 << 20
	for i := 0; i < 5; i++ {
		p := make([]byte, chunk1)
		if _, err := rand.Read(p); err != nil {
			t.Fatal(err)
		}
		want.Write(p)
		if err := v.Write(ctx, ino, p, uint64(i*chunk1), fh); err != 0 {
			t.Fatalf("vfs write @%d: %v", i*chunk1, err)
		}
	}
	if err := v.Flush(ctx, ino, fh, 0); err != 0 {
		t.Fatalf("vfs flush: %v", err)
	}
	if err := v.Fsync(ctx, ino, 0, fh); err != 0 {
		t.Fatalf("vfs fsync: %v", err)
	}

	// Slice commits ride the engine's asynchronous commit thread; wait for
	// the file to land server-side.
	var st *client.StatResult
	for i := 0; i < 100; i++ {
		var serr error
		st, serr = cl.StatCtx(ctx0, root+"/"+name)
		if serr == nil {
			break
		}
		if i == 99 {
			t.Fatalf("file never landed: %v", serr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st.ContentLayout != client.ContentLayoutExtent {
		t.Fatalf("content_layout = %q, want extent", st.ContentLayout)
	}
	if st.Size != int64(want.Len()) {
		t.Fatalf("server size = %d, want %d", st.Size, want.Len())
	}

	// 2. Read the whole file back through the engine (fresh open, so the
	//    read plan path is exercised, not just the write cache).
	v.Release(ctx, ino, fh)
	store.UsedMemory()
	_, fh2, oerr := v.Open(ctx, ino, syscall.O_RDWR)
	if oerr != 0 {
		t.Fatalf("vfs open: %v", oerr)
	}
	got := make([]byte, want.Len())
	for off := 0; off < want.Len(); {
		n := 128 << 10
		if off+n > want.Len() {
			n = want.Len() - off
		}
		rn, rerr := v.Read(ctx, ino, got[off:off+n], uint64(off), fh2)
		if rerr != 0 {
			t.Fatalf("vfs read @%d: %v", off, rerr)
		}
		if rn == 0 {
			t.Fatalf("short read at %d", off)
		}
		off += rn
	}
	if !bytes.Equal(got, want.Bytes()) {
		for i := 0; i < len(got); i++ {
			if got[i] != want.Bytes()[i] {
				lo := i - 16
				if lo < 0 {
					lo = 0
				}
				t.Fatalf("engine round-trip mismatch at %d (len %d): got %x want %x", i, len(got), got[lo:i+16], want.Bytes()[lo:i+16])
			}
		}
		t.Fatalf("engine round-trip mismatch: lengths %d", len(got))
	}

	// 3. Cross-plane check: the v2 thin client reads the same bytes the
	//    JuiceFS engine wrote.
	rc, err := cl.ReadStream(ctx0, root+"/"+name)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	thin, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(thin, want.Bytes()) {
		t.Fatalf("thin-client read mismatch: %d bytes", len(thin))
	}

	// 4. Incremental engine write on the existing extent file: overwrite a
	//    64 KiB window mid-file and append a tail, then re-read.
	patch := make([]byte, 64<<10)
	if _, err := rand.Read(patch); err != nil {
		t.Fatal(err)
	}
	copy(got[2*(1<<20):], patch)
	if err := v.Write(ctx, ino, patch, uint64(2*(1<<20)), fh2); err != 0 {
		t.Fatalf("vfs patch write: %v", err)
	}
	tail := []byte("engine-append-tail")
	if err := v.Write(ctx, ino, tail, uint64(want.Len()), fh2); err != 0 {
		t.Fatalf("vfs append write: %v", err)
	}
	if err := v.Flush(ctx, ino, fh2, 0); err != 0 {
		t.Fatalf("vfs flush 2: %v", err)
	}
	full := append(append([]byte{}, got...), tail...)

	// Fresh open + full read again.
	v.Release(ctx, ino, fh2)
	_, fh3, oerr2 := v.Open(ctx, ino, syscall.O_RDWR)
	if oerr2 != 0 {
		t.Fatalf("vfs open 2: %v", oerr2)
	}
	got2 := make([]byte, len(full))
	for off := 0; off < len(full); {
		n := 128 << 10
		if off+n > len(full) {
			n = len(full) - off
		}
		rn, rerr := v.Read(ctx, ino, got2[off:off+n], uint64(off), fh3)
		if rerr != 0 {
			t.Fatalf("vfs read 2 @%d: %v", off, rerr)
		}
		if rn == 0 {
			t.Fatalf("short read 2 at %d", off)
		}
		off += rn
	}
	if !bytes.Equal(got2, full) {
		t.Fatalf("post-patch round-trip mismatch: %d bytes", len(got2))
	}
}
