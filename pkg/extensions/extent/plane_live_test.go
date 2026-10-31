package extent

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// TestEnginePlaneLive drives the engine plane end to end against a live
// v2 server: a full-mode (threshold 0) create claimed by the extension,
// written through the community engine, landed server-side as extent,
// re-claimed by a fresh handler, read back byte-identical, and
// cross-checked by the v2 thin client.
//
//	DRIVE9_EXTENT_JOINT_BASE=http://127.0.0.1:9410 \
//	DRIVE9_EXTENT_JOINT_KEY=... \
//	  go test ./pkg/extensions/jfsengine/ -run TestEnginePlaneLive -v -count=1
func TestEnginePlaneLive(t *testing.T) {
	base := os.Getenv("DRIVE9_EXTENT_JOINT_BASE")
	if base == "" {
		t.Skip("DRIVE9_EXTENT_JOINT_BASE not set; live engine-plane test runs only against a v2 server")
	}
	ctx0 := context.Background()
	cl := client.New(base, os.Getenv("DRIVE9_EXTENT_JOINT_KEY"))
	for i := 0; i < 60; i++ {
		if _, err := cl.StatCtx(ctx0, "/"); err == nil {
			break
		} else if i == 59 {
			t.Fatalf("tenant never became active: %v", err)
		}
		time.Sleep(time.Second)
	}

	eng := NewEngine(EngineConfig{
		Client:     cl,
		RemoteRoot: "/jfsengine-live",
		CacheDir:   t.TempDir(),
	})
	if err := cl.MkdirCtx(ctx0, "/jfsengine-live", 0o755); err != nil && !strings.Contains(err.Error(), "exist") {
		t.Fatalf("mkdir root: %v", err)
	}
	h := New(Options{Engine: eng, Threshold: 0}) // full JuiceFS mode

	name := time.Now().Format("engine-150405.000.bin")
	res, err, passed := dispatchCreate(ctx0, h, "/"+name, syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("full mode must claim the fresh create")
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	var want []byte
	for off := 0; off < 5; off++ {
		p := make([]byte, 1<<20)
		if _, err := rand.Read(p); err != nil {
			t.Fatal(err)
		}
		want = append(want, p...)
		if err := eh.Write(ctx0, int64(off*(1<<20)), p); err != nil {
			t.Fatalf("write @%d: %v", off*(1<<20), err)
		}
	}
	if err := eh.Sync(ctx0); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// The engine's commit thread lands the file asynchronously.
	var st *client.StatResult
	for i := 0; i < 150; i++ {
		var serr error
		st, serr = cl.StatCtx(ctx0, "/jfsengine-live/"+name)
		if serr == nil {
			break
		}
		if i == 149 {
			t.Fatalf("file never landed: %v", serr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if st.ContentLayout != client.ContentLayoutExtent {
		t.Fatalf("content_layout = %q, want extent", st.ContentLayout)
	}
	if st.Size != int64(len(want)) {
		t.Fatalf("server size = %d, want %d", st.Size, len(want))
	}

	// Fresh handler (fresh claim of the now-extent file) reads it back.
	h2 := New(Options{Engine: NewEngine(EngineConfig{
		Client:     cl,
		RemoteRoot: "/jfsengine-live",
		CacheDir:   t.TempDir(),
	}), Threshold: 1 << 20})
	res2, err, passed := dispatchCreate(ctx0, h2, "/"+name, syscall.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	if passed {
		t.Fatal("extent file must be claimed by the engine plane")
	}
	eh2 := res2.Handle.(*engineHandle)
	got := make([]byte, len(want))
	for off := 0; off < len(want); {
		n := 128 << 10
		if off+n > len(want) {
			n = len(want) - off
		}
		rn, rerr := eh2.Read(ctx0, int64(off), got[off:off+n])
		if rerr != nil || rn == 0 {
			t.Fatalf("read @%d: %v (%d)", off, rerr, rn)
		}
		off += rn
	}
	if string(got) != string(want) {
		t.Fatalf("engine-plane round-trip mismatch: %d bytes", len(got))
	}

	// Cross-plane: the thin client agrees.
	rc, err := cl.ReadStream(ctx0, "/jfsengine-live/"+name)
	if err != nil {
		t.Fatal(err)
	}
	thin, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(thin) != string(want) {
		t.Fatalf("thin-client cross-check mismatch: %d bytes", len(thin))
	}
}
