package fuse

import (
	"context"
	"net/http/httptest"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/bulkfile"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/git"
	"github.com/mem9-ai/drive9/pkg/extensions/renameguard"
	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/extensions/sse"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/extensions/xattr"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// phantomMountChain builds the facade exactly like Mount() does for a
// cache-less default mount over the fstest backend.
func phantomMountChain(t *testing.T) *FS {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())

	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	bus := fs.NewEventBus()
	rg, _ := renameguard.New(renameguard.Options{})
	ge, err := git.New(git.Options{Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	bf, _ := bulkfile.New(bulkfile.Options{Client: cl, RemoteRoot: "/", Events: bus})
	sp, _ := specials.New(specials.Options{})
	xe, _ := xattr.New(xattr.Options{})
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, SyncMode: "strict", Client: cl, RemoteRoot: "/", Events: bus})
	se, _ := sse.New(sse.Options{Client: cl, Actor: "probe", RemoteRoot: "/", Events: bus})
	f, err := New(Options{Extensions: []fs.Handler{rg, ge, bf, sp, xe, wb, se, terminal}, Events: bus, LocalDirAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if dc, ok := f.Ctx.DirCache.(core.NamespaceCache); ok {
		terminal.Attach(dc)
	}
	return f
}

func TestFacadeLookupAfterReaddirMissingNameStaysENOENT(t *testing.T) {
	f := phantomMountChain(t)
	ctx := context.Background()
	f.Init2(ctx)

	// readdir the root first (populates the namespace cache)
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = 1
	if st := f.OpenDir(nil, oi, &oout); st != gofuse.OK {
		t.Fatalf("opendir: %v", st)
	}
	buf := make([]byte, 4096)
	dl := gofuse.NewDirEntryList(buf, 0)
	ri := &gofuse.ReadIn{Size: 4096}
	ri.NodeId, ri.Fh, ri.Offset = 1, oout.Fh, 0
	if st := f.ReadDir(nil, ri, dl); st != gofuse.OK {
		t.Fatalf("readdir: %v", st)
	}

	// lookup a missing name must stay ENOENT
	var out gofuse.EntryOut
	st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "d1", &out)
	if st != gofuse.ENOENT {
		t.Fatalf("lookup d1 = %v (attr mode=%o size=%d), want ENOENT", st, out.Mode, out.Size)
	}
}

// TestFacadeDrainPhaseContract locks the v1 drain JSON phase names the e2e
// contract validates (flush_open_handles, writeback_uploader, commit_queue).
func TestFacadeDrainPhaseContract(t *testing.T) {
	f := phantomMountChain(t)
	resp := f.Drain(context.Background())
	have := map[string]bool{}
	for _, p := range resp.Phases {
		have[p.Name] = true
	}
	for _, want := range []string{"flush_open_handles", "writeback_uploader", "commit_queue"} {
		if !have[want] {
			t.Fatalf("drain phases %v missing %q", resp.Phases, want)
		}
	}
	if !resp.OK {
		t.Fatalf("drain failed: %+v", resp)
	}
}
