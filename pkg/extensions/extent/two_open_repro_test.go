package extent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// provisionJointTenantT mirrors pkg/client.provisionJointTenant for this
// package's live tests.
func provisionJointTenantT(t *testing.T, ctx context.Context, base string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/provision", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("provision: HTTP %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		APIKey   string `json:"api_key"`
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	return out.APIKey
}

// TestClaimedFreshReleaseThenReopenLive reproduces the fio two-phase open
// against a live v2 server: an ordinary kernel O_CREAT is claimed by the
// engine plane from byte zero, the descriptor is closed with zero writes,
// and the file is then re-opened the way fio reopens for O_DIRECT IO
// (O_RDWR|O_DIRECT, no O_CREAT). After the first close the server must hold
// either nothing or an extent file — never a classic occupant that steals
// the reopen from the engine plane.
//
// DRIVE9_EXTENT_JOINT_BASE must point at the live server.
func TestClaimedFreshReleaseThenReopenLive(t *testing.T) {
	base := os.Getenv("DRIVE9_EXTENT_JOINT_BASE")
	if base == "" {
		t.Skip("DRIVE9_EXTENT_JOINT_BASE not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	apiKey := os.Getenv("DRIVE9_EXTENT_JOINT_KEY")
	if apiKey == "" {
		apiKey = provisionJointTenantT(t, ctx, base)
	}
	cl := client.New(base, apiKey)
	for i := 0; i < 60; i++ {
		if _, err := cl.StatCtx(ctx, "/"); err == nil {
			break
		} else if i == 59 {
			t.Fatalf("tenant never became active: %v", err)
		}
		time.Sleep(time.Second)
	}

	eng := NewEngine(EngineConfig{Client: cl, RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 1, Patterns: []string{"*"}, Events: fs.NewEventBus()})

	if err := cl.MkdirCtx(ctx, "/two-open", 0o755); err != nil {
		t.Fatalf("mkdir /two-open: %v", err)
	}

	path := "/two-open/repro-" + time.Now().Format("150405.000") + ".bin"
	next := fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Handle: &stubCore{}}, nil
	})

	// Linux wire flags 0x8041 (O_CREAT|O_WRONLY|O_LARGEFILE) OR'd with
	// darwin's O_CREAT bit so the claim guard passes on either test host;
	// the plane reads FUSE (linux) flags but builds with the host's
	// syscall constants.
	createFlags := uint32(0x8041 | syscall.O_CREAT)

	// Phase 1: fio's create open (O_CREAT|O_WRONLY|O_LARGEFILE).
	res, err := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: path,
		Flags: createFlags, Mode: 0o644, HasMode: true, Kernel: &gofuse.CreateIn{}, Caller: fs.Caller{Pid: 1}}, next)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("phase1 handle = %T, want claimed *engineHandle", res.Handle)
	}

	// Phase 2: fio closes the create fd with zero writes.
	if err := eh.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Phase 3: what does the server hold now?
	st, serr := cl.StatCtx(ctx, path)
	if serr != nil {
		t.Logf("after release: server holds nothing (%v)", serr)
	} else {
		t.Fatalf("after release: server already holds an occupant layout=%q size=%d rev=%d — the reopen will be stolen from the engine plane",
			st.ContentLayout, st.Size, st.Revision)
	}

	// Phase 4: fio's IO open (O_RDWR|O_DIRECT|O_LARGEFILE, no O_CREAT).
	res2, err2 := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: path,
		Flags: 0xc002, Mode: 0o644, HasMode: true, Kernel: &gofuse.OpenIn{}, Caller: fs.Caller{Pid: 1}}, next)
	if err2 != nil {
		t.Fatalf("reopen: %v", err2)
	}
	if _, ok := res2.Handle.(*engineHandle); !ok {
		t.Fatalf("reopen handle = %T, want *engineHandle (engine-created names must stay on the engine plane)", res2.Handle)
	}
}
