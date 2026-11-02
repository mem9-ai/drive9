package localize

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPolicyRemoteOnlyWins(t *testing.T) {
	p := NewPolicy("", []string{"**/x/**"}, nil, []string{"**/x/y/**"}, false)
	if p.Classify("/x/y/z", nil, false) != LayerRemotePersistent {
		t.Fatal("remote-only must win")
	}
}

func TestPolicyRouting(t *testing.T) {
	p := NewPolicy("", []string{"**/node_modules/**"}, nil, nil, false)
	if p.Classify("/repo/node_modules/x", nil, false) != LayerLocalOnly {
		t.Fatal("node_modules must be local")
	}
	if p.Classify("/repo/src/main.go", nil, false) != LayerRemotePersistent {
		t.Fatal("source must be remote")
	}
}

func TestReservedNamespaceNeverRoutesLocal(t *testing.T) {
	f, err := New(Options{
		LocalRoot:         t.TempDir(),
		LocalOnlyPatterns: []string{"**/node_modules/**"},
		TransientRoot:     t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		"/.drive9/git-workspaces/ws-1/overlay/repo/node_modules/x.js",
		"/.drive9/layers/layer_1/tree/repo/node_modules/y.js",
	} {
		if _, claimed, err := f.StatLocal(context.Background(), fs.NewEmptyFilesystem(), p); claimed || err != nil {
			t.Fatalf("StatLocal(%q) = claimed=%v err=%v, want reserved namespace to stay remote", p, claimed, err)
		}
	}
}

func TestStatLocalPlane(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "overlay", "repo", "node_modules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "overlay", "repo", "node_modules", "x.js"), []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{LocalRoot: root, LocalOnlyPatterns: []string{"**/node_modules/**"}})
	if err != nil {
		t.Fatal(err)
	}
	st, claimed, err := f.StatLocal(context.Background(), fs.NewEmptyFilesystem(), "/repo/node_modules/x.js")
	if err != nil || !claimed {
		t.Fatalf("StatLocal = (%v, %v, %v)", st, claimed, err)
	}
	if st.Size != 7 {
		t.Fatalf("size = %d", st.Size)
	}
	_, claimed, _ = f.StatLocal(context.Background(), fs.NewEmptyFilesystem(), "/repo/plain.go")
	if claimed {
		t.Fatal("unclaimed path must pass")
	}
}

func TestStatLocalShadowsRemote(t *testing.T) {
	// A local-classified path is claimed on classification alone: a remote
	// copy at the same path stays invisible while the overlay owns it, and
	// a missing backing file replies ENOENT instead of falling through.
	f, err := New(Options{LocalRoot: t.TempDir(), LocalOnlyPatterns: []string{"**/node_modules/**"}})
	if err != nil {
		t.Fatal(err)
	}
	inner := fs.HandlerFilesystem(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		if op.Kind == fs.OpGetAttr && op.Path == "/repo/node_modules/remote.js" {
			attr := fs.Attr{Kind: fs.KindFile, Size: 5}
			return fs.Result{Attr: &attr}, nil
		}
		return fs.Result{}, nil
	})
	_, err = f.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/node_modules/remote.js"}, fs.Next(inner))
	if !os.IsNotExist(err) {
		t.Fatalf("GetAttr on shadowed path = %v, want ENOENT", err)
	}
}

func TestTransientShmRouting(t *testing.T) {
	f, err := New(Options{TransientRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if f.transient == nil {
		t.Fatal("transient plane missing")
	}
	f.noteDBOpen("/a.db")
	if o := f.transientFor("/a.db-shm", false); o == nil {
		t.Fatal("shm not routed transient with db open")
	}
	if o := f.transientFor("/a.db-shm", true); o != nil {
		t.Fatal("dir-hinted shm must not route transient")
	}
	if o := f.transientFor("/bare--shm", false); o != nil {
		t.Fatal("bare -shm must never route transient")
	}
	f.noteDBClose("/a.db")
	if o := f.transientFor("/a.db-shm", false); o != nil {
		t.Fatal("shm still routed after close")
	}
}

func TestReexecRefusal(t *testing.T) {
	// An enabled persistent overlay alone refuses (its state is
	// process-local); the transient plane is only consulted without one.
	f, err := New(Options{LocalRoot: t.TempDir(), LocalOnlyPatterns: []string{"**/x/**"}})
	if err != nil {
		t.Fatal(err)
	}
	if reason, n := f.ReexecRefusal(); n != 1 || reason == "" {
		t.Fatalf("overlay-enabled refusal = (%q, %d), want count 1", reason, n)
	}

	// Transient-only mount: empty plane passes, live entries refuse.
	g, err := New(Options{TransientRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if reason, n := g.ReexecRefusal(); n != 0 || reason != "" {
		t.Fatalf("empty transient = (%q, %d), want pass", reason, n)
	}
	g.noteDBOpen("/a.db")
	if err := os.WriteFile(filepath.Join(g.transient.Root(), "a.db-shm"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if reason, n := g.ReexecRefusal(); n != 1 || reason == "" {
		t.Fatalf("live transient = (%q, %d), want refusal", reason, n)
	}
}
