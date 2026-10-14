package gitcache

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkspacePendingMarkerLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	if WorkspacePending(ctx, root, "/repo") {
		t.Fatal("no marker should mean not pending")
	}
	if err := MarkWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("MarkWorkspacePending: %v", err)
	}
	if !WorkspacePending(ctx, root, "/repo") {
		t.Fatal("marker written but WorkspacePending = false")
	}
	// Trailing/leading slashes normalize to the same key.
	if !WorkspacePending(ctx, root, "repo/") {
		t.Fatal("normalized root should match the same marker")
	}
	if WorkspacePending(ctx, root, "/other") {
		t.Fatal("unrelated root must not be pending")
	}
	if err := ClearWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("ClearWorkspacePending: %v", err)
	}
	if WorkspacePending(ctx, root, "/repo") {
		t.Fatal("marker cleared but WorkspacePending = true")
	}
	// Clearing an absent marker is not an error.
	if err := ClearWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("ClearWorkspacePending(absent): %v", err)
	}
	if err := ClearWorkspacePending(ctx, root, "/other"); err != nil {
		t.Fatalf("ClearWorkspacePending(other): %v", err)
	}
}

func TestWorkspacePendingMarkerPathLayout(t *testing.T) {
	root := "/local/root"
	// The marker name is a flat key derived from the canonical root: segment
	// layouts cannot represent "/" and "/root" at once, and parent/child roots
	// collide as file-vs-directory.
	tests := []struct {
		mountRoot string
		want      string
	}{
		{mountRoot: "/", want: filepath.Join(root, "git-workspaces", "pending", "pending-"+mustPendingMarkerName(t, "/"))},
		{mountRoot: "", want: filepath.Join(root, "git-workspaces", "pending", "pending-"+mustPendingMarkerName(t, "/"))},
		{mountRoot: "repo", want: filepath.Join(root, "git-workspaces", "pending", "pending-"+mustPendingMarkerName(t, "/repo"))},
		{mountRoot: "/repo/", want: filepath.Join(root, "git-workspaces", "pending", "pending-"+mustPendingMarkerName(t, "/repo"))},
		{mountRoot: "/a/b", want: filepath.Join(root, "git-workspaces", "pending", "pending-"+mustPendingMarkerName(t, "/a/b"))},
	}
	for _, test := range tests {
		if got := WorkspacePendingMarkerPath(root, test.mountRoot); got != test.want {
			t.Errorf("WorkspacePendingMarkerPath(%q) = %q, want %q", test.mountRoot, got, test.want)
		}
	}
}

func mustPendingMarkerName(t *testing.T, canonicalRoot string) string {
	t.Helper()
	h := fnv.New64a()
	_, _ = h.Write([]byte(canonicalRoot))
	return fmt.Sprintf("%016x", h.Sum64())
}

// TestWorkspacePendingMarkerRootVsNestedNames covers the two collisions of the
// old per-segment layout: the mount root "/" vs a workspace root named "root",
// and parent/child roots ("/repo" vs "/repo/sub") that used to fight over one
// pathname as file vs directory.
func TestWorkspacePendingMarkerRootVsNestedNames(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	// "/" and "/root" are distinct keys.
	if err := MarkWorkspacePending(ctx, root, "/"); err != nil {
		t.Fatalf("MarkWorkspacePending(/): %v", err)
	}
	if err := MarkWorkspacePending(ctx, root, "/root"); err != nil {
		t.Fatalf("MarkWorkspacePending(/root): %v", err)
	}
	if WorkspacePendingMarkerPath(root, "/") == WorkspacePendingMarkerPath(root, "/root") {
		t.Fatal("/ and /root must map to different markers")
	}
	if !WorkspacePending(ctx, root, "/") || !WorkspacePending(ctx, root, "/root") {
		t.Fatal("both / and /root should be pending")
	}
	if err := ClearWorkspacePending(ctx, root, "/"); err != nil {
		t.Fatalf("ClearWorkspacePending(/): %v", err)
	}
	if WorkspacePending(ctx, root, "/") {
		t.Fatal("/ should not be pending after clear")
	}
	if !WorkspacePending(ctx, root, "/root") {
		t.Fatal("clearing / must not disturb /root")
	}

	// Parent and child roots coexist.
	if err := MarkWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("MarkWorkspacePending(/repo): %v", err)
	}
	if err := MarkWorkspacePending(ctx, root, "/repo/sub"); err != nil {
		t.Fatalf("MarkWorkspacePending(/repo/sub): %v", err)
	}
	if !WorkspacePending(ctx, root, "/repo") || !WorkspacePending(ctx, root, "/repo/sub") {
		t.Fatal("both /repo and /repo/sub should be pending")
	}
	if err := ClearWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("ClearWorkspacePending(/repo): %v", err)
	}
	if WorkspacePending(ctx, root, "/repo") {
		t.Fatal("/repo should not be pending after clear")
	}
	if !WorkspacePending(ctx, root, "/repo/sub") {
		t.Fatal("clearing /repo must not disturb /repo/sub")
	}
}

// TestWorkspacePendingMarkerBodyValidation pins the hash-collision guard: a
// marker whose body names a different root is not treated as pending for this
// root.
func TestWorkspacePendingMarkerBodyValidation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := MarkWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("MarkWorkspacePending: %v", err)
	}
	if !WorkspacePending(ctx, root, "/repo") {
		t.Fatal("marker should be pending for its own root")
	}
	// Corrupt the body to name a different root.
	marker := WorkspacePendingMarkerPath(root, "/repo")
	if err := os.WriteFile(marker, []byte("/other\n"+time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		t.Fatalf("write corrupt marker: %v", err)
	}
	if WorkspacePending(ctx, root, "/repo") {
		t.Fatal("a marker naming another root must not count as pending for /repo")
	}
}
