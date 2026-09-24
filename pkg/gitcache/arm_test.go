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

func TestLocalArmSignalEmpty(t *testing.T) {
	root := t.TempDir()
	ok, gen := LocalArmSignal(context.Background(), root)
	if ok {
		t.Fatal("LocalArmSignal on empty root = true, want false")
	}
	if gen != "" {
		t.Fatalf("gen = %q, want empty", gen)
	}
}

func TestLocalArmSignalArmedFile(t *testing.T) {
	root := t.TempDir()
	if err := TouchWorkspaceArmed(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	ok, gen := LocalArmSignal(context.Background(), root)
	if !ok {
		t.Fatal("LocalArmSignal after TouchWorkspaceArmed = false, want true")
	}
	if gen == "" {
		t.Fatal("gen is empty after armed touch")
	}
}

func TestLocalArmSignalRefreshDir(t *testing.T) {
	root := t.TempDir()
	if err := ClearWorkspaceDeleted(context.Background(), root, "ws-new"); err != nil {
		t.Fatal(err)
	}
	ok, gen := LocalArmSignal(context.Background(), root)
	if !ok {
		t.Fatal("LocalArmSignal after refresh marker = false, want true")
	}
	if gen == "" {
		t.Fatal("gen empty after refresh marker")
	}
}

func TestLocalArmSignalGenerationAdvancesWithNewIDAtEqualMtime(t *testing.T) {
	root := t.TempDir()
	// Isolate the *name* contribution: fixed armed body + only refresh names change.
	if err := os.MkdirAll(filepath.Dir(WorkspaceArmedPath(root)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(WorkspaceArmedPath(root), []byte("fixed-armed-body\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(WorkspaceRefreshDir(root), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(WorkspaceRefreshMarkerPath(root, "ws1"), []byte("ws1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ok1, gen1 := LocalArmSignal(context.Background(), root)
	if !ok1 || gen1 == "" {
		t.Fatalf("first signal ok=%v gen=%q", ok1, gen1)
	}

	// Add a second refresh id without rewriting armed (name-only gen advance).
	if err := os.WriteFile(WorkspaceRefreshMarkerPath(root, "ws2"), []byte("ws2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fixed := time.Unix(1_700_000_000, 0)
	paths := []string{
		WorkspaceArmedPath(root),
		WorkspaceRefreshMarkerPath(root, "ws1"),
		WorkspaceRefreshMarkerPath(root, "ws2"),
	}
	for _, p := range paths {
		if err := os.Chtimes(p, fixed, fixed); err != nil {
			t.Fatalf("Chtimes %s: %v", p, err)
		}
	}
	ok2, gen2 := LocalArmSignal(context.Background(), root)
	if !ok2 {
		t.Fatal("second signal not armed")
	}
	if gen2 == gen1 {
		t.Fatalf("gen unchanged after new refresh/<id> name at equal mtime: %q", gen2)
	}

	// Unchanged set → same generation.
	ok3, gen3 := LocalArmSignal(context.Background(), root)
	if !ok3 || gen3 != gen2 {
		t.Fatalf("stable re-scan ok=%v gen=%q want %q", ok3, gen3, gen2)
	}

	// Body-only change (same names) still advances gen.
	if err := os.WriteFile(WorkspaceRefreshMarkerPath(root, "ws2"), []byte("ws2-rewritten\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(WorkspaceRefreshMarkerPath(root, "ws2"), fixed, fixed); err != nil {
		t.Fatal(err)
	}
	ok4, gen4 := LocalArmSignal(context.Background(), root)
	if !ok4 || gen4 == gen2 {
		t.Fatalf("body rewrite ok=%v gen=%q want != %q", ok4, gen4, gen2)
	}
}

func TestMarkWorkspaceRegistered(t *testing.T) {
	root := t.TempDir()
	if err := MarkWorkspaceRegistered(context.Background(), root, "ws1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(WorkspaceArmedPath(root)); err != nil {
		t.Fatalf("armed marker: %v", err)
	}
	if _, err := os.Stat(WorkspaceRefreshMarkerPath(root, "ws1")); err != nil {
		t.Fatalf("refresh marker: %v", err)
	}
	// deleted marker should not exist
	if _, err := os.Stat(WorkspaceDeletedMarkerPath(root, "ws1")); !os.IsNotExist(err) {
		t.Fatalf("deleted marker should be absent, err=%v", err)
	}
	_ = filepath.Join(root, "x")
}

func TestClearLocalArmSignals(t *testing.T) {
	root := t.TempDir()
	if err := MarkWorkspaceRegistered(context.Background(), root, "ws1"); err != nil {
		t.Fatal(err)
	}
	if err := ClearLocalArmSignals(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(WorkspaceArmedPath(root)); !os.IsNotExist(err) {
		t.Fatalf("armed marker still present after clear, err=%v", err)
	}
	if _, err := os.Stat(WorkspaceRefreshMarkerPath(root, "ws1")); !os.IsNotExist(err) {
		t.Fatalf("refresh marker still present after clear, err=%v", err)
	}
	ok, gen := LocalArmSignal(context.Background(), root)
	if ok || gen != "" {
		t.Fatalf("LocalArmSignal after ClearLocalArmSignals ok=%v gen=%q", ok, gen)
	}
}

func TestLocalArmSignalEmptyRefreshDir(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(WorkspaceRefreshDir(root), 0o755); err != nil {
		t.Fatal(err)
	}
	ok, gen := LocalArmSignal(context.Background(), root)
	if ok || gen != "" {
		t.Fatalf("LocalArmSignal with empty refresh/ only ok=%v gen=%q", ok, gen)
	}
}

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

func TestClearLocalArmSignalsDropsPendingMarkers(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := MarkWorkspacePending(ctx, root, "/repo"); err != nil {
		t.Fatalf("MarkWorkspacePending: %v", err)
	}
	if err := ClearLocalArmSignals(ctx, root); err != nil {
		t.Fatalf("ClearLocalArmSignals: %v", err)
	}
	if WorkspacePending(ctx, root, "/repo") {
		t.Fatal("ClearLocalArmSignals should drop pending markers")
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
