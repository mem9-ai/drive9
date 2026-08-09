package gitcache

import (
	"context"
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
