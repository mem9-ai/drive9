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
	ok, mt := LocalArmSignal(context.Background(), root, time.Time{})
	if ok {
		t.Fatal("LocalArmSignal on empty root = true, want false")
	}
	if !mt.IsZero() {
		t.Fatalf("mtime = %v, want zero", mt)
	}
}

func TestLocalArmSignalArmedFile(t *testing.T) {
	root := t.TempDir()
	if err := TouchWorkspaceArmed(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	ok, mt := LocalArmSignal(context.Background(), root, time.Time{})
	if !ok {
		t.Fatal("LocalArmSignal after TouchWorkspaceArmed = false, want true")
	}
	if mt.IsZero() {
		t.Fatal("mtime is zero after armed touch")
	}
}

func TestLocalArmSignalRefreshDir(t *testing.T) {
	root := t.TempDir()
	if err := ClearWorkspaceDeleted(context.Background(), root, "ws-new"); err != nil {
		t.Fatal(err)
	}
	ok, _ := LocalArmSignal(context.Background(), root, time.Time{})
	if !ok {
		t.Fatal("LocalArmSignal after refresh marker = false, want true")
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
