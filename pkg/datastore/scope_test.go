package datastore

import "testing"

func TestScopeSelCols(t *testing.T) {
	t.Helper()
	if got := SharedScope(42).SelCols("task_id"); got != "fs_id, task_id" {
		t.Fatalf("shared SelCols = %q, want %q", got, "fs_id, task_id")
	}
	if got := StandaloneScope(42).SelCols("task_id"); got != "task_id" {
		t.Fatalf("standalone SelCols = %q, want %q", got, "task_id")
	}
}
