package datastore

import "testing"

// jfsInited accelerates the read-only dispatch; it is not evidence that the
// tenant's root row exists. Re-reading the root row is what keeps a stale
// positive from turning the first extent meta op on a fresh tenant into a
// no-op, which surfaced as EIO on a lookup of inode 1. A nil tx therefore has
// to be refused rather than silently skipped.
func TestJfsEnsureInitRequiresTransaction(t *testing.T) {
	s := &Store{}
	if err := s.jfsEnsureInitTx(nil); err == nil {
		t.Fatal("jfsEnsureInitTx(nil) must return an error")
	}
}

func TestJfsEnsureInitCreatesRootWhenAbsent(t *testing.T) {
	s := newTestStore(t)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.jfsEnsureInitTx(tx); err != nil {
		t.Fatalf("jfsEnsureInitTx: %v", err)
	}
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, jfsRootIno).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("root rows = %d, want 1", n)
	}
	if !s.extentMetaPresent.Load() {
		t.Fatal("creating the root row must mark the tenant as having extent metadata")
	}
}
