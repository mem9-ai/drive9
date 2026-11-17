package writeback

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// A descriptor opened on a 0-byte object can legitimately carry no base
// revision: the server reports none for an empty file on some paths, and
// `expected_revision: 0` means "the path must NOT exist" — so sending it is a
// conflict, not a write. The queue used to refuse the commit outright and the
// object stayed empty forever.
//
// This is exactly what SQLite does at startup: create an empty database file,
// open it, write the schema. The schema never landed, so every later statement
// saw an empty database and reported SQLITE_SCHEMA on the first CREATE INDEX.
func TestEmptyOccupantCommitReclaimsAndPublishes(t *testing.T) {
	srv := newMigrationServer(t)
	// The remote holds a 0-byte object with no revision, as staging reports
	// for an empty file.
	srv.mu.Lock()
	srv.layout = "classic"
	srv.size = 0
	srv.revision = 0
	srv.exists = true // an empty file that really exists
	srv.mu.Unlock()

	shadows, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(client.New(srv.URL, ""), shadows, pending, nil, 1, 8)
	cq.SetExtentTakeover(true)

	const path = "/t.db"
	content := []byte("CREATE TABLE a(x)")
	if err := shadows.WriteFull(path, content, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutShadowSpillWithMode(path, int64(len(content)), staging.PendingOverwrite, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	entry := &CommitEntry{
		Path: path, Size: int64(len(content)), Kind: staging.PendingOverwrite,
		BaseRev:     0,
		ShadowSpill: true, ShadowGen: shadows.ActiveGeneration(path), PendingIndexGen: gen,
	}
	if _, err := cq.uploadEntry(context.Background(), entry); err != nil {
		t.Fatalf("commit over an empty occupant: %v", err)
	}
	// The image must now be remote: the schema write actually landed.
	srv.mu.Lock()
	size, rev := srv.size, srv.revision
	srv.mu.Unlock()
	if size != int64(len(content)) {
		t.Fatalf("remote size after commit = %d, want %d", size, len(content))
	}
	if rev == 0 {
		t.Fatal("remote revision was never advanced: the publication did not happen")
	}
}

// A NON-empty occupant with no base revision must still be refused: its bytes
// belong to someone, and overwriting them without naming a revision is exactly
// the silent-adoption class this check exists for.
func TestNonEmptyOccupantStillRequiresBaseRevision(t *testing.T) {
	srv := newMigrationServer(t)
	srv.mu.Lock()
	srv.layout = "classic"
	srv.size = 4096 // content someone owns
	srv.revision = 0
	srv.exists = true
	srv.mu.Unlock()

	shadows, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(client.New(srv.URL, ""), shadows, pending, nil, 1, 8)
	cq.SetExtentTakeover(true)

	const path = "/owned.bin"
	content := []byte("new bytes")
	if err := shadows.WriteFull(path, content, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutShadowSpillWithMode(path, int64(len(content)), staging.PendingOverwrite, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	entry := &CommitEntry{
		Path: path, Size: int64(len(content)), Kind: staging.PendingOverwrite,
		BaseRev:     0,
		ShadowSpill: true, ShadowGen: shadows.ActiveGeneration(path), PendingIndexGen: gen,
	}
	if _, err := cq.uploadEntry(context.Background(), entry); err == nil {
		t.Fatal("a non-empty object was overwritten without a base revision")
	}
}
