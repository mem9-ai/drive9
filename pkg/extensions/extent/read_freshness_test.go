package extent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
)

// A read must never serve an image OLDER than a revision this mount already
// published.
//
// The write path publishes through the server, and the following read can be
// answered by a replica that has not caught up. Serving that plan hands the
// caller a pre-commit image. For SQLite this is fatal: it reads page 1 with an
// old schema cookie, reports SQLITE_SCHEMA, reloads the schema, and then
// cannot find the table the previous statement just created.
//
// The revision arrives in the RESPONSE HEADER (X-Dat9-Revision), never in the
// plan body — the server writes setFileReadHeaders before encoding the plan,
// and the plan struct has only size/blocks/next_offset. This test models that
// exactly: a body without a revision, a header that carries it.
func TestReadRefusesPlanOlderThanPublishedRevision(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		// The first two answers are the lagging replica; the third is caught up.
		rev := int64(7)
		if n >= 3 {
			rev = 9
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
		// Deliberately NO revision field in the body, as the server sends it.
		_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
			Size: 4096,
			Blocks: []client.ExtentReadPlanBlock{
				{FileOffset: 0, Length: 4096, Zero: true},
			},
		})
	}))
	defer srv.Close()

	br := NewBridge(Config{Client: client.New(srv.URL, ""), RemoteRoot: "/"})
	const ino = meta.Ino(42)
	// This mount already published revision 9.
	br.recordLandedRevision(ino, 9)

	plan, err := br.fetchFreshReadPlan(context.Background(), ino, "/t.db", 0, 4095)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	if plan.Revision != 9 {
		t.Fatalf("read served plan revision %d; a revision this mount already published (9) must not be undercut", plan.Revision)
	}
	if calls.Load() < 3 {
		t.Fatalf("fetched %d plans; the stale answers should have been retried", calls.Load())
	}
}

// When no landings are known for the inode, the first plan is accepted: a
// read-only mount (or a file written by another client) must not pay a retry
// budget it cannot use.
func TestReadAcceptsFirstPlanWithoutKnownRevision(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Dat9-Content-Layout", "extent")
		_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{Size: 4096, Revision: 2})
	}))
	defer srv.Close()

	br := NewBridge(Config{Client: client.New(srv.URL, ""), RemoteRoot: "/"})
	plan, err := br.fetchFreshReadPlan(context.Background(), meta.Ino(43), "/t.db", 0, 4095)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	if plan.Revision != 2 {
		t.Fatalf("plan revision = %d, want 2", plan.Revision)
	}
	if calls.Load() != 1 {
		t.Fatalf("fetched %d plans, want exactly 1 (no retry budget to spend)", calls.Load())
	}
}

// The published revision must be recorded from the completion echo, not
// guessed: without it the read side has nothing to compare against.
func TestCommitRecordsEchoedRevision(t *testing.T) {
	br := NewBridge(Config{RemoteRoot: "/"})
	const ino = meta.Ino(44)
	if got := br.publishedRevision(ino); got != 0 {
		t.Fatalf("fresh bridge reports published revision %d, want 0", got)
	}
	br.recordLandedRevision(ino, 5)
	if got := br.publishedRevision(ino); got != 5 {
		t.Fatalf("published revision = %d, want 5", got)
	}
	// A later echo only moves forward.
	br.recordLandedRevision(ino, 3)
	if got := br.publishedRevision(ino); got != 5 {
		t.Fatalf("published revision regressed to %d, want 5", got)
	}
	br.recordLandedRevision(ino, 8)
	if got := br.publishedRevision(ino); got != 8 {
		t.Fatalf("published revision = %d, want 8", got)
	}
}
