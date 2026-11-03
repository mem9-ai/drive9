// v1 recovery_incarnation rows, queue/ledger halves: a torn (short)
// shadow without journal evidence is never uploaded and never becomes a
// read source; a conflicted-but-valid recovery entry stays readable
// locally (preserved for manual recovery); shadow provenance transitions
// follow full coverage only.
package writeback

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// newTrackedStaging creates a ledger whose root is registered for
// shadow-file lookup.
func newTrackedStaging(t *testing.T) *staging.Staging {
	t.Helper()
	dir := t.TempDir()
	ledger, err := staging.NewStaging(dir)
	if err != nil {
		t.Fatal(err)
	}
	shadowRoots.Store(ledger, dir)
	return ledger
}

// v1 TestRecoveryRejectsShortShadowWithoutJournal: meta promises 6 bytes,
// the shadow holds 2 — recovery rejects (no upload, no readable
// generation).
func TestPortRecoveryRejectsShortShadowWithoutJournal(t *testing.T) {
	var uploads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			uploads.Add(1)
			_, _ = w.Write([]byte(`{"revision":8}`))
			return
		}
		_, _ = w.Write([]byte("REMOTE"))
	}))
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.SetSmallFileThresholdForTests(1 << 20)

	const path = "/torn"
	ledger := newTrackedStaging(t)
	// Stage 6 real bytes, then tear the file down to 2: metadata
	// promises 6, disk holds 2.
	if err := ledger.Shadow.WriteFull(path, []byte("six-by"), 7); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutShadowSpill(path, 6, staging.PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}
	shadowFile := findShadowFile(t, ledger)
	if err := os.WriteFile(shadowFile, []byte("TO"), 0o600); err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(cl, ledger.Shadow, ledger.Pending, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()
	if got := uploads.Load(); got != 0 {
		t.Fatalf("uploaded truncated recovery %d times", got)
	}
}

// v1 TestRecoveryKeepsValidConflictReadable (queue half): a conflicted
// entry is never re-uploaded by recovery, and its bytes stay in the
// shadow (preserved for manual recovery).
func TestPortRecoveryKeepsConflictPreservedNotUploaded(t *testing.T) {
	var uploads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		uploads.Add(1)
		http.Error(w, "should not be reached", http.StatusMethodNotAllowed)
	}))
	defer ts.Close()
	cl := client.New(ts.URL, "")

	const path = "/conflict"
	ledger := newTrackedStaging(t)
	if err := ledger.Shadow.WriteFull(path, []byte("staged"), 1); err != nil {
		t.Fatal(err)
	}
	gen, err := ledger.Pending.PutShadowSpill(path, 6, staging.PendingOverwrite, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.MarkConflictIfGeneration(path, gen); err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(cl, ledger.Shadow, ledger.Pending, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()
	if got := uploads.Load(); got != 0 {
		t.Fatalf("conflicted entry was re-uploaded %d times", got)
	}
	if data, rerr := ledger.Shadow.ReadAll(path); rerr != nil || string(data) != "staged" {
		t.Fatalf("conflict bytes lost: %q err=%v", data, rerr)
	}
}

// v1 TestShadowExtentsEstablishProvenanceOnlyAfterFullCoverage:
// provenance binds only when the recovered bytes cover the promised
// size — the queue half: a full shadow mints a generation, a short one
// does not.
func TestPortRecoveryProvenanceRequiresFullCoverage(t *testing.T) {
	ledger := newTrackedStaging(t)
	const full = "/full"
	if err := ledger.Shadow.WriteFull(full, []byte("six-bytes"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutShadowSpill(full, 9, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Shadow.WriteFull("/shorty", []byte("TO"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutShadowSpill("/shorty", 6, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	cl := client.New("http://127.0.0.1:1", "")
	cq := NewCommitQueue(cl, ledger.Shadow, ledger.Pending, nil, 1, 8)
	cq.RecoverPending()
	// The full entry was enqueued for upload (recovered); the short one
	// was skipped without a generation.
	snapFull := cq.Snapshot()
	_ = snapFull
	fullMeta, ok := ledger.Pending.GetMeta(full)
	if !ok {
		t.Fatal("full meta lost")
	}
	if gen := cq.index.RecoverShadowSource(full, fullMeta.Generation, ledger.Shadow); gen == 0 {
		t.Fatal("full-coverage shadow did not mint a generation")
	}
	shortMeta, ok := ledger.Pending.GetMeta("/shorty")
	if !ok {
		t.Fatal("short meta lost")
	}
	if gen := cq.index.RecoverShadowSource("/shorty", shortMeta.Generation, ledger.Shadow); gen != 0 {
		t.Fatalf("short shadow minted generation %d, want rejection", gen)
	}
}

// shadowRoot tracks the ledger's root for shadow-file location.
var shadowRoots sync.Map // *staging.Staging → string

// findShadowFile locates the single shadow file of a fresh ledger: the
// store writes <root>/shadow/<hash>.shadow, and NewStaging root is the
// dir passed in — captured by the caller.
func findShadowFile(t *testing.T, ledger *staging.Staging) string {
	t.Helper()
	root, _ := shadowRoots.Load(ledger)
	str, ok := root.(string)
	if !ok || str == "" {
		t.Fatal("shadow root not registered for this ledger")
	}
	matches, err := filepath.Glob(filepath.Join(str, "shadow", "*.shadow"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("shadow files under %s: %v %v", str, matches, err)
	}
	return matches[0]
}
