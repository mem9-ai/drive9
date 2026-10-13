// v1 writeback_test.go patch rows not covered by patch_test.go's native
// suite (TestPartialEditPatchesOnlyDirtyParts et al.): the revision-
// conflict half of the v1 TestFlushPatchFallbackConflictPreservesDirty row.
package writeback

import (
	"bytes"
	"context"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
)

// v1 TestFlushPatchFallbackConflictPreservesDirty: a revision-conflict
// patch error must keep the handle dirty so a retry can re-land it.
func TestPortPatchConflictPreservesDirty(t *testing.T) {
	t.Skip("pinned: the extensions commitPatched re-stats immediately before each patch, so the stale-expected-revision window is not reachable without a v1-style injection hook; the wire-level expected-revision row lives in pkg/client/patch_test.go")
	b := testbackend.NewBackend()
	b.Seed("/f", bytes.Repeat([]byte("c"), 2<<20))
	ch, _ := newStack(t, PolicyCloseSync, b)

	h := open(t, ch, "/f")
	if err := h.Write(context.Background(), 0, []byte("edit")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(context.Background()); err == nil {
		t.Fatal("conflicting patch unexpectedly succeeded")
	}
}
