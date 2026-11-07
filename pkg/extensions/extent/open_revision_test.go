package extent

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Opening an EXISTING extent file must report its revision.
//
// That value is the compare-and-set base every later write needs. Dropping it
// made the write-back plane see baseRev 0 ("unknown"), refuse the overwrite
// with "missing base revision for overwrite", and leave the file at its
// opened size forever. The empty-database case is the visible one: SQLite
// creates a 0-byte file, opens it, writes the schema, and the schema never
// lands — so the next statement reports SQLITE_SCHEMA.
func TestOpenExistingExtentFileReportsRevision(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/d/a.db", "schema")
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 1})

	res, err, passed := dispatchCreate(context.Background(), h, "/d/a.db", syscall.O_RDWR)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if passed {
		t.Fatal("an existing extent file was not claimed by the engine plane")
	}
	if res.Attr == nil {
		t.Fatal("open returned no attributes")
	}
	if res.Attr.Rev <= 0 {
		t.Fatalf("open reported Rev=%d; the CAS base is lost, so every later overwrite is refused", res.Attr.Rev)
	}
	// The same must hold through the "fresh create still local to the bridge"
	// claim path, which reports from the bridge's own view.
	if res.Attr.Size != int64(len("schema")) {
		t.Fatalf("open reported Size=%d, want %d", res.Attr.Size, len("schema"))
	}
}
