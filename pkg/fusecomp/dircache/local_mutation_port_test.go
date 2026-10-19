package dircache

// A row this mount wrote keeps its provenance through the cache: the
// namespace lookup hands LocalMutation back to the terminal's trust gate.

import (
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestLocalMutationRowKeepsProvenance(t *testing.T) {
	dc := New(time.Minute)
	dc.Upsert("/", fs.DirEntryInfo{Name: "local.txt", Size: 5, LocalMutation: true})
	item, kind := dc.LookupNamespace("/", "local.txt")
	if kind != fs.LookupPositive || !item.LocalMutation || item.Size != 5 {
		t.Fatalf("row = %+v kind=%v, want the local provenance preserved", item, kind)
	}
	// A listing observation carries no provenance.
	dc.Upsert("/", fs.DirEntryInfo{Name: "listed.txt", Size: 7})
	if item, _ := dc.LookupNamespace("/", "listed.txt"); item.LocalMutation {
		t.Fatalf("listing row = %+v, want no local provenance", item)
	}
}
