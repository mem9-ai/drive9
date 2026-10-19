package core

// qiffang twenty-first round: the namespace-cache stat shortcut serves
// listing-observed POSITIVE rows only on mounts that trust process-local
// events; negative rows stay short-TTL hints everywhere.

import (
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

type stubNamespace struct {
	item fs.DirEntryInfo
	kind fs.LookupKind
}

func (s stubNamespace) LookupNamespace(parent, name string) (fs.DirEntryInfo, fs.LookupKind) {
	return s.item, s.kind
}

func TestPortTrustLocalEventsGatesPositiveNamespaceRows(t *testing.T) {
	row := fs.DirEntryInfo{Size: 7, Rev: 3, Mtime: time.Unix(100, 0)}
	for _, tc := range []struct {
		name  string
		trust bool
		want  bool
	}{
		{name: "default revalidates", trust: false, want: false},
		{name: "trusted serves", trust: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Core{trustLocalEvents: tc.trust}
			c.Attach(stubNamespace{item: row, kind: fs.LookupPositive})
			attr, ok := c.statFromNamespaceCache("/d/a.txt")
			if ok != tc.want {
				t.Fatalf("served = %v, want %v", ok, tc.want)
			}
			if ok && (attr.Size != 7 || attr.Rev != 3) {
				t.Fatalf("attr = %+v, want the cached row", attr)
			}
		})
	}

	// A row THIS mount wrote is its own state and serves without the
	// trust flag: revalidating it would lose read-your-writes while the
	// commit is still in flight.
	authored := &Core{}
	authored.Attach(stubNamespace{
		item: fs.DirEntryInfo{Size: 9, Rev: 4, LocalMutation: true},
		kind: fs.LookupPositive,
	})
	if attr, ok := authored.statFromNamespaceCache("/d/b.txt"); !ok || attr.Size != 9 {
		t.Fatalf("locally authored row = %+v ok=%v, want the local size", attr, ok)
	}

	// Negative rows are short-TTL hints on every mount.
	c := &Core{}
	c.Attach(stubNamespace{kind: fs.LookupNegative})
	if _, ok := c.statFromNamespaceCache("/d/missing"); !ok {
		t.Fatal("negative row not served without trust")
	}
}
