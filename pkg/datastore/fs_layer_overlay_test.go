package datastore

import "testing"

func TestReplayFSLayerOverlayUpsertThenChmodKeepsBody(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("hello"), Mode: 0o644, ChecksumSHA256: "x", SizeBytes: 5},
		{Path: "/repo/a.txt", Op: FSLayerEntryOpChmod, Kind: FSLayerEntryKindFile, Mode: 0o600},
	}
	tree := replayFSLayerOverlay(log)
	n, ok := tree["/repo/a.txt"]
	if !ok {
		t.Fatal("missing overlay node")
	}
	if n.Whiteout || !n.HasBody {
		t.Fatalf("node=%+v, want body kept after chmod", n)
	}
	if string(n.Entry.ContentBlob) != "hello" {
		t.Fatalf("content=%q, chmod must not drop upsert body", n.Entry.ContentBlob)
	}
	if n.Mode&0o777 != 0o600 {
		t.Fatalf("mode=%o, want 0600", n.Mode)
	}
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 1 || drafts[0].Op != FSLayerEntryOpUpsert {
		t.Fatalf("drafts=%+v, want one upsert", drafts)
	}
}

func TestReplayFSLayerOverlayRenameAndWhiteout(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("v"), SizeBytes: 1},
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
	}
	tree := replayFSLayerOverlay(log)
	if n, ok := tree["/repo/a.txt"]; !ok || !n.Whiteout {
		t.Fatalf("src whiteout missing: %+v", tree["/repo/a.txt"])
	}
	if n, ok := tree["/repo/b.txt"]; !ok || !n.HasBody || string(n.Entry.ContentBlob) != "v" {
		t.Fatalf("dest missing body: %+v ok=%v", tree["/repo/b.txt"], ok)
	}
}

func TestReplayFSLayerOverlayMainBackedRenameKeepsRenameOp(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
	}
	tree := replayFSLayerOverlay(log)
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 1 {
		t.Fatalf("drafts=%+v, want single rename", drafts)
	}
	if drafts[0].Op != FSLayerEntryOpRename || drafts[0].Path != "/repo/a.txt" || drafts[0].ContentText != "/repo/b.txt" {
		t.Fatalf("draft=%+v, want rename a→b", drafts[0])
	}
	src := OverlayNodeEntry(tree["/repo/a.txt"])
	if src.Op != FSLayerEntryOpWhiteout {
		t.Fatalf("resolve src op=%s, want whiteout", src.Op)
	}
}

func TestOverlayCommitDraftsWhiteoutDeepestFirst(t *testing.T) {
	tree := map[string]FSLayerOverlayNode{
		"/repo/a/":     {Path: "/repo/a/", Whiteout: true, Kind: FSLayerEntryKindDir, Entry: FSLayerEntry{Path: "/repo/a/", Op: FSLayerEntryOpWhiteout, Kind: FSLayerEntryKindDir}},
		"/repo/a/b.txt": {Path: "/repo/a/b.txt", Whiteout: true, Kind: FSLayerEntryKindFile, Entry: FSLayerEntry{Path: "/repo/a/b.txt", Op: FSLayerEntryOpWhiteout}},
	}
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 2 {
		t.Fatalf("drafts=%d", len(drafts))
	}
	if drafts[0].Path != "/repo/a/b.txt" || drafts[1].Path != "/repo/a/" {
		t.Fatalf("order=%q then %q, want file then dir", drafts[0].Path, drafts[1].Path)
	}
}
