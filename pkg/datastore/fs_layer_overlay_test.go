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

func TestReplayFSLayerOverlayDirectoryRenameMovesDescendants(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/old/", Op: FSLayerEntryOpMkdir, Kind: FSLayerEntryKindDir, Mode: 0o755},
		{Path: "/repo/old/child.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("v"), SizeBytes: 1},
		{Path: "/repo/old/", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindDir, ContentText: "/repo/new/"},
	}
	tree := replayFSLayerOverlay(log)
	if n, ok := tree["/repo/old/"]; !ok || !n.Whiteout {
		t.Fatalf("src dir whiteout missing: %+v", tree["/repo/old/"])
	}
	if _, ok := tree["/repo/old/child.txt"]; ok {
		t.Fatal("stale descendant left under old prefix")
	}
	n, ok := tree["/repo/new/child.txt"]
	if !ok || !n.HasBody || string(n.Entry.ContentBlob) != "v" {
		t.Fatalf("moved child = %+v ok=%v", n, ok)
	}
	drafts := OverlayCommitDrafts(tree)
	var sawChild, sawOldChild bool
	for _, d := range drafts {
		if d.Path == "/repo/new/child.txt" && d.Op == FSLayerEntryOpUpsert {
			sawChild = true
		}
		if d.Path == "/repo/old/child.txt" {
			sawOldChild = true
		}
	}
	if !sawChild || sawOldChild {
		t.Fatalf("drafts=%+v, want upsert at new child and no old child", drafts)
	}
}

func TestReplayFSLayerOverlayMainBackedDirRenameMovesOverlayChild(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/old/extra.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("x"), SizeBytes: 1},
		{Path: "/repo/old/", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindDir, ContentText: "/repo/new/"},
	}
	tree := replayFSLayerOverlay(log)
	if _, ok := tree["/repo/old/extra.txt"]; ok {
		t.Fatal("overlay child should move with directory rename")
	}
	n, ok := tree["/repo/new/extra.txt"]
	if !ok || string(n.Entry.ContentBlob) != "x" {
		t.Fatalf("moved overlay child = %+v ok=%v", n, ok)
	}
	drafts := OverlayCommitDrafts(tree)
	var sawRename bool
	for _, d := range drafts {
		if d.Op == FSLayerEntryOpRename && d.Path == "/repo/old/" && d.ContentText == "/repo/new/" {
			sawRename = true
		}
	}
	if !sawRename {
		t.Fatalf("drafts=%+v, want main-backed dir rename", drafts)
	}
}

func TestReplayFSLayerOverlayPreservesTrailingSpaceRenameTarget(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/trailing "},
	}
	tree := replayFSLayerOverlay(log)
	n, ok := tree["/repo/trailing "]
	if !ok || n.RenameFrom != "/repo/a.txt" {
		t.Fatalf("dest node=%+v ok=%v, want rename to trailing-space path", n, ok)
	}
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 1 || drafts[0].ContentText != "/repo/trailing " {
		t.Fatalf("drafts=%+v, want preserved trailing space", drafts)
	}
}

func TestReplayFSLayerOverlayChmodAfterMainBackedRename(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt", Mode: 0o644},
		{Path: "/repo/b.txt", Op: FSLayerEntryOpChmod, Kind: FSLayerEntryKindFile, Mode: 0o600},
	}
	tree := replayFSLayerOverlay(log)
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 2 {
		t.Fatalf("drafts=%+v, want rename then chmod", drafts)
	}
	if drafts[0].Op != FSLayerEntryOpRename || drafts[0].Path != "/repo/a.txt" || drafts[0].ContentText != "/repo/b.txt" {
		t.Fatalf("first draft=%+v, want rename a→b", drafts[0])
	}
	if drafts[1].Op != FSLayerEntryOpChmod || drafts[1].Path != "/repo/b.txt" || drafts[1].Mode&0o777 != 0o600 {
		t.Fatalf("second draft=%+v, want chmod b 0600", drafts[1])
	}
}

func TestReplayFSLayerOverlayWhiteoutAfterRenameKeepsSourceDelete(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
		{Path: "/repo/b.txt", Op: FSLayerEntryOpWhiteout, Kind: FSLayerEntryKindFile},
	}
	tree := replayFSLayerOverlay(log)
	src := tree["/repo/a.txt"]
	if !src.Whiteout || src.ConsumedByRename {
		t.Fatalf("src=%+v, want real whiteout after dest delete", src)
	}
	drafts := OverlayCommitDrafts(tree)
	var sawSrc, sawDest bool
	for _, d := range drafts {
		if d.Op == FSLayerEntryOpWhiteout && d.Path == "/repo/a.txt" {
			sawSrc = true
		}
		if d.Op == FSLayerEntryOpWhiteout && d.Path == "/repo/b.txt" {
			sawDest = true
		}
	}
	if !sawSrc || !sawDest {
		t.Fatalf("drafts=%+v, want whiteouts for both a and b", drafts)
	}
}

func TestReplayFSLayerOverlayRenameOntoRenameDestUnconsumesPreviousSource(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
		{Path: "/repo/c.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
	}
	tree := replayFSLayerOverlay(log)
	src := tree["/repo/a.txt"]
	if !src.Whiteout || src.ConsumedByRename {
		t.Fatalf("a=%+v, want unconsumed whiteout after later rename onto b", src)
	}
	drafts := OverlayCommitDrafts(tree)
	var sawA, sawRenameC bool
	for _, d := range drafts {
		if d.Op == FSLayerEntryOpWhiteout && d.Path == "/repo/a.txt" {
			sawA = true
		}
		if d.Op == FSLayerEntryOpRename && d.Path == "/repo/c.txt" && d.ContentText == "/repo/b.txt" {
			sawRenameC = true
		}
	}
	if !sawA || !sawRenameC {
		t.Fatalf("drafts=%+v, want delete a and rename c→b", drafts)
	}
}

func TestReplayFSLayerOverlayRecreateSourceAppliesAfterRename(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
		{Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("new"), SizeBytes: 3},
	}
	tree := replayFSLayerOverlay(log)
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 2 {
		t.Fatalf("drafts=%+v, want rename then upsert", drafts)
	}
	if drafts[0].Op != FSLayerEntryOpRename || drafts[0].Path != "/repo/a.txt" || drafts[0].ContentText != "/repo/b.txt" {
		t.Fatalf("first draft=%+v, want rename a→b before recreate", drafts[0])
	}
	if drafts[1].Op != FSLayerEntryOpUpsert || drafts[1].Path != "/repo/a.txt" || string(drafts[1].ContentBlob) != "new" {
		t.Fatalf("second draft=%+v, want upsert new a", drafts[1])
	}
}

func TestReplayFSLayerOverlayRecreateSourceAfterRenameIntoNewDir(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/new/", Op: FSLayerEntryOpMkdir, Kind: FSLayerEntryKindDir, Mode: 0o755},
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/new/a.txt"},
		{Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("new"), SizeBytes: 3},
		{Path: "/repo/n.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("n"), SizeBytes: 1},
	}
	drafts := OverlayCommitDrafts(replayFSLayerOverlay(log))
	idx := map[string]int{}
	for i, d := range drafts {
		switch {
		case d.Op == FSLayerEntryOpMkdir && d.Path == "/repo/new/":
			idx["mkdir"] = i
		case d.Op == FSLayerEntryOpRename && d.ContentText == "/repo/new/a.txt":
			idx["rename"] = i
		case d.Op == FSLayerEntryOpUpsert && d.Path == "/repo/a.txt":
			idx["recreate"] = i
		}
	}
	if len(idx) != 3 {
		t.Fatalf("drafts=%+v, missing mkdir/rename/recreate", drafts)
	}
	if idx["mkdir"] >= idx["rename"] || idx["rename"] >= idx["recreate"] {
		t.Fatalf("order mkdir=%d rename=%d recreate=%d, want mkdir < rename < recreate; drafts=%+v", idx["mkdir"], idx["rename"], idx["recreate"], drafts)
	}
}

func TestReplayFSLayerOverlayMkdirRecreateAfterDirRename(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/old/", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindDir, ContentText: "/repo/new/"},
		{Path: "/repo/old/", Op: FSLayerEntryOpMkdir, Kind: FSLayerEntryKindDir, Mode: 0o755},
	}
	drafts := OverlayCommitDrafts(replayFSLayerOverlay(log))
	if len(drafts) != 2 {
		t.Fatalf("drafts=%+v, want rename then mkdir", drafts)
	}
	if drafts[0].Op != FSLayerEntryOpRename || drafts[0].Path != "/repo/old/" || drafts[0].ContentText != "/repo/new/" {
		t.Fatalf("first=%+v, want dir rename", drafts[0])
	}
	if drafts[1].Op != FSLayerEntryOpMkdir || drafts[1].Path != "/repo/old/" {
		t.Fatalf("second=%+v, want mkdir recreate after rename", drafts[1])
	}
}

func TestReplayFSLayerOverlayRenameChainKeepsIntermediateWhiteout(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
		{Path: "/repo/b.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/c.txt"},
	}
	tree := replayFSLayerOverlay(log)
	mid := tree["/repo/b.txt"]
	if !mid.Whiteout || mid.ConsumedByRename {
		t.Fatalf("b=%+v, want real intermediate whiteout", mid)
	}
	drafts := OverlayCommitDrafts(tree)
	var sawRename, sawMid bool
	for _, d := range drafts {
		if d.Op == FSLayerEntryOpRename && d.Path == "/repo/a.txt" && d.ContentText == "/repo/c.txt" {
			sawRename = true
		}
		if d.Op == FSLayerEntryOpWhiteout && d.Path == "/repo/b.txt" {
			sawMid = true
		}
	}
	if !sawRename || !sawMid {
		t.Fatalf("drafts=%+v, want rename a→c and whiteout b", drafts)
	}
}

func TestReplayFSLayerOverlayUpsertAfterRenameKeepsSourceDelete(t *testing.T) {
	log := []FSLayerEntry{
		{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/b.txt"},
		{Path: "/repo/b.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("new"), SizeBytes: 3},
	}
	tree := replayFSLayerOverlay(log)
	src := tree["/repo/a.txt"]
	if !src.Whiteout || src.ConsumedByRename {
		t.Fatalf("src=%+v, want real whiteout after dest upsert", src)
	}
	drafts := OverlayCommitDrafts(tree)
	var sawUpsert, sawWhiteout bool
	for _, d := range drafts {
		if d.Op == FSLayerEntryOpUpsert && d.Path == "/repo/b.txt" {
			sawUpsert = true
		}
		if d.Op == FSLayerEntryOpWhiteout && d.Path == "/repo/a.txt" {
			sawWhiteout = true
		}
		if d.Op == FSLayerEntryOpRename {
			t.Fatalf("unexpected rename after dest upsert: %+v", d)
		}
	}
	if !sawUpsert || !sawWhiteout {
		t.Fatalf("drafts=%+v, want dest upsert and src whiteout", drafts)
	}
}

func TestOverlayCommitDraftsSortsRenameByDestination(t *testing.T) {
	tree := map[string]FSLayerOverlayNode{
		"/repo/new/": {
			Path:    "/repo/new/",
			HasBody: true,
			Kind:    FSLayerEntryKindDir,
			Entry:   FSLayerEntry{Path: "/repo/new/", Op: FSLayerEntryOpMkdir, Kind: FSLayerEntryKindDir},
		},
		"/repo/new/a.txt": {
			Path:       "/repo/new/a.txt",
			RenameFrom: "/repo/a.txt",
			Kind:       FSLayerEntryKindFile,
			Entry:      FSLayerEntry{Path: "/repo/a.txt", Op: FSLayerEntryOpRename, Kind: FSLayerEntryKindFile, ContentText: "/repo/new/a.txt"},
		},
		"/repo/a.txt": {
			Path:             "/repo/a.txt",
			Whiteout:         true,
			ConsumedByRename: true,
			Kind:             FSLayerEntryKindFile,
			Entry:            FSLayerEntry{Path: "/repo/a.txt", Op: FSLayerEntryOpWhiteout},
		},
	}
	drafts := OverlayCommitDrafts(tree)
	if len(drafts) != 2 {
		t.Fatalf("drafts=%+v, want mkdir then rename", drafts)
	}
	if drafts[0].Op != FSLayerEntryOpMkdir || drafts[0].Path != "/repo/new/" {
		t.Fatalf("first draft=%+v, want mkdir dest parent", drafts[0])
	}
	if drafts[1].Op != FSLayerEntryOpRename || drafts[1].ContentText != "/repo/new/a.txt" {
		t.Fatalf("second draft=%+v, want rename into dest", drafts[1])
	}
}

func TestOverlayCommitDraftsWhiteoutDeepestFirst(t *testing.T) {
	tree := map[string]FSLayerOverlayNode{
		"/repo/a/":      {Path: "/repo/a/", Whiteout: true, Kind: FSLayerEntryKindDir, Entry: FSLayerEntry{Path: "/repo/a/", Op: FSLayerEntryOpWhiteout, Kind: FSLayerEntryKindDir}},
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
