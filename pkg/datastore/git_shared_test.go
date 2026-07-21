package datastore

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// runGitWorkspaceCoreScenario exercises the git workspace flow (workspace
// upsert/ODKU, lookup by id and root, tree nodes, git state, object packs,
// overlay entries, soft delete) against a store. It is run against both
// schema shapes to prove behavioral parity.
func runGitWorkspaceCoreScenario(t *testing.T, store *Store, prefix string) {
	t.Helper()
	ctx := context.Background()
	sha1 := strings.Repeat("1", 40)
	sha2 := strings.Repeat("2", 40)
	wsID := prefix + "-ws"
	root := "/" + prefix + "-repo/"

	ws := GitWorkspace{
		WorkspaceID: wsID,
		RootPath:    root,
		RepoURL:     "https://github.com/example/repo.git",
		BranchName:  "main",
		BaseCommit:  sha1,
		HeadCommit:  sha1,
		Mode:        GitWorkspaceModeFast,
		Kind:        GitWorkspaceKindMain,
	}
	if err := store.UpsertGitWorkspace(ctx, ws); err != nil {
		t.Fatalf("UpsertGitWorkspace: %v", err)
	}
	got, err := store.GetGitWorkspace(ctx, wsID)
	if err != nil {
		t.Fatalf("GetGitWorkspace: %v", err)
	}
	if got.RootPath != root || got.BranchName != "main" || got.HeadCommit != sha1 ||
		got.Mode != GitWorkspaceModeFast || got.Kind != GitWorkspaceKindMain || got.Status != GitWorkspaceStatusLive {
		t.Fatalf("unexpected workspace: %+v", got)
	}
	// Upsert again to exercise the ON DUPLICATE KEY UPDATE branch.
	ws.HeadCommit = sha2
	ws.BranchName = "feature"
	if err := store.UpsertGitWorkspace(ctx, ws); err != nil {
		t.Fatalf("UpsertGitWorkspace update: %v", err)
	}
	got, err = store.GetGitWorkspace(ctx, wsID)
	if err != nil {
		t.Fatalf("GetGitWorkspace after update: %v", err)
	}
	if got.HeadCommit != sha2 || got.BranchName != "feature" {
		t.Fatalf("workspace after ODKU = %+v", got)
	}
	byRoot, err := store.GetGitWorkspaceByRoot(ctx, "/"+prefix+"-repo")
	if err != nil {
		t.Fatalf("GetGitWorkspaceByRoot: %v", err)
	}
	if byRoot.WorkspaceID != wsID {
		t.Fatalf("GetGitWorkspaceByRoot = %+v, want %s", byRoot, wsID)
	}
	list, err := store.ListGitWorkspaces(ctx)
	if err != nil {
		t.Fatalf("ListGitWorkspaces: %v", err)
	}
	if len(list) != 1 || list[0].WorkspaceID != wsID {
		t.Fatalf("workspaces = %+v, want only %s", list, wsID)
	}

	// Tree nodes are replaced per (workspace, commit).
	nodes := []GitTreeNode{
		{Path: "a.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha1, SizeBytes: 3},
		{Path: "dir/b.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha2, SizeBytes: 5},
	}
	if err := store.ReplaceGitTreeNodes(ctx, wsID, sha1, nodes); err != nil {
		t.Fatalf("ReplaceGitTreeNodes: %v", err)
	}
	tree, err := store.ListGitTreeNodes(ctx, wsID, sha1)
	if err != nil {
		t.Fatalf("ListGitTreeNodes: %v", err)
	}
	if len(tree) != 2 || tree[0].Path != "a.txt" || tree[1].Path != "dir/b.txt" {
		t.Fatalf("tree = %+v", tree)
	}
	if tree[1].ParentPath != "dir" || tree[1].Name != "b.txt" || tree[1].WorkspaceID != wsID || tree[1].CommitSHA != sha1 {
		t.Fatalf("tree node = %+v", tree[1])
	}
	if err := store.ReplaceGitTreeNodes(ctx, wsID, sha1, []GitTreeNode{
		{Path: "c.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha1, SizeBytes: 1},
	}); err != nil {
		t.Fatalf("ReplaceGitTreeNodes second: %v", err)
	}
	tree, err = store.ListGitTreeNodes(ctx, wsID, sha1)
	if err != nil {
		t.Fatalf("ListGitTreeNodes second: %v", err)
	}
	if len(tree) != 1 || tree[0].Path != "c.txt" {
		t.Fatalf("tree after replace = %+v, want only c.txt", tree)
	}

	// Git state round trip plus ODKU update.
	stateBlob := []byte("state-blob")
	if err := store.UpsertGitState(ctx, GitState{
		WorkspaceID: wsID, CheckpointCommit: sha1, StorageType: "db9",
		StorageRef: prefix + "-state-1", ChecksumSHA256: "sum1",
		SizeBytes: int64(len(stateBlob)), ContentBlob: stateBlob,
	}); err != nil {
		t.Fatalf("UpsertGitState: %v", err)
	}
	state, err := store.GetGitState(ctx, wsID)
	if err != nil {
		t.Fatalf("GetGitState: %v", err)
	}
	if state.StorageRef != prefix+"-state-1" || !bytes.Equal(state.ContentBlob, stateBlob) {
		t.Fatalf("git state = %+v", state)
	}
	if err := store.UpsertGitState(ctx, GitState{WorkspaceID: wsID, CheckpointCommit: sha2, StorageType: "db9", StorageRef: prefix + "-state-2"}); err != nil {
		t.Fatalf("UpsertGitState update: %v", err)
	}
	state, err = store.GetGitState(ctx, wsID)
	if err != nil {
		t.Fatalf("GetGitState after update: %v", err)
	}
	if state.StorageRef != prefix+"-state-2" || state.CheckpointCommit != sha2 {
		t.Fatalf("git state after ODKU = %+v", state)
	}

	// Object pack round trip; listing returns metadata only.
	packBlob := []byte("PACK test")
	if err := store.UpsertGitObjectPack(ctx, GitObjectPack{
		WorkspaceID: wsID, PackID: prefix + "-pack-1", ChecksumSHA256: "psum",
		SizeBytes: int64(len(packBlob)), ContentBlob: packBlob,
	}); err != nil {
		t.Fatalf("UpsertGitObjectPack: %v", err)
	}
	pack, err := store.GetGitObjectPack(ctx, wsID, prefix+"-pack-1")
	if err != nil {
		t.Fatalf("GetGitObjectPack: %v", err)
	}
	if pack.ChecksumSHA256 != "psum" || pack.SizeBytes != int64(len(packBlob)) || !bytes.Equal(pack.ContentBlob, packBlob) {
		t.Fatalf("pack = %+v", pack)
	}
	packs, err := store.ListGitObjectPacks(ctx, wsID)
	if err != nil {
		t.Fatalf("ListGitObjectPacks: %v", err)
	}
	if len(packs) != 1 || packs[0].PackID != prefix+"-pack-1" || len(packs[0].ContentBlob) != 0 {
		t.Fatalf("listed packs = %+v, want metadata only", packs)
	}

	// Overlay entries round trip plus ODKU update.
	overlayBlob := []byte("dirty")
	if err := store.UpsertGitOverlayEntry(ctx, GitOverlayEntry{
		WorkspaceID: wsID, Path: "dirty.txt", Op: GitOverlayOpUpsert, Kind: GitOverlayKindFile,
		Mode: "100644", StorageType: "db9", StorageRef: prefix + "-ov-1",
		SizeBytes: int64(len(overlayBlob)), ContentBlob: overlayBlob,
	}); err != nil {
		t.Fatalf("UpsertGitOverlayEntry: %v", err)
	}
	entry, err := store.GetGitOverlayEntry(ctx, wsID, "dirty.txt")
	if err != nil {
		t.Fatalf("GetGitOverlayEntry: %v", err)
	}
	if entry.StorageRef != prefix+"-ov-1" || entry.Op != GitOverlayOpUpsert || !bytes.Equal(entry.ContentBlob, overlayBlob) {
		t.Fatalf("overlay entry = %+v", entry)
	}
	if err := store.UpsertGitOverlayEntry(ctx, GitOverlayEntry{
		WorkspaceID: wsID, Path: "dirty.txt", Op: GitOverlayOpChmod, Kind: GitOverlayKindFile,
		Mode: "100755", StorageType: "db9", StorageRef: prefix + "-ov-2",
	}); err != nil {
		t.Fatalf("UpsertGitOverlayEntry update: %v", err)
	}
	entry, err = store.GetGitOverlayEntry(ctx, wsID, "dirty.txt")
	if err != nil {
		t.Fatalf("GetGitOverlayEntry after update: %v", err)
	}
	if entry.StorageRef != prefix+"-ov-2" || entry.Op != GitOverlayOpChmod || entry.Mode != "100755" {
		t.Fatalf("overlay entry after ODKU = %+v", entry)
	}
	entries, err := store.ListGitOverlayEntries(ctx, wsID)
	if err != nil {
		t.Fatalf("ListGitOverlayEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "dirty.txt" {
		t.Fatalf("overlay entries = %+v", entries)
	}

	// Soft delete hides the workspace from listings while the row stays
	// readable with deleted status.
	if err := store.DeleteGitWorkspace(ctx, wsID); err != nil {
		t.Fatalf("DeleteGitWorkspace: %v", err)
	}
	list, err = store.ListGitWorkspaces(ctx)
	if err != nil {
		t.Fatalf("ListGitWorkspaces after delete: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("workspaces after delete = %+v, want empty", list)
	}
	got, err = store.GetGitWorkspace(ctx, wsID)
	if err != nil {
		t.Fatalf("GetGitWorkspace after delete: %v", err)
	}
	if got.Status != GitWorkspaceStatusDeleted {
		t.Fatalf("workspace status=%q, want %q", got.Status, GitWorkspaceStatusDeleted)
	}
}

// TestGitSharedShapeParity runs the same scenario used by the standalone git
// workspace tests against the shared (fs_id) schema shape.
func TestGitSharedShapeParity(t *testing.T) {
	installSharedGitSchema(t)
	store := newSharedStore(t, 4302001)
	runGitWorkspaceCoreScenario(t, store, "shr")
}

// TestGitSharedShapeCrossTenantIsolation proves git workspace rows of one
// fs_id are invisible to another fs_id, and that the same workspace_id,
// root_path, commit sha, pack_id, and overlay path can coexist under both
// fs_ids.
func TestGitSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedGitSchema(t)
	ctx := context.Background()
	storeA := newSharedStore(t, 4302002)
	storeB := newSharedStore(t, 4302003)
	sha1 := strings.Repeat("1", 40)
	sha2 := strings.Repeat("2", 40)

	// Same workspace_id and root_path under both fs_ids must coexist.
	for _, tc := range []struct {
		store  *Store
		branch string
		head   string
	}{
		{storeA, "main-a", sha1},
		{storeB, "main-b", sha2},
	} {
		if err := tc.store.UpsertGitWorkspace(ctx, GitWorkspace{
			WorkspaceID: "ws-iso",
			RootPath:    "/repo-iso/",
			RepoURL:     "https://github.com/example/repo.git",
			BranchName:  tc.branch,
			BaseCommit:  sha1,
			HeadCommit:  tc.head,
		}); err != nil {
			t.Fatalf("UpsertGitWorkspace %s: %v", tc.branch, err)
		}
	}
	gotA, err := storeA.GetGitWorkspace(ctx, "ws-iso")
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := storeB.GetGitWorkspace(ctx, "ws-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotA.BranchName != "main-a" || gotB.BranchName != "main-b" {
		t.Fatalf("cross-tenant workspaces mixed up: A=%+v B=%+v", gotA, gotB)
	}
	byRoot, err := storeA.GetGitWorkspaceByRoot(ctx, "/repo-iso")
	if err != nil {
		t.Fatal(err)
	}
	if byRoot.BranchName != "main-a" {
		t.Fatalf("GetGitWorkspaceByRoot A = %+v, want branch main-a", byRoot)
	}
	listA, err := storeA.ListGitWorkspaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	listB, err := storeB.ListGitWorkspaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listA) != 1 || listA[0].BranchName != "main-a" || len(listB) != 1 || listB[0].BranchName != "main-b" {
		t.Fatalf("list leaks across fs_id: A=%+v B=%+v", listA, listB)
	}

	// Tree nodes with the same (workspace, commit, path) key coexist per
	// fs_id and replace independently.
	if err := storeA.ReplaceGitTreeNodes(ctx, "ws-iso", sha1, []GitTreeNode{
		{Path: "a-only.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha1},
		{Path: "shared.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha1},
	}); err != nil {
		t.Fatalf("ReplaceGitTreeNodes A: %v", err)
	}
	if err := storeB.ReplaceGitTreeNodes(ctx, "ws-iso", sha1, []GitTreeNode{
		{Path: "b-only.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha2},
		{Path: "shared.txt", Kind: GitTreeNodeKindFile, Mode: "100644", ObjectSHA: sha2},
	}); err != nil {
		t.Fatalf("ReplaceGitTreeNodes B: %v", err)
	}
	treeA, err := storeA.ListGitTreeNodes(ctx, "ws-iso", sha1)
	if err != nil {
		t.Fatal(err)
	}
	if len(treeA) != 2 || treeA[0].Path != "a-only.txt" || treeA[1].Path != "shared.txt" || treeA[1].ObjectSHA != sha1 {
		t.Fatalf("A tree = %+v", treeA)
	}
	treeB, err := storeB.ListGitTreeNodes(ctx, "ws-iso", sha1)
	if err != nil {
		t.Fatal(err)
	}
	if len(treeB) != 2 || treeB[0].Path != "b-only.txt" || treeB[1].Path != "shared.txt" || treeB[1].ObjectSHA != sha2 {
		t.Fatalf("B tree = %+v", treeB)
	}

	// Git state upserts stay inside the fs_id.
	if err := storeA.UpsertGitState(ctx, GitState{WorkspaceID: "ws-iso", CheckpointCommit: sha1, StorageRef: "state-ref-a"}); err != nil {
		t.Fatalf("UpsertGitState A: %v", err)
	}
	if err := storeB.UpsertGitState(ctx, GitState{WorkspaceID: "ws-iso", CheckpointCommit: sha2, StorageRef: "state-ref-b"}); err != nil {
		t.Fatalf("UpsertGitState B: %v", err)
	}
	if err := storeA.UpsertGitState(ctx, GitState{WorkspaceID: "ws-iso", CheckpointCommit: sha2, StorageRef: "state-ref-a2"}); err != nil {
		t.Fatalf("UpsertGitState A update: %v", err)
	}
	stateA, err := storeA.GetGitState(ctx, "ws-iso")
	if err != nil {
		t.Fatal(err)
	}
	stateB, err := storeB.GetGitState(ctx, "ws-iso")
	if err != nil {
		t.Fatal(err)
	}
	if stateA.StorageRef != "state-ref-a2" || stateB.StorageRef != "state-ref-b" {
		t.Fatalf("git state mixed up: A=%+v B=%+v", stateA, stateB)
	}

	// Same pack_id under both fs_ids coexists.
	if err := storeA.UpsertGitObjectPack(ctx, GitObjectPack{WorkspaceID: "ws-iso", PackID: "pack-iso", ChecksumSHA256: "sum-a", ContentBlob: []byte("PACK-A")}); err != nil {
		t.Fatalf("UpsertGitObjectPack A: %v", err)
	}
	if err := storeB.UpsertGitObjectPack(ctx, GitObjectPack{WorkspaceID: "ws-iso", PackID: "pack-iso", ChecksumSHA256: "sum-b", ContentBlob: []byte("PACK-B")}); err != nil {
		t.Fatalf("UpsertGitObjectPack B: %v", err)
	}
	packA, err := storeA.GetGitObjectPack(ctx, "ws-iso", "pack-iso")
	if err != nil {
		t.Fatal(err)
	}
	packB, err := storeB.GetGitObjectPack(ctx, "ws-iso", "pack-iso")
	if err != nil {
		t.Fatal(err)
	}
	if packA.ChecksumSHA256 != "sum-a" || !bytes.Equal(packA.ContentBlob, []byte("PACK-A")) ||
		packB.ChecksumSHA256 != "sum-b" || !bytes.Equal(packB.ContentBlob, []byte("PACK-B")) {
		t.Fatalf("packs mixed up: A=%+v B=%+v", packA, packB)
	}
	packsB, err := storeB.ListGitObjectPacks(ctx, "ws-iso")
	if err != nil {
		t.Fatal(err)
	}
	if len(packsB) != 1 || packsB[0].ChecksumSHA256 != "sum-b" {
		t.Fatalf("B packs = %+v, want only sum-b", packsB)
	}

	// Same overlay path under both fs_ids coexists.
	if err := storeA.UpsertGitOverlayEntry(ctx, GitOverlayEntry{WorkspaceID: "ws-iso", Path: "dirty.txt", StorageRef: "ov-a"}); err != nil {
		t.Fatalf("UpsertGitOverlayEntry A: %v", err)
	}
	if err := storeB.UpsertGitOverlayEntry(ctx, GitOverlayEntry{WorkspaceID: "ws-iso", Path: "dirty.txt", StorageRef: "ov-b"}); err != nil {
		t.Fatalf("UpsertGitOverlayEntry B: %v", err)
	}
	entryA, err := storeA.GetGitOverlayEntry(ctx, "ws-iso", "dirty.txt")
	if err != nil {
		t.Fatal(err)
	}
	entryB, err := storeB.GetGitOverlayEntry(ctx, "ws-iso", "dirty.txt")
	if err != nil {
		t.Fatal(err)
	}
	if entryA.StorageRef != "ov-a" || entryB.StorageRef != "ov-b" {
		t.Fatalf("overlay entries mixed up: A=%+v B=%+v", entryA, entryB)
	}

	// Soft delete never crosses fs_id.
	if err := storeA.DeleteGitWorkspace(ctx, "ws-iso"); err != nil {
		t.Fatalf("DeleteGitWorkspace A: %v", err)
	}
	listA, err = storeA.ListGitWorkspaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listA) != 0 {
		t.Fatalf("A workspaces after delete = %+v, want empty", listA)
	}
	gotB, err = storeB.GetGitWorkspace(ctx, "ws-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotB.Status != GitWorkspaceStatusLive || gotB.BranchName != "main-b" {
		t.Fatalf("B workspace changed by A's delete: %+v", gotB)
	}
	listB, err = storeB.ListGitWorkspaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listB) != 1 {
		t.Fatalf("B workspaces = %+v, want 1", listB)
	}
}

// TestGitSharedShapeStoresFsID asserts every git workspace table row written
// by the core scenario carries the scope's fs_id as its row key.
func TestGitSharedShapeStoresFsID(t *testing.T) {
	installSharedGitSchema(t)
	const fsID int64 = 4302004
	store := newSharedStore(t, fsID)
	runGitWorkspaceCoreScenario(t, store, "fsid")

	for _, tbl := range []string{"git_workspaces", "git_workspace_tree_nodes", "git_workspace_git_state", "git_workspace_object_packs", "git_workspace_overlay"} {
		var got int64
		err := store.DB().QueryRow("SELECT COUNT(*) FROM "+tbl+" WHERE fs_id != ?", fsID).Scan(&got)
		if err != nil {
			t.Fatalf("count %s rows with foreign fs_id: %v", tbl, err)
		}
		if got != 0 {
			t.Fatalf("%s has %d rows with fs_id != %d", tbl, got, fsID)
		}
		var total int64
		if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&total); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if total == 0 {
			t.Fatalf("%s is empty; scenario should have written rows", tbl)
		}
	}
}
