package datastore

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// runFSLayerCoreScenario exercises the fs layer flow (create, tags, ref
// resolution, state transitions, entries, checkpoints, events, rollback)
// against a store. It is run against both schema shapes to prove parity.
func runFSLayerCoreScenario(t *testing.T, store *Store, prefix string) {
	t.Helper()
	ctx := context.Background()

	// Layer round trip with tags and name/tag ref resolution.
	layer := FSLayer{
		LayerID:        prefix + "-layer-1",
		BaseRootPath:   "/work",
		Name:           prefix + " agent task",
		Tags:           map[string]string{"task": "auth", "env": "dev"},
		DurabilityMode: FSLayerDurabilityRestoreSafe,
		ActorID:        "actor-1",
	}
	if err := store.CreateFSLayer(ctx, &layer); err != nil {
		t.Fatalf("CreateFSLayer: %v", err)
	}
	got, err := store.GetFSLayer(ctx, prefix+"-layer-1")
	if err != nil {
		t.Fatalf("GetFSLayer: %v", err)
	}
	if got.BaseRootPath != "/work/" || got.Name != prefix+" agent task" || got.State != FSLayerStateActive || got.ActorID != "actor-1" {
		t.Fatalf("unexpected layer: %+v", got)
	}
	if got.Tags["task"] != "auth" || got.Tags["env"] != "dev" {
		t.Fatalf("unexpected layer tags: %+v", got.Tags)
	}
	for _, ref := range []string{prefix + " agent task", "task=auth", "tag:env"} {
		byRef, err := store.ResolveFSLayerRef(ctx, ref)
		if err != nil {
			t.Fatalf("ResolveFSLayerRef %q: %v", ref, err)
		}
		if byRef.LayerID != prefix+"-layer-1" {
			t.Fatalf("ResolveFSLayerRef %q = %+v, want %s", ref, byRef, prefix+"-layer-1")
		}
	}
	layers, err := store.ListFSLayers(ctx)
	if err != nil {
		t.Fatalf("ListFSLayers: %v", err)
	}
	if len(layers) != 1 || layers[0].LayerID != prefix+"-layer-1" {
		t.Fatalf("layers = %+v, want only %s", layers, prefix+"-layer-1")
	}
	if err := store.SetFSLayerState(ctx, prefix+"-layer-1", FSLayerStateSealed); err != nil {
		t.Fatalf("SetFSLayerState: %v", err)
	}
	got, err = store.GetFSLayer(ctx, prefix+"-layer-1")
	if err != nil {
		t.Fatalf("GetFSLayer sealed: %v", err)
	}
	if got.State != FSLayerStateSealed || got.SealedAt == nil {
		t.Fatalf("sealed layer = %+v, want sealed with sealed_at", got)
	}

	// Entries and checkpoints on a second layer.
	entriesLayer := prefix + "-layer-entries"
	if err := store.CreateFSLayer(ctx, &FSLayer{LayerID: entriesLayer, BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer entries: %v", err)
	}
	if err := store.UpsertFSLayerEntry(ctx, &FSLayerEntry{LayerID: entriesLayer, Path: "/repo/tmp", Op: FSLayerEntryOpMkdir}); err != nil {
		t.Fatalf("UpsertFSLayerEntry dir: %v", err)
	}
	if err := store.UpsertFSLayerEntry(ctx, &FSLayerEntry{LayerID: entriesLayer, Path: "/repo/tmp/link", Op: FSLayerEntryOpSymlink, ContentText: "/repo/tmp/a.txt"}); err != nil {
		t.Fatalf("UpsertFSLayerEntry symlink: %v", err)
	}
	if err := store.UpsertFSLayerEntry(ctx, &FSLayerEntry{LayerID: entriesLayer, Path: "/repo/tmp/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("hello"), ContentType: "text/plain", SizeBytes: 5}); err != nil {
		t.Fatalf("UpsertFSLayerEntry file: %v", err)
	}
	gotDir, err := store.GetFSLayerEntry(ctx, entriesLayer, "/repo/tmp/")
	if err != nil {
		t.Fatalf("GetFSLayerEntry dir: %v", err)
	}
	if gotDir.Path != "/repo/tmp/" || gotDir.Kind != FSLayerEntryKindDir || gotDir.Mode != 0o755 {
		t.Fatalf("dir entry = %+v", gotDir)
	}
	gotFile, err := store.GetFSLayerEntry(ctx, entriesLayer, "/repo/tmp/a.txt")
	if err != nil {
		t.Fatalf("GetFSLayerEntry file: %v", err)
	}
	if gotFile.EntrySeq != 3 || !bytes.Equal(gotFile.ContentBlob, []byte("hello")) || gotFile.ParentPath != "/repo/tmp/" {
		t.Fatalf("file entry = %+v", gotFile)
	}
	entries, err := store.ListFSLayerEntries(ctx, entriesLayer)
	if err != nil {
		t.Fatalf("ListFSLayerEntries: %v", err)
	}
	if len(entries) != 3 || entries[0].Path != "/repo/tmp/" || entries[1].Path != "/repo/tmp/link" || entries[2].Path != "/repo/tmp/a.txt" {
		t.Fatalf("entries = %+v", entries)
	}
	checkpoint := FSLayerCheckpoint{CheckpointID: prefix + "-ckpt-1", LayerID: entriesLayer, Label: "restore point"}
	if err := store.CreateFSLayerCheckpoint(ctx, &checkpoint); err != nil {
		t.Fatalf("CreateFSLayerCheckpoint: %v", err)
	}
	gotCheckpoint, err := store.GetFSLayerCheckpoint(ctx, prefix+"-ckpt-1")
	if err != nil {
		t.Fatalf("GetFSLayerCheckpoint: %v", err)
	}
	if gotCheckpoint.DurableSeq != 3 || gotCheckpoint.Label != "restore point" {
		t.Fatalf("checkpoint = %+v, want durable_seq=3", gotCheckpoint)
	}
	gotLayer, err := store.GetFSLayer(ctx, entriesLayer)
	if err != nil {
		t.Fatalf("GetFSLayer entries: %v", err)
	}
	if gotLayer.DurableSeq != 3 {
		t.Fatalf("layer durable_seq=%d, want 3", gotLayer.DurableSeq)
	}
	if err := store.UpsertFSLayerEntry(ctx, &FSLayerEntry{LayerID: entriesLayer, Path: "/repo/tmp/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("goodbye"), ContentType: "text/plain", SizeBytes: 7}); err != nil {
		t.Fatalf("UpsertFSLayerEntry file v2: %v", err)
	}
	current, err := store.GetFSLayerEntry(ctx, entriesLayer, "/repo/tmp/a.txt")
	if err != nil {
		t.Fatalf("GetFSLayerEntry current: %v", err)
	}
	if current.EntrySeq != 4 || !bytes.Equal(current.ContentBlob, []byte("goodbye")) {
		t.Fatalf("current entry = %+v", current)
	}
	logEntries, err := store.ListFSLayerEntryLog(ctx, entriesLayer)
	if err != nil {
		t.Fatalf("ListFSLayerEntryLog: %v", err)
	}
	if len(logEntries) != 4 || logEntries[0].EntrySeq != 1 || logEntries[3].EntrySeq != 4 {
		t.Fatalf("log entries = %+v, want full ordered log", logEntries)
	}
	atCheckpoint, err := store.GetFSLayerEntryAtSeq(ctx, entriesLayer, "/repo/tmp/a.txt", gotCheckpoint.DurableSeq)
	if err != nil {
		t.Fatalf("GetFSLayerEntryAtSeq: %v", err)
	}
	if atCheckpoint.EntrySeq != 3 || !bytes.Equal(atCheckpoint.ContentBlob, []byte("hello")) {
		t.Fatalf("checkpoint entry = %+v, want seq=3 hello", atCheckpoint)
	}
	checkpointEntries, err := store.ListFSLayerEntriesAtSeq(ctx, entriesLayer, gotCheckpoint.DurableSeq)
	if err != nil {
		t.Fatalf("ListFSLayerEntriesAtSeq: %v", err)
	}
	if len(checkpointEntries) != 3 || checkpointEntries[2].EntrySeq != 3 || !bytes.Equal(checkpointEntries[2].ContentBlob, []byte("hello")) {
		t.Fatalf("checkpoint entries = %+v", checkpointEntries)
	}
	checkpointLog, err := store.ListFSLayerEntryLogAtSeq(ctx, entriesLayer, gotCheckpoint.DurableSeq)
	if err != nil {
		t.Fatalf("ListFSLayerEntryLogAtSeq: %v", err)
	}
	if len(checkpointLog) != 3 || checkpointLog[0].EntrySeq != 1 || checkpointLog[2].EntrySeq != 3 {
		t.Fatalf("checkpoint log = %+v, want ordered entries through checkpoint", checkpointLog)
	}

	// Rollback emits a synthetic event once; a repeated rollback is a no-op.
	rollbackLayer := prefix + "-layer-rb"
	if err := store.CreateFSLayer(ctx, &FSLayer{LayerID: rollbackLayer, BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer rollback: %v", err)
	}
	if err := store.UpsertFSLayerEntry(ctx, &FSLayerEntry{LayerID: rollbackLayer, Path: "/repo/a.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("data")}); err != nil {
		t.Fatalf("UpsertFSLayerEntry rollback layer: %v", err)
	}
	priorEvents, err := store.ListFSLayerEvents(ctx, rollbackLayer, 0, 1000)
	if err != nil {
		t.Fatalf("ListFSLayerEvents before rollback: %v", err)
	}
	if len(priorEvents) != 1 || priorEvents[0].Op != "upsert" {
		t.Fatalf("prior events = %+v, want 1 upsert event", priorEvents)
	}
	if err := store.RollbackFSLayer(ctx, rollbackLayer); err != nil {
		t.Fatalf("RollbackFSLayer: %v", err)
	}
	events, err := store.ListFSLayerEvents(ctx, rollbackLayer, 0, 1000)
	if err != nil {
		t.Fatalf("ListFSLayerEvents after rollback: %v", err)
	}
	if len(events) != 2 || events[1].Op != "rollback" || events[1].Seq <= priorEvents[0].Seq {
		t.Fatalf("events after rollback = %+v, want upsert then rollback with increasing seq", events)
	}
	if err := store.RollbackFSLayer(ctx, rollbackLayer); err != nil {
		t.Fatalf("RollbackFSLayer idempotent: %v", err)
	}
	events2, err := store.ListFSLayerEvents(ctx, rollbackLayer, 0, 1000)
	if err != nil {
		t.Fatalf("ListFSLayerEvents after idempotent rollback: %v", err)
	}
	if len(events2) != len(events) {
		t.Fatalf("events after idempotent rollback = %d, want %d (no re-emit)", len(events2), len(events))
	}
	rolledBack, err := store.GetFSLayer(ctx, rollbackLayer)
	if err != nil {
		t.Fatalf("GetFSLayer after rollback: %v", err)
	}
	if rolledBack.State != FSLayerStateAbandoned || rolledBack.SealedAt == nil {
		t.Fatalf("rollback layer = %+v, want abandoned with sealed_at", rolledBack)
	}

	// State transition conflicts.
	commitLayer := prefix + "-layer-commit"
	if err := store.CreateFSLayer(ctx, &FSLayer{LayerID: commitLayer, BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("CreateFSLayer commit: %v", err)
	}
	if err := store.BeginFSLayerCommit(ctx, commitLayer); err != nil {
		t.Fatalf("BeginFSLayerCommit: %v", err)
	}
	if err := store.BeginFSLayerCommit(ctx, commitLayer); !errors.Is(err, ErrFSLayerStateConflict) {
		t.Fatalf("BeginFSLayerCommit reentry err=%v, want ErrFSLayerStateConflict", err)
	}
	if err := store.UpsertFSLayerEntry(ctx, &FSLayerEntry{LayerID: commitLayer, Path: "/repo/late.txt", Op: FSLayerEntryOpUpsert, Kind: FSLayerEntryKindFile, ContentBlob: []byte("late")}); !errors.Is(err, ErrFSLayerStateConflict) {
		t.Fatalf("UpsertFSLayerEntry committing err=%v, want ErrFSLayerStateConflict", err)
	}
	if err := store.SetFSLayerStateIf(ctx, commitLayer, []FSLayerState{FSLayerStateActive}, FSLayerStateSealed); !errors.Is(err, ErrFSLayerStateConflict) {
		t.Fatalf("SetFSLayerStateIf err=%v, want ErrFSLayerStateConflict", err)
	}
}

// TestFSLayerSharedShapeParity runs the same scenario used by the standalone
// fs layer tests against the shared (fs_id) schema shape.
func TestFSLayerSharedShapeParity(t *testing.T) {
	installSharedFSLayerSchema(t)
	store := newSharedStore(t, 4301001)
	runFSLayerCoreScenario(t, store, "shr")
}

// TestFSLayerSharedShapeCrossTenantIsolation proves fs layer rows of one
// fs_id are invisible to another fs_id, and that the same layer_id (plus
// entry paths, event seqs, and checkpoint ids) can coexist under both fs_ids.
func TestFSLayerSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedFSLayerSchema(t)
	ctx := context.Background()
	storeA := newSharedStore(t, 4301002)
	storeB := newSharedStore(t, 4301003)

	// Same layer_id under both fs_ids must coexist.
	if err := storeA.CreateFSLayer(ctx, &FSLayer{
		LayerID:      "layer-iso",
		BaseRootPath: "/work",
		Name:         "alpha",
		Tags:         map[string]string{"shared": "yes", "env": "a"},
	}); err != nil {
		t.Fatalf("CreateFSLayer A: %v", err)
	}
	if err := storeB.CreateFSLayer(ctx, &FSLayer{
		LayerID:      "layer-iso",
		BaseRootPath: "/work",
		Name:         "beta",
		Tags:         map[string]string{"shared": "yes", "env": "b"},
	}); err != nil {
		t.Fatalf("CreateFSLayer B: %v", err)
	}
	gotA, err := storeA.GetFSLayer(ctx, "layer-iso")
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := storeB.GetFSLayer(ctx, "layer-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotA.Name != "alpha" || gotB.Name != "beta" || gotA.Tags["env"] != "a" || gotB.Tags["env"] != "b" {
		t.Fatalf("cross-tenant layers mixed up: A=%+v B=%+v", gotA, gotB)
	}
	layersA, err := storeA.ListFSLayers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	layersB, err := storeB.ListFSLayers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(layersA) != 1 || layersA[0].Name != "alpha" || len(layersB) != 1 || layersB[0].Name != "beta" {
		t.Fatalf("list leaks across fs_id: A=%+v B=%+v", layersA, layersB)
	}

	// Name and tag refs resolve within the fs_id only. The shared=yes tag
	// exists on BOTH layers: resolving it must return exactly the caller's
	// own layer (not an ambiguous match across the join).
	for _, tc := range []struct {
		store   *Store
		name    string
		wantRef string
		wantErr bool
	}{
		{storeA, "A by name", "alpha", false},
		{storeB, "B by foreign name", "alpha", true},
		{storeA, "A by own tag", "env=a", false},
		{storeA, "A by foreign tag", "env=b", true},
		{storeA, "A by colliding tag", "shared=yes", false},
		{storeB, "B by colliding tag", "shared=yes", false},
	} {
		got, err := tc.store.ResolveFSLayerRef(ctx, tc.wantRef)
		if tc.wantErr {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s: ResolveFSLayerRef %q err=%v, want ErrNotFound", tc.name, tc.wantRef, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: ResolveFSLayerRef %q: %v", tc.name, tc.wantRef, err)
		}
		if got.LayerID != "layer-iso" {
			t.Fatalf("%s: ResolveFSLayerRef %q = %+v, want layer-iso", tc.name, tc.wantRef, got)
		}
	}

	// Entries and events never cross fs_id.
	if err := storeA.UpsertFSLayerEntry(ctx, &FSLayerEntry{
		LayerID: "layer-iso", Path: "/work/a.txt", Op: FSLayerEntryOpUpsert,
		Kind: FSLayerEntryKindFile, ContentBlob: []byte("a-data"),
	}); err != nil {
		t.Fatalf("UpsertFSLayerEntry A: %v", err)
	}
	entriesA, err := storeA.ListFSLayerEntries(ctx, "layer-iso")
	if err != nil {
		t.Fatal(err)
	}
	if len(entriesA) != 1 || entriesA[0].Path != "/work/a.txt" {
		t.Fatalf("A entries = %+v", entriesA)
	}
	entriesB, err := storeB.ListFSLayerEntries(ctx, "layer-iso")
	if err != nil {
		t.Fatal(err)
	}
	if len(entriesB) != 0 {
		t.Fatalf("B sees %d entries, want 0 (cross-tenant leak)", len(entriesB))
	}
	if _, err := storeB.GetFSLayerEntry(ctx, "layer-iso", "/work/a.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B GetFSLayerEntry err=%v, want ErrNotFound", err)
	}
	eventsA, err := storeA.ListFSLayerEvents(ctx, "layer-iso", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	eventsB, err := storeB.ListFSLayerEvents(ctx, "layer-iso", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsA) != 1 || len(eventsB) != 0 {
		t.Fatalf("event leak: A=%+v B=%+v", eventsA, eventsB)
	}

	// Same checkpoint_id under both fs_ids coexists with independent seqs.
	if err := storeA.CreateFSLayerCheckpoint(ctx, &FSLayerCheckpoint{CheckpointID: "ckpt-iso", LayerID: "layer-iso"}); err != nil {
		t.Fatalf("CreateFSLayerCheckpoint A: %v", err)
	}
	if err := storeB.CreateFSLayerCheckpoint(ctx, &FSLayerCheckpoint{CheckpointID: "ckpt-iso", LayerID: "layer-iso"}); err != nil {
		t.Fatalf("CreateFSLayerCheckpoint B: %v", err)
	}
	ckptA, err := storeA.GetFSLayerCheckpoint(ctx, "ckpt-iso")
	if err != nil {
		t.Fatal(err)
	}
	ckptB, err := storeB.GetFSLayerCheckpoint(ctx, "ckpt-iso")
	if err != nil {
		t.Fatal(err)
	}
	if ckptA.DurableSeq != 1 || ckptB.DurableSeq != 0 {
		t.Fatalf("checkpoints mixed up: A=%+v B=%+v", ckptA, ckptB)
	}

	// State changes and rollback stay inside the fs_id.
	if err := storeB.SetFSLayerState(ctx, "layer-iso", FSLayerStateSealed); err != nil {
		t.Fatalf("SetFSLayerState B: %v", err)
	}
	if err := storeB.RollbackFSLayer(ctx, "layer-iso"); err != nil {
		t.Fatalf("RollbackFSLayer B: %v", err)
	}
	gotB, err = storeB.GetFSLayer(ctx, "layer-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotB.State != FSLayerStateAbandoned {
		t.Fatalf("B layer state=%q, want abandoned", gotB.State)
	}
	gotA, err = storeA.GetFSLayer(ctx, "layer-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotA.State != FSLayerStateActive {
		t.Fatalf("A layer state=%q, want active (untouched by B)", gotA.State)
	}
	eventsA, err = storeA.ListFSLayerEvents(ctx, "layer-iso", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsA) != 1 || eventsA[0].Op != "upsert" {
		t.Fatalf("A events changed by B's rollback: %+v", eventsA)
	}
	eventsB, err = storeB.ListFSLayerEvents(ctx, "layer-iso", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsB) != 1 || eventsB[0].Op != "rollback" {
		t.Fatalf("B events = %+v, want one rollback event", eventsB)
	}
}

// TestFSLayerSharedShapeStoresFsID asserts every fs layer table row written
// by the core scenario carries the scope's fs_id as its row key.
func TestFSLayerSharedShapeStoresFsID(t *testing.T) {
	installSharedFSLayerSchema(t)
	const fsID int64 = 4301004
	store := newSharedStore(t, fsID)
	runFSLayerCoreScenario(t, store, "fsid")

	for _, tbl := range []string{"fs_layers", "fs_layer_tags", "fs_layer_entries", "fs_layer_events", "fs_layer_checkpoints"} {
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
