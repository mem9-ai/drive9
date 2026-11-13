package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

// A layer entry that rewrites an existing path's mode is a chmod, so the owner-only rule
// has to cover it: an fs_scoped token with write access to a prefix must not be able to
// change permission bits there, either at entry write time or at commit time.
func TestFSLayerRejectsModeChangeFromScopedToken(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()

	if _, err := s.fallback.WriteCtx(ctx, "/zone/f.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fallback.WriteCtx(ctx, "/elsewhere/g.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
	}

	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-mode", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-mode")
	if err != nil {
		t.Fatal(err)
	}

	// Writing the entry itself is refused: the mode would change a live path.
	body := []byte(`{"path":"/zone/f.txt","op":"chmod","kind":"file","mode":511}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-mode/entries", bytes.NewReader(body))
	req = req.WithContext(withScope(req.Context(), scoped))
	rr := httptest.NewRecorder()
	s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("scoped chmod entry status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
	if entries, err := store.ListFSLayerEntryLog(ctx, "layer-mode"); err != nil || len(entries) != 0 {
		t.Fatalf("entries=%+v err=%v, want none after the denial", entries, err)
	}

	// A creation mode on a path that does not exist yet is still allowed: that is the
	// same power create/mkdir already have.
	createBody := []byte(`{"path":"/zone/new.txt","op":"upsert","kind":"file","mode":384,"content_text":"x"}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/layers/layer-mode/entries", bytes.NewReader(createBody))
	req = req.WithContext(withScope(req.Context(), scoped))
	rr = httptest.NewRecorder()
	s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusOK {
		t.Fatalf("scoped create entry status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}

	// Commit-time check: an owner-prepared mode change must not be committed by a scoped
	// token, even though the entries predate this request.
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-mode-owner", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	ownerLayer, err := store.GetFSLayer(ctx, "layer-mode-owner")
	if err != nil {
		t.Fatal(err)
	}
	ownerBody := []byte(`{"path":"/zone/f.txt","op":"chmod","kind":"file","mode":511}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/layers/layer-mode-owner/entries", bytes.NewReader(ownerBody))
	req = req.WithContext(withScope(req.Context(), &TenantScope{Backend: s.fallback}))
	rr = httptest.NewRecorder()
	s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, ownerLayer)
	if rr.Code != http.StatusOK {
		t.Fatalf("owner chmod entry status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/layers/layer-mode-owner/commit", nil)
	req = req.WithContext(withScope(req.Context(), scoped))
	rr = httptest.NewRecorder()
	s.handleFSLayerCommit(rr, req, s.fallback, store, ownerLayer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("scoped commit of a mode-changing layer status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Mode&0o777 == 0o777 {
		t.Fatal("denied scoped commit still applied the mode change")
	}

	// The owner commits the same kind of plan on a fresh layer: the denial above left
	// the first one conflicted, which is the documented state after a refused commit.
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-mode-pass", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	passLayer, err := store.GetFSLayer(ctx, "layer-mode-pass")
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/layers/layer-mode-pass/entries", bytes.NewReader(ownerBody))
	req = req.WithContext(withScope(req.Context(), &TenantScope{Backend: s.fallback}))
	rr = httptest.NewRecorder()
	s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, passLayer)
	if rr.Code != http.StatusOK {
		t.Fatalf("owner entry on fresh layer status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/layers/layer-mode-pass/commit", nil)
	req = req.WithContext(withScope(req.Context(), &TenantScope{Backend: s.fallback}))
	rr = httptest.NewRecorder()
	passLayer, err = store.GetFSLayer(ctx, "layer-mode-pass")
	if err != nil {
		t.Fatal(err)
	}
	s.handleFSLayerCommit(rr, req, s.fallback, store, passLayer)
	if rr.Code != http.StatusOK {
		t.Fatalf("owner commit status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	nf, err = s.fallback.StatNodeCtx(ctx, "/zone/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Mode&0o777 != 0o777 {
		t.Fatalf("owner commit mode=%o, want 777", nf.Mode&0o777)
	}
}

// An upload entry always carries an effective mode (the datastore defaults it), so a
// scoped upload over a file whose live mode differs is a mode change and is refused
// rather than silently resetting it to the default.
func TestFSLayerScopedObjectUploadRefusesModeChange(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/secret.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/secret.txt", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-upload", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-upload")
	if err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
	}
	// The defaulted 0644 would silently reset the live 0600 file, so the upload is
	// refused instead of applying it.
	req := httptest.NewRequest(http.MethodPut, "/v1/layers/layer-upload/objects?path=/zone/secret.txt&size=3", bytes.NewReader([]byte("new")))
	req = req.WithContext(withScope(req.Context(), scoped))
	rr := httptest.NewRecorder()
	s.handleFSLayerObjectUpload(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("scoped upload status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
	if entries, err := store.ListFSLayerEntryLog(ctx, "layer-upload"); err != nil || len(entries) != 0 {
		t.Fatalf("entries=%+v err=%v, want none", entries, err)
	}
	// A path that does not exist yet has no mode to change, so the same token may
	// create it.
	req = httptest.NewRequest(http.MethodPut, "/v1/layers/layer-upload/objects?path=/zone/fresh.txt&size=3", bytes.NewReader([]byte("new")))
	req = req.WithContext(withScope(req.Context(), scoped))
	rr = httptest.NewRecorder()
	s.handleFSLayerObjectUpload(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusOK {
		t.Fatalf("scoped create status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
}

// Plan order decides whether an upsert rewrites a live inode's mode or creates a new
// file: a path recoccupied by a later rename is not a creation, a vacated path is, and an
// upsert before the rename still overwrites the live inode. The predicate follows rename
// chains to a fixpoint, so a plan cannot smuggle a live inode's mode through two hops.
func TestFSLayerModeChangeRespectsPlanOrder(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	for _, path := range []string{"/x/f.txt", "/x/g.txt"} {
		if _, err := s.fallback.WriteCtx(ctx, path, []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
			t.Fatal(err)
		}
		if err := s.fallback.ChmodCtx(ctx, path, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rename := func(from, to string) datastore.FSLayerEntry {
		return datastore.FSLayerEntry{Path: from, Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindFile, ContentText: to}
	}
	upsert := func(path string, mode uint32) datastore.FSLayerEntry {
		return datastore.FSLayerEntry{Path: path, Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: mode}
	}
	whiteout := func(path string) datastore.FSLayerEntry {
		return datastore.FSLayerEntry{Path: path, Op: datastore.FSLayerEntryOpWhiteout, Kind: datastore.FSLayerEntryKindFile}
	}
	decide := func(plan ...datastore.FSLayerEntry) bool {
		t.Helper()
		changes, err := fsLayerPlanChangesExistingMode(ctx, s.fallback, plan)
		if err != nil {
			t.Fatal(err)
		}
		return changes
	}

	// The live 0600 inode moves away and the path is recreated: a creation mode.
	if decide(rename("/x/f.txt", "/x/g.txt"), upsert("/x/f.txt", 0o644)) {
		t.Fatal("an upsert at a vacated path was treated as a mode change")
	}
	// The upsert comes first: it overwrites the live inode.
	if !decide(upsert("/x/f.txt", 0o644), rename("/x/f.txt", "/x/g.txt")) {
		t.Fatal("an upsert before the rename was treated as a creation")
	}
	// A whiteout vacates the path the same way.
	if decide(whiteout("/x/f.txt"), upsert("/x/f.txt", 0o644)) {
		t.Fatal("an upsert after a whiteout was treated as a mode change")
	}
	// A rename destination resolves to the live source inode.
	if !decide(rename("/x/f.txt", "/x/g.txt"), upsert("/x/g.txt", 0o777)) {
		t.Fatal("an upsert at a rename destination bypassed the live-inode comparison")
	}
	// Two hops: the plan renames the live 0600 inode twice, so the upsert at the final
	// name still owns that inode.
	if !decide(rename("/x/f.txt", "/x/g.txt"), rename("/x/g.txt", "/x/h.txt"), upsert("/x/h.txt", 0o644)) {
		t.Fatal("a two-hop rename chain ended in a live inode but was treated as a creation")
	}
	// Renamed away and back: the original inode returns to its own path.
	if !decide(rename("/x/f.txt", "/x/g.txt"), rename("/x/g.txt", "/x/f.txt"), upsert("/x/f.txt", 0o644)) {
		t.Fatal("a rename that lands back on its source was treated as a creation")
	}
	// Vacated, then re-occupied by another live inode: the vacate must not short-circuit.
	if !decide(whiteout("/x/f.txt"), rename("/x/g.txt", "/x/f.txt"), upsert("/x/f.txt", 0o644)) {
		t.Fatal("a rename into a vacated path lost its live inode identity")
	}
	// The mode already matches, so nothing changes.
	if decide(rename("/x/f.txt", "/x/g.txt"), upsert("/x/g.txt", 0o600)) {
		t.Fatal("a matching mode was reported as a change")
	}
	// A chmod entry on a live path is always a change, and on a plan-created one is not.
	if !decide(rename("/x/f.txt", "/x/g.txt"), datastore.FSLayerEntry{Path: "/x/g.txt", Op: datastore.FSLayerEntryOpChmod, Mode: 0o600}) {
		t.Fatal("a chmod entry on a live inode was not reported as a change")
	}
	if decide(rename("/x/f.txt", "/x/g.txt"), upsert("/x/f.txt", 0o644), datastore.FSLayerEntry{Path: "/x/f.txt", Op: datastore.FSLayerEntryOpChmod, Mode: 0o700}) {
		t.Fatal("a chmod entry on a plan-created path was reported as a live change")
	}
}

// The entries route decides with the same plan semantics as commit, so a plan that
// renames a 0600 file and then recreates the source path is allowed for a scoped token.
func TestFSLayerEntryWriteAllowsVacatedPathForScopedToken(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/f.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/f.txt", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-vacate", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-vacate")
	if err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	owner := &TenantScope{Backend: s.fallback}
	post := func(scope *TenantScope, body string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-vacate/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scope))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	// The owner stages the rename, then the scoped token recreates the vacated path.
	if got := post(owner, `{"path":"/zone/f.txt","op":"rename","kind":"file","content_text":"/zone/g.txt"}`); got != http.StatusOK {
		t.Fatalf("owner rename status=%d, want 200", got)
	}
	if got := post(scoped, `{"path":"/zone/f.txt","op":"upsert","kind":"file","content_text":"new"}`); got != http.StatusOK {
		t.Fatalf("scoped upsert at the vacated path status=%d, want 200", got)
	}
}

// The apply-time guard is the second layer: even if a handler decides to apply a
// mode-changing entry, the apply itself refuses without chmod authority.
func TestApplyFSLayerEntryRefusesModeChangeWithoutChmod(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, err := s.fallback.WriteCtx(ctx, "/f.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	entry := datastore.FSLayerEntry{Path: "/f.txt", Op: datastore.FSLayerEntryOpChmod, Kind: datastore.FSLayerEntryKindFile, Mode: 0o777}
	err := applyFSLayerEntry(ctx, s.fallback, &entry, map[string]struct{}{}, fsLayerApplyOptions{})
	if !errors.Is(err, errChmodForbidden) {
		t.Fatalf("apply err=%v, want errChmodForbidden", err)
	}
	if nf, statErr := s.fallback.StatNodeCtx(ctx, "/f.txt"); statErr != nil || nf.Mode&0o777 == 0o777 {
		t.Fatalf("mode change applied without chmod authority: mode=%o err=%v", nf.Mode&0o777, statErr)
	}
	// With authority the same entry applies.
	if err := applyFSLayerEntry(ctx, s.fallback, &entry, map[string]struct{}{}, fsLayerApplyOptions{canChmod: true}); err != nil {
		t.Fatalf("owner apply: %v", err)
	}
	if nf, err := s.fallback.StatNodeCtx(ctx, "/f.txt"); err != nil || nf.Mode&0o777 != 0o777 {
		t.Fatalf("owner apply mode=%o err=%v, want 777", nf.Mode&0o777, err)
	}
}
