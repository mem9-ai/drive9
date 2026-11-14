package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	for _, path := range []string{"/x/f.txt", "/x/g.txt", "/zone/live.txt"} {
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
	// A directory rename carries the subtree: a live file moved inside, then the container
	// renamed, must still be recognised as that live inode.
	{
		plan := []datastore.FSLayerEntry{
			{Path: "/zone/d/", Op: datastore.FSLayerEntryOpWhiteout, Kind: datastore.FSLayerEntryKindDir},
			{Path: "/zone/d/", Op: datastore.FSLayerEntryOpMkdir, Kind: datastore.FSLayerEntryKindDir, Mode: 0o755},
			{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindFile, ContentText: "/zone/d/live.txt"},
			{Path: "/zone/d/", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindDir, ContentText: "/zone/e/"},
			{Path: "/zone/e/live.txt", Op: datastore.FSLayerEntryOpChmod, Kind: datastore.FSLayerEntryKindFile, Mode: 0o777},
		}
		changes, err := fsLayerPlanChangesExistingMode(ctx, s.fallback, plan)
		if err != nil {
			t.Fatal(err)
		}
		if !changes {
			t.Fatal("a directory rename laundered a live inode past the pre-apply scan")
		}
	}

	// A self-rename is a backend no-op, so the path keeps its live inode and an upsert
	// with a different mode is a change.
	if !decide(rename("/x/f.txt", "/x/f.txt"), upsert("/x/f.txt", 0o644)) {
		t.Fatal("a self-rename vacated its own path and masked a live mode change")
	}
	if !decide(rename("/x/f.txt", "/x/f.txt"), datastore.FSLayerEntry{Path: "/x/f.txt", Op: datastore.FSLayerEntryOpChmod, Mode: 0o644}) {
		t.Fatal("a self-rename masked a chmod of a live inode")
	}
	// Two sources renamed into one target: the second replaces the first, so the final
	// name holds /x/g.txt's live inode and compares against that.
	if !decide(rename("/x/f.txt", "/x/t.txt"), rename("/x/g.txt", "/x/t.txt"), upsert("/x/t.txt", 0o644)) {
		t.Fatal("the last rename into a shared target did not define the inode")
	}
	// The mode already matches, so nothing changes.
	if decide(rename("/x/f.txt", "/x/g.txt"), upsert("/x/g.txt", 0o600)) {
		t.Fatal("a matching mode was reported as a change")
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

// The blocking case from the review, end to end: a self-rename is a backend no-op, so a
// scoped token must not be able to stage it and then rewrite the live file's mode under
// the cover of "the path was vacated".
func TestFSLayerSelfRenameCannotMaskModeChange(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/f.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/f.txt", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-self", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-self")
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
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-self/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scope))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	if got := post(owner, `{"path":"/zone/f.txt","op":"rename","kind":"file","content_text":"/zone/f.txt"}`); got != http.StatusOK {
		t.Fatalf("owner self-rename status=%d, want 200", got)
	}
	// The upsert names a different mode for a path the self-rename left untouched, which is
	// the masking this test is about (a chmod entry is refused by policy, see
	// TestFSLayerScopedChmodEntryOnPlanCreatedPathRefused, so it cannot pin this).
	if got := post(scoped, `{"path":"/zone/f.txt","op":"upsert","kind":"file","mode":511,"content_text":"new"}`); got != http.StatusForbidden {
		t.Fatalf("scoped upsert after a self-rename status=%d, want 403", got)
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Mode&0o777 != 0o600 {
		t.Fatalf("live mode = %o, want 600", nf.Mode&0o777)
	}
	// A scoped commit of an owner-staged self-rename plan is refused as well.
	ownerLayer := datastore.FSLayer{LayerID: "layer-self-commit", BaseRootPath: "/zone"}
	if err := store.CreateFSLayer(ctx, &ownerLayer); err != nil {
		t.Fatal(err)
	}
	commitLayer, err := store.GetFSLayer(ctx, "layer-self-commit")
	if err != nil {
		t.Fatal(err)
	}
	postTo := func(scope *TenantScope, layer *datastore.FSLayer, body string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layer.LayerID+"/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scope))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	if got := postTo(owner, commitLayer, `{"path":"/zone/f.txt","op":"rename","kind":"file","content_text":"/zone/f.txt"}`); got != http.StatusOK {
		t.Fatalf("owner self-rename on commit layer status=%d", got)
	}
	if got := postTo(owner, commitLayer, `{"path":"/zone/f.txt","op":"chmod","kind":"file","mode":511}`); got != http.StatusOK {
		t.Fatalf("owner chmod entry status=%d", got)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-self-commit/commit", nil)
	req = req.WithContext(withScope(req.Context(), scoped))
	rr := httptest.NewRecorder()
	commitLayer, err = store.GetFSLayer(ctx, "layer-self-commit")
	if err != nil {
		t.Fatal(err)
	}
	s.handleFSLayerCommit(rr, req, s.fallback, store, commitLayer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("scoped commit of a self-rename plan status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
}

// The chain/reoccupy families are also refused through the real commit handler, not only
// at the predicate: a scoped token committing an owner-staged two-hop rename plan that
// ends on a live inode's mode gets 403.
func TestFSLayerScopedCommitRefusesRenameChainModeChange(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/f.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/f.txt", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-chain", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-chain")
	if err != nil {
		t.Fatal(err)
	}
	owner := &TenantScope{Backend: s.fallback}
	post := func(body string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-chain/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), owner))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	if got := post(`{"path":"/zone/f.txt","op":"rename","kind":"file","content_text":"/zone/g.txt"}`); got != http.StatusOK {
		t.Fatalf("owner rename 1 status=%d", got)
	}
	if got := post(`{"path":"/zone/g.txt","op":"rename","kind":"file","content_text":"/zone/h.txt"}`); got != http.StatusOK {
		t.Fatalf("owner rename 2 status=%d", got)
	}
	if got := post(`{"path":"/zone/h.txt","op":"upsert","kind":"file","mode":511,"content_text":"new"}`); got != http.StatusOK {
		t.Fatalf("owner upsert at the chain end status=%d", got)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-chain/commit", nil)
	req = req.WithContext(withScope(req.Context(), scoped))
	rr := httptest.NewRecorder()
	layer, err = store.GetFSLayer(ctx, "layer-chain")
	if err != nil {
		t.Fatal(err)
	}
	s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("scoped commit of a rename-chain mode change status=%d body=%s, want 403", rr.Code, rr.Body.String())
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Mode&0o777 != 0o600 {
		t.Fatalf("live mode = %o, want 600", nf.Mode&0o777)
	}
}

// A plan that creates a path and then restyles it must commit for a scoped token: the
// second entry writes a mode to an inode this commit just made, so there is no live mode
// to change. Regression for the apply-time re-check denying on the plan's own creation.
// fs parity: a creation mode belongs to the inode the write created, so a plan may create a
// path with a mode (and rename it), but it may not restyle a path the same plan created —
// that is a mode change on an existing inode, which an fs_scoped caller may not make. These
// are the shapes fs's createdIDs + mode-match design accepts and refuses, end to end.
func TestFSLayerScopedCommitModeSemantics(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	for _, path := range []string{"/zone/rot.txt", "/zone/rep.txt", "/zone/w-gone.txt"} {
		if _, err := s.fallback.WriteCtx(ctx, path, []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
			t.Fatal(err)
		}
		if err := s.fallback.ChmodCtx(ctx, path, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	type planCase struct {
		name    string
		entries []string
		want    int
	}
	cases := []planCase{
		{
			// Log rotation: the live inode moves aside and keeps its 0644; the new file is a
			// creation and gets 0600.
			name: "rotation",
			entries: []string{
				`{"path":"/zone/rot.txt","op":"rename","kind":"file","content_text":"/zone/rot.txt.1"}`,
				`{"path":"/zone/rot.txt","op":"upsert","kind":"file","mode":384,"content_text":"fresh"}`,
				`{"path":"/zone/rot.txt.1","op":"upsert","kind":"file","mode":420,"content_text":"rotated"}`,
			},
			want: http.StatusOK,
		},
		{
			// Recreate after a whiteout: a creation mode.
			name: "whiteout-recreate",
			entries: []string{
				`{"path":"/zone/rep.txt","op":"whiteout","kind":"file"}`,
				`{"path":"/zone/rep.txt","op":"upsert","kind":"file","mode":384,"content_text":"new"}`,
			},
			want: http.StatusOK,
		},
		{
			// Restyling a path the same plan created is a mode change on an existing inode.
			name: "restyle-plan-created",
			entries: []string{
				`{"path":"/zone/a.txt","op":"upsert","kind":"file","mode":420,"content_text":"one"}`,
				`{"path":"/zone/a.txt","op":"upsert","kind":"file","mode":384,"content_text":"two"}`,
			},
			want: http.StatusForbidden,
		},
		{
			name: "restyle-after-whiteout",
			entries: []string{
				`{"path":"/zone/w.txt","op":"upsert","kind":"file","mode":420,"content_text":"one"}`,
				`{"path":"/zone/w-gone.txt","op":"whiteout","kind":"file"}`,
				`{"path":"/zone/w.txt","op":"upsert","kind":"file","mode":384,"content_text":"two"}`,
			},
			want: http.StatusForbidden,
		},
		{
			// An implicitly created parent is a creation for the upsert, but a later entry
			// that gives it other bits changes an existing inode: fs refuses it, and the scan
			// has to refuse it before anything applies rather than leave the plan half-applied.
			name: "implicit-parent-restyle",
			entries: []string{
				`{"path":"/zone/n/f.txt","op":"upsert","kind":"file","mode":420,"content_text":"x"}`,
				`{"path":"/zone/n/","op":"upsert","kind":"dir","mode":448,"content_text":""}`,
			},
			want: http.StatusForbidden,
		},
		{
			name: "restyle-after-rename",
			entries: []string{
				`{"path":"/zone/rep.txt","op":"whiteout","kind":"file"}`,
				`{"path":"/zone/rep.txt","op":"upsert","kind":"file","mode":420,"content_text":"one"}`,
				`{"path":"/zone/rep.txt","op":"rename","kind":"file","content_text":"/zone/rep2.txt"}`,
				`{"path":"/zone/rep2.txt","op":"upsert","kind":"file","mode":384,"content_text":"two"}`,
			},
			want: http.StatusForbidden,
		},
	}
	for idx, tc := range cases {
		layerID := fmt.Sprintf("layer-mode-%d", idx)
		if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: layerID, BaseRootPath: "/zone"}); err != nil {
			t.Fatal(err)
		}
		layer, err := store.GetFSLayer(ctx, layerID)
		if err != nil {
			t.Fatal(err)
		}
		for _, body := range tc.entries {
			req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/entries", bytes.NewReader([]byte(body)))
			req = req.WithContext(withScope(req.Context(), scoped))
			rr := httptest.NewRecorder()
			s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
			if rr.Code != http.StatusOK {
				t.Fatalf("%s: staging %s status=%d body=%s, want 200", tc.name, body, rr.Code, rr.Body.String())
			}
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/commit", nil)
		req = req.WithContext(withScope(req.Context(), scoped))
		rr := httptest.NewRecorder()
		layer, err = store.GetFSLayer(ctx, layerID)
		if err != nil {
			t.Fatal(err)
		}
		s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
		if rr.Code != tc.want {
			t.Fatalf("%s: commit status=%d body=%s, want %d", tc.name, rr.Code, rr.Body.String(), tc.want)
		}
	}

	// The rotation committed: the moved live inode kept 0644, the new file took 0600.
	if nf, err := s.fallback.StatNodeCtx(ctx, "/zone/rot.txt.1"); err != nil {
		t.Fatal(err)
	} else if got := fsLayerNodeMode(nf) & 0o777; got != 0o644 {
		t.Fatalf("rotated file mode = %o, want the live 644", got)
	}
	if nf, err := s.fallback.StatNodeCtx(ctx, "/zone/rot.txt"); err != nil {
		t.Fatal(err)
	} else if got := fsLayerNodeMode(nf) & 0o777; got != 0o600 {
		t.Fatalf("recreated log file mode = %o, want 600", got)
	}
	// The whiteout-recreate committed with the creation mode.
	if nf, err := s.fallback.StatNodeCtx(ctx, "/zone/rep.txt"); err != nil {
		t.Fatal(err)
	} else if got := fsLayerNodeMode(nf) & 0o777; got != 0o600 {
		t.Fatalf("recreated file mode = %o, want 600", got)
	}
	// The refused plans left no partial state: the restyled files are absent.
	for _, path := range []string{"/zone/a.txt", "/zone/w.txt", "/zone/rep2.txt"} {
		if _, err := s.fallback.StatNodeCtx(ctx, path); !errors.Is(err, datastore.ErrNotFound) {
			t.Fatalf("%s survived a refused commit: err=%v", path, err)
		}
	}
}

// A chmod entry can never apply for a caller that may not chmod, so staging it is refused
// even when the target is a path the same plan creates — otherwise the layer accepts an
// entry that no commit can apply.
func TestFSLayerScopedChmodEntryOnPlanCreatedPathRefused(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-plan-chmod", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-plan-chmod")
	if err != nil {
		t.Fatal(err)
	}
	stage := func(body string) int {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-plan-chmod/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scoped))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	if code := stage(`{"path":"/zone/fresh.txt","op":"upsert","kind":"file","mode":384,"content_text":"x"}`); code != http.StatusOK {
		t.Fatalf("staging the creating upsert status=%d, want 200", code)
	}
	if code := stage(`{"path":"/zone/fresh.txt","op":"chmod","kind":"file","mode":448}`); code != http.StatusForbidden {
		t.Fatalf("staging a chmod entry status=%d, want 403", code)
	}
	// The commit scan decides the same way for a plan that predates the caller's scope: a
	// chmod entry can never apply, on a live inode, on one the plan creates, or on a missing
	// path.
	ctx2 := context.Background()
	for _, plan := range [][]datastore.FSLayerEntry{
		{{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpChmod, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600}},
		{
			{Path: "/zone/created.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
			{Path: "/zone/created.txt", Op: datastore.FSLayerEntryOpChmod, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600},
		},
		{{Path: "/zone/absent.txt", Op: datastore.FSLayerEntryOpChmod, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600}},
	} {
		changes, err := fsLayerPlanChangesExistingMode(ctx2, s.fallback, plan)
		if err != nil {
			t.Fatalf("scan %v: %v", plan, err)
		}
		if !changes {
			t.Fatalf("the scan let a scoped chmod plan through: %v", plan)
		}
	}
}

// The apply-time backstop refuses a mode application on an inode that already carries other
// bits — which is what a renamed or concurrently chmod-ed inode looks like to a caller that
// may not chmod.
func TestFSLayerRequireModeMatch(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/m.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/m.txt", 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fsLayerRequireModeMatch(ctx, s.fallback, "/zone/m.txt", 0o600, false); err != nil {
		t.Fatalf("a matching mode was refused: %v", err)
	}
	if err := fsLayerRequireModeMatch(ctx, s.fallback, "/zone/m.txt", 0o644, false); !errors.Is(err, errChmodForbidden) {
		t.Fatalf("a differing mode = %v, want errChmodForbidden", err)
	}
	if err := fsLayerRequireModeMatch(ctx, s.fallback, "/zone/missing.txt", 0o600, true); err != nil {
		t.Fatalf("a missing target with allowMissing was refused: %v", err)
	}
	if err := fsLayerRequireModeMatch(ctx, s.fallback, "/zone/missing.txt", 0o600, false); err == nil {
		t.Fatal("a missing target without allowMissing was accepted")
	}
}

// A body that names op=mkdir without a kind is a directory for the store, so it has to be
// one for the default mode too: 0644 would create an untraversable directory.
func TestFSLayerKindlessMkdirDefaultsToDirMode(t *testing.T) {
	entry := datastore.FSLayerEntry{Path: "/zone/d/", Op: datastore.FSLayerEntryOpMkdir}
	defaultFSLayerEntryMode(&entry)
	if entry.Mode != datastore.FSLayerDefaultDirMode {
		t.Fatalf("kindless mkdir mode = %o, want %o", entry.Mode, datastore.FSLayerDefaultDirMode)
	}
}

// A plan may stage the same object-backed path more than once — a mounted file flushes
// repeatedly — so the second apply has to overwrite the inode the first one created instead
// of inserting again and failing the whole commit.
func TestFSLayerCommitAppliesRepeatedObjectUpsertToSamePath(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	layerID := "layer-object-repeat"
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: layerID, BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, layerID)
	if err != nil {
		t.Fatal(err)
	}
	entries := []string{
		`{"path":"/zone/append.bin","op":"upsert","kind":"file","storage_type":"s3","storage_ref":"layers/layer-object-repeat/o1","size_bytes":3}`,
		`{"path":"/zone/append.bin","op":"upsert","kind":"file","storage_type":"s3","storage_ref":"layers/layer-object-repeat/o2","size_bytes":5}`,
	}
	owner := &TenantScope{Backend: s.fallback}
	for _, body := range entries {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), owner))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		if rr.Code != http.StatusOK {
			t.Fatalf("staging %s status=%d body=%s", body, rr.Code, rr.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/commit", nil)
	req = req.WithContext(withScope(req.Context(), owner))
	rr := httptest.NewRecorder()
	s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusOK {
		t.Fatalf("commit status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/append.bin")
	if err != nil {
		t.Fatalf("stat committed file: %v", err)
	}
	if nf.File == nil || nf.File.StorageRef != "layers/layer-object-repeat/o2" || nf.File.SizeBytes != 5 {
		t.Fatalf("committed file = %+v, want the second upsert's payload", nf.File)
	}
}

// Re-uploading over an existing file through a layer commit is the ordinary object-backed
// upsert, and it has to work for an owner and for a scoped token alike: the write finds the
// path taken, applies as an overwrite, and reports no created inode.
func TestFSLayerCommitOverwritesExistingObjectBackedFile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scoped bool
	}{
		{name: "owner"},
		{name: "scoped", scoped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			ctx := context.Background()
			store := s.fallback.Store()
			if _, err := s.fallback.WriteCtx(ctx, "/zone/upload.bin", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
				t.Fatal(err)
			}
			if err := s.fallback.ChmodCtx(ctx, "/zone/upload.bin", 0o644); err != nil {
				t.Fatal(err)
			}
			layerID := "layer-object-overwrite"
			if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: layerID, BaseRootPath: "/zone"}); err != nil {
				t.Fatal(err)
			}
			layer, err := store.GetFSLayer(ctx, layerID)
			if err != nil {
				t.Fatal(err)
			}
			scope := &TenantScope{Backend: s.fallback}
			if tc.scoped {
				scope = &TenantScope{
					Backend:  s.fallback,
					IsScoped: true,
					FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
				}
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/entries",
				bytes.NewReader([]byte(`{"path":"/zone/upload.bin","op":"upsert","kind":"file","storage_type":"s3","storage_ref":"layers/layer-object-overwrite/o1","size_bytes":9,"mode":420}`)))
			req = req.WithContext(withScope(req.Context(), scope))
			rr := httptest.NewRecorder()
			s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
			if rr.Code != http.StatusOK {
				t.Fatalf("staging status=%d body=%s, want 200", rr.Code, rr.Body.String())
			}
			req = httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/commit", nil)
			req = req.WithContext(withScope(req.Context(), scope))
			rr = httptest.NewRecorder()
			s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
			if rr.Code != http.StatusOK {
				t.Fatalf("commit status=%d body=%s, want 200", rr.Code, rr.Body.String())
			}
			nf, err := s.fallback.StatNodeCtx(ctx, "/zone/upload.bin")
			if err != nil {
				t.Fatalf("stat committed file: %v", err)
			}
			if nf.File == nil || nf.File.StorageRef != "layers/layer-object-overwrite/o1" || nf.File.SizeBytes != 9 {
				t.Fatalf("committed file = %+v, want the uploaded payload", nf.File)
			}
		})
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
	_, err := applyFSLayerEntry(ctx, s.fallback, &entry, map[string]struct{}{}, fsLayerApplyOptions{})
	if !errors.Is(err, errChmodForbidden) {
		t.Fatalf("apply err=%v, want errChmodForbidden", err)
	}
	if nf, statErr := s.fallback.StatNodeCtx(ctx, "/f.txt"); statErr != nil || nf.Mode&0o777 == 0o777 {
		t.Fatalf("mode change applied without chmod authority: mode=%o err=%v", nf.Mode&0o777, statErr)
	}
	// With authority the same entry applies.
	if _, err := applyFSLayerEntry(ctx, s.fallback, &entry, map[string]struct{}{}, fsLayerApplyOptions{canChmod: true}); err != nil {
		t.Fatalf("owner apply: %v", err)
	}
	if nf, err := s.fallback.StatNodeCtx(ctx, "/f.txt"); err != nil || nf.Mode&0o777 != 0o777 {
		t.Fatalf("owner apply mode=%o err=%v, want 777", nf.Mode&0o777, err)
	}
}

// The commit scan has to model what the plan itself creates: an entry that gives other
// bits to a name an earlier entry in the same plan created is a mode change on an existing
// inode by the time it applies, and the scan is where a refusal must happen so the commit
// never applies half a plan and rolls it back.
func TestFSLayerPlanScanSeesPlanCreations(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/live.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/live.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	type scanCase struct {
		name    string
		entries []datastore.FSLayerEntry
		want    bool
	}
	cases := []scanCase{
		{
			name:    "creation only",
			entries: []datastore.FSLayerEntry{{Path: "/zone/c1.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600}},
			want:    false,
		},
		{
			name: "restyle of a plan-created path",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/c2.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
				{Path: "/zone/c2.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600},
			},
			want: true,
		},
		{
			name: "second write with the creation's own bits",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/c3.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
				{Path: "/zone/c3.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
			},
			want: false,
		},
		{
			name: "sibling creation under an implicitly created parent",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/p1/f1.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
				{Path: "/zone/p1/f2.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600},
			},
			want: false,
		},
		{
			// The recording is keyed like resolve reads, so an implicit parent is a real
			// record: by the time the second entry applies, that directory exists with the
			// default bits.
			name: "restyle of an implicitly created parent",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/p2/f.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
				{Path: "/zone/p2/", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindDir, Mode: 0o700},
			},
			want: true,
		},
		{
			name: "whiteout then recreate",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpWhiteout, Kind: datastore.FSLayerEntryKindFile},
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600},
			},
			want: false,
		},
		{
			// A name the plan empties and refills is a creation for the entries after it,
			// exactly like a name that was empty all along.
			name: "restyle of a name recreated after a whiteout",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpWhiteout, Kind: datastore.FSLayerEntryKindFile},
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600},
			},
			want: true,
		},
		{
			// The creation identity follows the name through a rename of the recreated path.
			name: "restyle of a recreated and renamed path",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpWhiteout, Kind: datastore.FSLayerEntryKindFile},
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o644},
				{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindFile, ContentText: "/zone/moved.txt"},
				{Path: "/zone/moved.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600},
			},
			want: true,
		},
		{
			name: "mkdir then a differing mode",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/d1/", Op: datastore.FSLayerEntryOpMkdir, Kind: datastore.FSLayerEntryKindDir, Mode: 0o755},
				{Path: "/zone/d1/", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindDir, Mode: 0o700},
			},
			want: true,
		},
		{
			name: "rename of a plan-created directory keeps its bits",
			entries: []datastore.FSLayerEntry{
				{Path: "/zone/d2/", Op: datastore.FSLayerEntryOpMkdir, Kind: datastore.FSLayerEntryKindDir, Mode: 0o755},
				{Path: "/zone/d2/", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindDir, ContentText: "/zone/d3/"},
				{Path: "/zone/d3/", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindDir, Mode: 0o755},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		got, err := fsLayerPlanChangesExistingMode(ctx, s.fallback, tc.entries)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: changesMode=%v, want %v", tc.name, got, tc.want)
		}
	}
}

// A creation reports every inode it made, parents included: a rollback bound needs the
// directories, not just the file. An apply onto a path that already exists reports none.
func TestApplyFSLayerEntryReportsCreatedInodes(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	entry := datastore.FSLayerEntry{Path: "/zone/n/f.txt", Op: datastore.FSLayerEntryOpUpsert, Kind: datastore.FSLayerEntryKindFile, Mode: 0o600, ContentBlob: []byte("x")}
	created, err := applyFSLayerEntry(ctx, s.fallback, &entry, map[string]struct{}{}, fsLayerApplyOptions{canChmod: true})
	if err != nil {
		t.Fatalf("apply creation: %v", err)
	}
	want := []string{}
	for _, path := range []string{"/zone/", "/zone/n/", "/zone/n/f.txt"} {
		nf, statErr := s.fallback.StatNodeCtx(ctx, path)
		if statErr != nil {
			t.Fatalf("stat %s: %v", path, statErr)
		}
		want = append(want, fsLayerNodeInodeID(nf))
	}
	if len(created) != len(want) {
		t.Fatalf("created ids = %v, want %v", created, want)
	}
	for i := range want {
		if created[i] != want[i] {
			t.Fatalf("created ids = %v, want %v", created, want)
		}
	}
	// The commit loop marks a path it has applied, so a repeated entry is applied as an
	// overwrite rather than behind a revision expectation.
	again, err := applyFSLayerEntry(ctx, s.fallback, &entry, map[string]struct{}{entry.Path: {}}, fsLayerApplyOptions{canChmod: true})
	if err != nil {
		t.Fatalf("apply onto existing path: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("overwrite reported created ids %v, want none", again)
	}
}

// The rollback predicate is what keeps a failed commit from deleting a concurrent writer's
// tree: a non-empty directory at a name the snapshot says was empty is force-removed only
// while the inode there is still the one the commit created.
func TestRemoveFSLayerBasePathBoundsForceRemove(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	created, err := s.fallback.MkdirCreatedCtx(ctx, "/zone/own/", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.fallback.WriteCtx(ctx, "/zone/own/child.txt", []byte("own"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	createdSet := map[string]struct{}{}
	for _, id := range created {
		createdSet[id] = struct{}{}
	}
	ownsInode := func(p string) bool {
		nf, statErr := s.fallback.StatNodeCtx(ctx, p)
		if statErr != nil || nf == nil {
			return false
		}
		_, ok := createdSet[fsLayerNodeInodeID(nf)]
		return ok
	}
	if err := removeFSLayerBasePathIfExists(ctx, s.fallback, "/zone/own/", ownsInode); err != nil {
		t.Fatalf("force remove of the commit's own directory: %v", err)
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/zone/own/"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("own directory survived rollback: err=%v", err)
	}
	// A directory this commit did not create is left in place, and the rollback reports
	// that it could not restore the path rather than deleting foreign state.
	if _, err := s.fallback.MkdirCreatedCtx(ctx, "/zone/foreign/", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fallback.WriteCtx(ctx, "/zone/foreign/child.txt", []byte("foreign"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := removeFSLayerBasePathIfExists(ctx, s.fallback, "/zone/foreign/", ownsInode); err == nil {
		t.Fatal("a foreign non-empty directory was force-removed")
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/zone/foreign/child.txt"); err != nil {
		t.Fatalf("foreign child removed by rollback: %v", err)
	}
}

// A failed commit must undo the rename it already applied. The moved inode is not this
// commit's creation, but the target name is one the commit filled, so the rollback has to be
// allowed to empty it — otherwise a later entry's failure leaves the tree at the target and
// the source empty.
func TestFSLayerCommitRollbackUndoesAppliedRename(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.MkdirCreatedCtx(ctx, "/zone/src/", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fallback.WriteCtx(ctx, "/zone/src/child.txt", []byte("payload"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	layerID := "layer-rollback-rename"
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: layerID, BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	owner := &TenantScope{Backend: s.fallback}
	stage := func(body string) {
		t.Helper()
		layer, err := store.GetFSLayer(ctx, layerID)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), owner))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		if rr.Code != http.StatusOK {
			t.Fatalf("staging %s status=%d body=%s", body, rr.Code, rr.Body.String())
		}
	}
	stage(`{"path":"/zone/src/","op":"rename","kind":"dir","content_text":"/zone/dst/"}`)
	// A chmod of a path that does not exist cannot apply, so the commit fails after the
	// rename has been applied.
	stage(`{"path":"/zone/missing.txt","op":"chmod","kind":"file","mode":384}`)
	layer, err := store.GetFSLayer(ctx, layerID)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/commit", nil)
	req = req.WithContext(withScope(req.Context(), owner))
	rr := httptest.NewRecorder()
	s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
	if rr.Code == http.StatusOK {
		t.Fatalf("commit succeeded, want the chmod entry to fail it: %s", rr.Body.String())
	}
	if _, err := s.fallback.StatNodeCtx(ctx, "/zone/dst/"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("the failed commit kept the rename target: err=%v", err)
	}
	content, err := s.fallback.ReadCtx(ctx, "/zone/src/child.txt", 0, -1)
	if err != nil {
		t.Fatalf("the failed commit did not restore the source tree: %v", err)
	}
	if string(content) != "payload" {
		t.Fatalf("restored content = %q, want payload", content)
	}
}

// Parents an entry creates implicitly are the commit's own work, so a failed commit must
// remove them instead of leaving an empty tree behind.
func TestFSLayerCommitRollbackRemovesCreatedParents(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	layerID := "layer-rollback-parents"
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: layerID, BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	owner := &TenantScope{Backend: s.fallback}
	stage := func(body string) {
		t.Helper()
		layer, err := store.GetFSLayer(ctx, layerID)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), owner))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		if rr.Code != http.StatusOK {
			t.Fatalf("staging %s status=%d body=%s", body, rr.Code, rr.Body.String())
		}
	}
	stage(`{"path":"/zone/deep/nested/","op":"mkdir","kind":"dir","mode":493}`)
	stage(`{"path":"/zone/missing.txt","op":"chmod","kind":"file","mode":384}`)
	layer, err := store.GetFSLayer(ctx, layerID)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/commit", nil)
	req = req.WithContext(withScope(req.Context(), owner))
	rr := httptest.NewRecorder()
	s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
	if rr.Code == http.StatusOK {
		t.Fatalf("commit succeeded, want the chmod entry to fail it: %s", rr.Body.String())
	}
	for _, path := range []string{"/zone/deep/", "/zone/deep/nested/"} {
		if _, err := s.fallback.StatNodeCtx(ctx, path); !errors.Is(err, datastore.ErrNotFound) {
			t.Fatalf("the failed commit kept %s: err=%v", path, err)
		}
	}
}
