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
	// The upsert names a different mode for a path the self-rename left untouched.
	if got := post(scoped, `{"path":"/zone/f.txt","op":"upsert","kind":"file","mode":511,"content_text":"new"}`); got != http.StatusForbidden {
		t.Fatalf("scoped upsert after a self-rename status=%d, want 403", got)
	}
	if got := post(scoped, `{"path":"/zone/f.txt","op":"chmod","kind":"file","mode":511}`); got != http.StatusForbidden {
		t.Fatalf("scoped chmod after a self-rename status=%d, want 403", got)
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
func TestFSLayerScopedCommitAllowsRestylingAPlanCreatedPath(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/rot.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/rot.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	for idx, entries := range [][]string{
		{
			`{"path":"/zone/a.txt","op":"upsert","kind":"file","mode":420,"content_text":"one"}`,
			`{"path":"/zone/a.txt","op":"upsert","kind":"file","mode":384,"content_text":"two"}`,
		},
		{
			`{"path":"/zone/d/","op":"mkdir","kind":"dir","mode":448}`,
			`{"path":"/zone/d/","op":"upsert","kind":"dir","mode":493}`,
		},
		{
			// Log rotation: a live inode moves aside, a new file is written at the old
			// name, and the rotated file keeps its own mode.
			`{"path":"/zone/rot.txt","op":"rename","kind":"file","content_text":"/zone/rot.txt.1"}`,
			`{"path":"/zone/rot.txt","op":"upsert","kind":"file","mode":384,"content_text":"fresh"}`,
			`{"path":"/zone/rot.txt.1","op":"upsert","kind":"file","mode":420,"content_text":"rotated"}`,
		},
		{
			// A symlink is the one creation that carries no mode of its own; a later
			// entry at its path is still a creation-mode write.
			`{"path":"/zone/link","op":"symlink","kind":"symlink","content_text":"rot.txt"}`,
			`{"path":"/zone/link","op":"upsert","kind":"file","mode":384,"content_text":"through"}`,
		},
	} {
		layerID := fmt.Sprintf("layer-restyle-%d", idx)
		if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: layerID, BaseRootPath: "/zone"}); err != nil {
			t.Fatal(err)
		}
		layer, err := store.GetFSLayer(ctx, layerID)
		if err != nil {
			t.Fatal(err)
		}
		for _, body := range entries {
			req := httptest.NewRequest(http.MethodPost, "/v1/layers/"+layerID+"/entries", bytes.NewReader([]byte(body)))
			req = req.WithContext(withScope(req.Context(), scoped))
			rr := httptest.NewRecorder()
			s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
			if rr.Code != http.StatusOK {
				t.Fatalf("case %d: scoped staging %s status=%d body=%s, want 200", idx, body, rr.Code, rr.Body.String())
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
		if rr.Code != http.StatusOK {
			t.Fatalf("case %d: scoped commit of a restyled plan status=%d body=%s, want 200", idx, rr.Code, rr.Body.String())
		}
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.Mode&0o777 != 0o600 {
		t.Fatalf("restyled file mode = %o, want 600", nf.Mode&0o777)
	}
	// The rotation kept the live inode's mode and gave the new file the requested one.
	if nf, err = s.fallback.StatNodeCtx(ctx, "/zone/rot.txt.1"); err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o644 {
		t.Fatalf("rotated file mode = %o, want the live 644", got)
	}
	if nf, err = s.fallback.StatNodeCtx(ctx, "/zone/rot.txt"); err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o600 {
		t.Fatalf("recreated log file mode = %o, want 600", got)
	}
	// The symlink's path was plan-created too, so the write after it is a creation mode.
	if nf, err = s.fallback.StatNodeCtx(ctx, "/zone/link"); err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o600 {
		t.Fatalf("symlink-path write mode = %o, want 600", got)
	}
	nf, err = s.fallback.StatNodeCtx(ctx, "/zone/d")
	if err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o755 {
		t.Fatalf("restyled dir mode = %o, want 755", got)
	}
}

// The apply-time guard compares a renamed live inode as it is now, not the path it used
// to have: otherwise the drift check would be vacuous for exactly the shapes it exists
// for.
func TestFSLayerAppliedModeChangeFollowsRenamedInode(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/a.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.RenameCtx(ctx, "/zone/a.txt", "/zone/b.txt"); err != nil {
		t.Fatal(err)
	}
	ownership := newFSLayerPlanOwnership()
	ownership.noteMove(&datastore.FSLayerEntry{Path: "/zone/a.txt", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindFile, ContentText: "/zone/b.txt"})
	if ownership.owns("/zone/b.txt") {
		t.Fatal("a rename of a live inode made the target plan-owned")
	}
	changes, err := ownership.appliedModeChange(ctx, s.fallback, &datastore.FSLayerEntry{Path: "/zone/b.txt", Op: datastore.FSLayerEntryOpUpsert, Mode: 0o777})
	if err != nil {
		t.Fatal(err)
	}
	if !changes {
		t.Fatal("a mode change on the renamed live inode was not detected")
	}
	// Once the commit owns the name, there is no live inode to protect.
	ownership.own("/zone/b.txt")
	changes, err = ownership.appliedModeChange(ctx, s.fallback, &datastore.FSLayerEntry{Path: "/zone/b.txt", Op: datastore.FSLayerEntryOpUpsert, Mode: 0o777})
	if err != nil {
		t.Fatal(err)
	}
	if changes {
		t.Fatal("a plan-owned name was still treated as a live inode")
	}
	// A chmod on a path the commit does not own and live main does not have is a no-op.
	changes, err = ownership.appliedModeChange(ctx, s.fallback, &datastore.FSLayerEntry{Path: "/zone/missing.txt", Op: datastore.FSLayerEntryOpChmod, Mode: 0o700})
	if err != nil {
		t.Fatal(err)
	}
	if changes {
		t.Fatal("a chmod of a missing path was reported as a live change")
	}

	// A self-rename is a backend no-op: it must not drop ownership of a name the commit
	// created, or the next entry there is compared against that very inode.
	self := newFSLayerPlanOwnership()
	self.own("/zone/own.txt")
	self.noteMove(&datastore.FSLayerEntry{Path: "/zone/own.txt", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindFile, ContentText: "/zone/own.txt"})
	if !self.owns("/zone/own.txt") {
		t.Fatal("a self-rename dropped ownership of a plan-created name")
	}

	// A live inode renamed under a plan-created directory stays live: the guard must not
	// inherit it as plan-owned through the ancestor walk.
	nested := newFSLayerPlanOwnership()
	nested.own("/zone/d")
	nested.noteMove(&datastore.FSLayerEntry{Path: "/zone/live.txt", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindFile, ContentText: "/zone/d/live.txt"})
	if nested.owns("/zone/d/live.txt") {
		t.Fatal("a live inode under a plan-created directory was treated as plan-owned")
	}
	if !nested.owns("/zone/d/other.txt") {
		t.Fatal("a name under a plan-created directory lost its inherited ownership")
	}

	// A directory rename carries its subtree: the live exclusion must move with it, or the
	// live inode is laundered into the renamed plan-owned container.
	renamed := newFSLayerPlanOwnership()
	renamed.own("/zone/d")
	renamed.markLive("/zone/d/live.txt")
	renamed.noteMove(&datastore.FSLayerEntry{Path: "/zone/d", Op: datastore.FSLayerEntryOpRename, Kind: datastore.FSLayerEntryKindDir, ContentText: "/zone/e"})
	if !renamed.owns("/zone/e") {
		t.Fatal("the renamed container lost its plan ownership")
	}
	if renamed.owns("/zone/e/live.txt") {
		t.Fatal("a directory rename laundered a live inode into plan ownership")
	}
}

// The scoped delete-then-recreate flow the vacated-path rule blesses has to survive the
// gate: a staged whiteout stores a NULL target, so the moves projection used to fail the
// next staging with a scan error.
func TestFSLayerScopedStageDeleteThenRecreate(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/w.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-recreate", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-recreate")
	if err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	post := func(body string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-recreate/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scoped))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	if got := post(`{"path":"/zone/w.txt","op":"whiteout","kind":"file"}`); got != http.StatusOK {
		t.Fatalf("scoped whiteout staging status=%d, want 200", got)
	}
	if got := post(`{"path":"/zone/w.txt","op":"upsert","kind":"file","mode":384,"content_text":"recreated"}`); got != http.StatusOK {
		t.Fatalf("scoped recreate staging status=%d, want 200", got)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-recreate/commit", nil)
	req = req.WithContext(withScope(req.Context(), scoped))
	rr := httptest.NewRecorder()
	layer, err = store.GetFSLayer(ctx, "layer-recreate")
	if err != nil {
		t.Fatal(err)
	}
	s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusOK {
		t.Fatalf("scoped recreate commit status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/w.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o600 {
		t.Fatalf("recreated mode = %o, want 600", got)
	}
}

// A self-rename of a name the plan created is a no-op, so the restyle entry after it is
// still a creation-mode write on the plan's own inode.
func TestFSLayerScopedCommitAllowsRestyleAfterSelfRename(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/a.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-self-restyle", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-self-restyle")
	if err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	for _, body := range []string{
		`{"path":"/zone/a.txt","op":"rename","kind":"file","content_text":"/zone/c.txt"}`,
		`{"path":"/zone/a.txt","op":"upsert","kind":"file","mode":384,"content_text":"new"}`,
		`{"path":"/zone/a.txt","op":"rename","kind":"file","content_text":"/zone/a.txt"}`,
		`{"path":"/zone/a.txt","op":"upsert","kind":"file","mode":420,"content_text":"newer"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-self-restyle/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scoped))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		if rr.Code != http.StatusOK {
			t.Fatalf("scoped staging %s status=%d body=%s, want 200", body, rr.Code, rr.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-self-restyle/commit", nil)
	req = req.WithContext(withScope(req.Context(), scoped))
	rr := httptest.NewRecorder()
	layer, err = store.GetFSLayer(ctx, "layer-self-restyle")
	if err != nil {
		t.Fatal(err)
	}
	s.handleFSLayerCommit(rr, req, s.fallback, store, layer)
	if rr.Code != http.StatusOK {
		t.Fatalf("scoped commit after self-rename status=%d body=%s, want 200", rr.Code, rr.Body.String())
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o644 {
		t.Fatalf("final mode = %o, want 644", got)
	}
}

// The five-entry laundering attempt from the review, end to end: a live file is moved
// into a plan-created directory, the container is renamed, and the chmod under the new
// name is refused because the rename carries the subtree's live identity with it.
func TestFSLayerScopedStageRefusesModeChangeThroughDirectoryRename(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	store := s.fallback.Store()
	if _, err := s.fallback.WriteCtx(ctx, "/zone/live.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	if err := s.fallback.ChmodCtx(ctx, "/zone/live.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateFSLayer(ctx, &datastore.FSLayer{LayerID: "layer-launder", BaseRootPath: "/zone"}); err != nil {
		t.Fatal(err)
	}
	layer, err := store.GetFSLayer(ctx, "layer-launder")
	if err != nil {
		t.Fatal(err)
	}
	scoped := &TenantScope{
		Backend:  s.fallback,
		IsScoped: true,
		FSScopes: []FSScope{{Prefix: "/zone", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true, FSOpDelete: true}}},
	}
	post := func(body string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/layers/layer-launder/entries", bytes.NewReader([]byte(body)))
		req = req.WithContext(withScope(req.Context(), scoped))
		rr := httptest.NewRecorder()
		s.handleFSLayerEntryUpsert(rr, req, s.fallback, store, layer)
		return rr.Code
	}
	for _, body := range []string{
		`{"path":"/zone/d/","op":"whiteout","kind":"dir"}`,
		`{"path":"/zone/d/","op":"mkdir","kind":"dir","mode":493}`,
		`{"path":"/zone/live.txt","op":"rename","kind":"file","content_text":"/zone/d/live.txt"}`,
		`{"path":"/zone/d/","op":"rename","kind":"dir","content_text":"/zone/e/"}`,
	} {
		if got := post(body); got != http.StatusOK {
			t.Fatalf("scoped staging %s status=%d, want 200", body, got)
		}
	}
	// The chmod is the escalation attempt: refused at staging, before any commit.
	if got := post(`{"path":"/zone/e/live.txt","op":"chmod","kind":"file","mode":511}`); got != http.StatusForbidden {
		t.Fatalf("scoped chmod through a directory rename status=%d, want 403", got)
	}
	nf, err := s.fallback.StatNodeCtx(ctx, "/zone/live.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := fsLayerNodeMode(nf) & 0o777; got != 0o644 {
		t.Fatalf("live mode = %o, want 644", got)
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
