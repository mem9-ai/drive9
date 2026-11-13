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

// The apply-time guard is the second layer: even if a handler decides to apply a
// mode-changing entry, the apply itself refuses without chmod authority.
func TestApplyFSLayerEntryRefusesModeChangeWithoutChmod(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	if _, err := s.fallback.WriteCtx(ctx, "/f.txt", []byte("live"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	entry := datastore.FSLayerEntry{Path: "/f.txt", Op: datastore.FSLayerEntryOpChmod, Kind: datastore.FSLayerEntryKindFile, Mode: 0o777}
	err := applyFSLayerEntry(ctx, s.fallback, &entry, false, map[string]struct{}{}, false, false)
	if !errors.Is(err, errChmodForbidden) {
		t.Fatalf("apply err=%v, want errChmodForbidden", err)
	}
	if nf, statErr := s.fallback.StatNodeCtx(ctx, "/f.txt"); statErr != nil || nf.Mode&0o777 == 0o777 {
		t.Fatalf("mode change applied without chmod authority: mode=%o err=%v", nf.Mode&0o777, statErr)
	}
	// With authority the same entry applies.
	if err := applyFSLayerEntry(ctx, s.fallback, &entry, false, map[string]struct{}{}, false, true); err != nil {
		t.Fatalf("owner apply: %v", err)
	}
	if nf, err := s.fallback.StatNodeCtx(ctx, "/f.txt"); err != nil || nf.Mode&0o777 != 0o777 {
		t.Fatalf("owner apply mode=%o err=%v, want 777", nf.Mode&0o777, err)
	}
}
