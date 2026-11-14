package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/backend"
)

// newBatchWriteModeTestServer builds a server over a real tenant schema so the mode
// bits can be read back from the datastore after the request.
func newBatchWriteModeTestServer(t *testing.T) (*Server, *backend.Dat9Backend) {
	t.Helper()
	s := newTestServer(t)
	return s, s.fallback
}

func doBatchWrite(t *testing.T, s *Server, scope *TenantScope, body batchWriteRequest) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/fs:batch-write", strings.NewReader(string(payload)))
	r.Header.Set("Content-Type", "application/json")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	s.handleBatchWrite(w, r)
	return w
}

// chmod is owner-only because it escalates ACLs, and a batch item's mode bits are the
// same power. A scoped agent token must not be able to set them through the batch route
// even though it may write bytes in its zone.
func TestBatchWriteRejectsModeFromScopedToken(t *testing.T) {
	s, b := newBatchWriteModeTestServer(t)
	ctx := t.Context()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/allowed/f.txt", []byte("old"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatal(err)
	}
	before, err := b.StatNodeCtx(ctx, "/allowed/f.txt")
	if err != nil {
		t.Fatal(err)
	}

	scoped := &TenantScope{
		IsScoped: true,
		Backend:  b,
		FSScopes: []FSScope{{Prefix: "/allowed", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
	}

	// The mode item is refused with the per-item 403 the BatchWriteResult contract
	// documents, and the content-only item of the same batch still commits. The
	// dimensions the contract pins: status 403, no revision, no mutation.
	w := doBatchWrite(t, s, scoped, batchWriteRequest{Items: []batchWriteItem{
		{Path: "/allowed/f.txt", ExpectedRevision: 1, Data: []byte("new"), Mode: 0o777, HasMode: true},
		{Path: "/allowed/g.txt", Data: []byte("content only")},
	}})
	if w.Code != http.StatusOK {
		t.Fatalf("scoped batch write status = %d, want 200 with a per-item 403: %s", w.Code, w.Body.String())
	}
	var out batchWriteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 2 {
		t.Fatalf("results len = %d, want 2", len(out.Results))
	}
	if got := out.Results[0]; got.Status != http.StatusForbidden || got.Revision != 0 || got.Error != chmodOwnerOnlyMessage {
		t.Fatalf("scoped mode item = %+v, want status 403, revision 0 and an error", got)
	}
	if got := out.Results[1]; got.Status != http.StatusOK {
		t.Fatalf("content-only item = %+v, want it to commit", got)
	}
	after, err := b.StatNodeCtx(ctx, "/allowed/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode&0o777 != before.Mode&0o777 || after.File == nil || after.File.Revision != before.File.Revision {
		t.Fatalf("denied batch item changed the file: before=%+v after=%+v", before, after)
	}

	// The same token may still write bytes; only the mode is owner-only.
	w = doBatchWrite(t, s, scoped, batchWriteRequest{Items: []batchWriteItem{
		{Path: "/allowed/f.txt", ExpectedRevision: 1, Data: []byte("new")},
	}})
	if w.Code != http.StatusOK {
		t.Fatalf("scoped batch write without mode status = %d, want 200: %s", w.Code, w.Body.String())
	}

	// And an owner token keeps the documented capability.
	owner := &TenantScope{Backend: b}
	w = doBatchWrite(t, s, owner, batchWriteRequest{Items: []batchWriteItem{
		{Path: "/allowed/f.txt", ExpectedRevision: 2, Data: []byte("owner"), Mode: 0o640, HasMode: true},
	}})
	if w.Code != http.StatusOK {
		t.Fatalf("owner batch write with mode status = %d, want 200: %s", w.Code, w.Body.String())
	}
	after, err = b.StatNodeCtx(ctx, "/allowed/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode&0o777 != 0o640 {
		t.Fatalf("owner mode = %o, want 640", after.Mode&0o777)
	}
}
