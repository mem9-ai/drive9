package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func setMetaRequest(t *testing.T, ts *httptest.Server, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs"+path+"?setmeta=1", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

type setMetaStatResponse struct {
	Revision    int64             `json:"revision"`
	Description string            `json:"description"`
	Tags        map[string]string `json:"tags"`
}

func statMetadata(t *testing.T, ts *httptest.Server, path string) setMetaStatResponse {
	t.Helper()
	resp, err := http.Get(ts.URL + "/v1/fs" + path + "?stat=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat metadata: %d", resp.StatusCode)
	}
	var out setMetaStatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	return out
}

func TestSetMetaUpdateTagsAndDescription(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	writeReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/meta.txt", strings.NewReader("hello metadata"))
	writeReq.Header.Add("X-Dat9-Tag", "owner=alice")
	writeReq.Header.Add("X-Dat9-Description", "initial description")
	writeResp, err := http.DefaultClient.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusOK {
		t.Fatalf("write: %d", writeResp.StatusCode)
	}

	before := statMetadata(t, ts, "/meta.txt")
	if before.Tags["owner"] != "alice" || before.Description != "initial description" {
		t.Fatalf("unexpected initial metadata: %+v", before)
	}

	// Replace tags only; description must stay unchanged.
	resp := setMetaRequest(t, ts, "/meta.txt", `{"tags":{"owner":"bob","env":"prod"}}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setmeta tags: %d", resp.StatusCode)
	}
	after := statMetadata(t, ts, "/meta.txt")
	if len(after.Tags) != 2 || after.Tags["owner"] != "bob" || after.Tags["env"] != "prod" {
		t.Fatalf("tags after setmeta: %+v", after.Tags)
	}
	if after.Description != "initial description" {
		t.Fatalf("description changed unexpectedly: %q", after.Description)
	}
	if after.Revision != before.Revision {
		t.Fatalf("revision bumped by setmeta: %d -> %d", before.Revision, after.Revision)
	}

	// Update description only; tags must stay unchanged.
	resp = setMetaRequest(t, ts, "/meta.txt", `{"description":"updated description"}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setmeta description: %d", resp.StatusCode)
	}
	after = statMetadata(t, ts, "/meta.txt")
	if after.Description != "updated description" {
		t.Fatalf("description after setmeta: %q", after.Description)
	}
	if len(after.Tags) != 2 || after.Tags["owner"] != "bob" {
		t.Fatalf("tags changed unexpectedly: %+v", after.Tags)
	}

	// Clear both.
	resp = setMetaRequest(t, ts, "/meta.txt", `{"tags":{},"description":""}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setmeta clear: %d", resp.StatusCode)
	}
	after = statMetadata(t, ts, "/meta.txt")
	if len(after.Tags) != 0 {
		t.Fatalf("tags after clear: %+v", after.Tags)
	}
	if after.Description != "" {
		t.Fatalf("description after clear: %q", after.Description)
	}
}

func TestSetMetaErrors(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	writeReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/exists.txt", strings.NewReader("data"))
	writeResp, err := http.DefaultClient.Do(writeReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = writeResp.Body.Close()
	if writeResp.StatusCode != http.StatusOK {
		t.Fatalf("write: %d", writeResp.StatusCode)
	}

	mkdirReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/dir?mkdir=1", nil)
	mkdirResp, err := http.DefaultClient.Do(mkdirReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = mkdirResp.Body.Close()
	if mkdirResp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir: %d", mkdirResp.StatusCode)
	}

	cases := []struct {
		name string
		path string
		body string
		want int
	}{
		{"not found", "/missing.txt", `{"tags":{"a":"b"}}`, http.StatusNotFound},
		{"empty update", "/exists.txt", `{}`, http.StatusBadRequest},
		{"invalid tag", "/exists.txt", `{"tags":{"a=b":"v"}}`, http.StatusBadRequest},
		{"invalid body", "/exists.txt", `not-json`, http.StatusBadRequest},
		{"directory", "/dir", `{"tags":{"a":"b"}}`, http.StatusBadRequest},
		{"description too long", "/exists.txt", `{"description":"` + strings.Repeat("x", backend.MaxDescriptionLen+1) + `"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := setMetaRequest(t, ts, tc.path, tc.body)
			_ = resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("setmeta %s: status = %d, want %d", tc.name, resp.StatusCode, tc.want)
			}
		})
	}

	t.Run("body too large", func(t *testing.T) {
		resp := setMetaRequest(t, ts, "/exists.txt", `{"description":"`+strings.Repeat("x", maxSetMetaBodyBytes)+`"}`)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("setmeta oversized body: status = %d, want 413", resp.StatusCode)
		}
	})
}

// Scoped tokens may setmeta only within their write-scoped zones.
func TestSetMetaScopedTokenZones(t *testing.T) {
	s3Dir, err := os.MkdirTemp("", "dat9-srv-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })
	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testtidb.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })
	s3c, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}
	s := NewWithConfig(Config{Backend: b})
	t.Cleanup(func() { s.Close() })

	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(context.Background(), "/allowed/f.txt", []byte("data"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(context.Background(), "/denied/f.txt", []byte("data"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatal(err)
	}

	scope := &TenantScope{
		IsScoped: true,
		Backend:  b,
		FSScopes: []FSScope{{Prefix: "/allowed", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}},
	}
	doSetMeta := func(path string) int {
		r := httptest.NewRequest(http.MethodPost, "/v1/fs"+path+"?setmeta=1", strings.NewReader(`{"tags":{"a":"b"}}`))
		r = r.WithContext(withScope(r.Context(), scope))
		w := httptest.NewRecorder()
		s.handleSetMeta(w, r, path)
		return w.Result().StatusCode
	}
	if got := doSetMeta("/allowed/f.txt"); got != http.StatusOK {
		t.Fatalf("in-zone setmeta status = %d, want 200", got)
	}
	if got := doSetMeta("/denied/f.txt"); got != http.StatusForbidden {
		t.Fatalf("out-of-zone setmeta status = %d, want 403", got)
	}
}
