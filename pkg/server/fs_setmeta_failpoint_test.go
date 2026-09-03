//go:build failpoint

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

// TestSetMetaDeleteRecreateRaceReturnsConflict pins the public API behavior of
// the dentry fence: a delete+recreate between path resolution and the
// metadata transaction yields HTTP 409 (not 500), and the replacement inode
// keeps its own metadata.
func TestSetMetaDeleteRecreateRaceReturnsConflict(t *testing.T) {
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
	ts := httptest.NewServer(s)
	defer ts.Close()

	ctx := context.Background()
	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/race.txt", []byte("old"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1,
		map[string]string{"owner": "old"}, "old description"); err != nil {
		t.Fatal(err)
	}

	var fired atomic.Bool
	if err := failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/backend/setmetaAfterResolve", func(path string) {
		if path != "/race.txt" || !fired.CompareAndSwap(false, true) {
			return
		}
		if _, err := store.DeleteFileWithRefCheck(ctx, "/race.txt"); err != nil {
			t.Errorf("race delete: %v", err)
			return
		}
		now := time.Now().UTC()
		if err := store.InsertFile(ctx, &datastore.File{
			FileID: "f-race-new", StorageType: datastore.StorageDB9, StorageRef: "inline",
			Revision: 1, Status: datastore.StatusConfirmed, Description: "replacement description",
			CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			t.Errorf("race recreate file: %v", err)
			return
		}
		if err := store.InsertNode(ctx, &datastore.FileNode{
			NodeID: "n-race-new", Path: "/race.txt", ParentPath: "/", Name: "race.txt",
			FileID: "f-race-new", CreatedAt: now,
		}); err != nil {
			t.Errorf("race recreate node: %v", err)
		}
	}); err != nil {
		t.Fatalf("enable failpoint: %v", err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/mem9-ai/drive9/pkg/backend/setmetaAfterResolve")
	})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/fs/race.txt?setmeta=1",
		strings.NewReader(`{"tags":{"owner":"attacker"},"description":"attacker description"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("setmeta after delete+recreate: status = %d, want 409", resp.StatusCode)
	}
	if !fired.Load() {
		t.Fatal("failpoint did not fire")
	}

	tags, err := store.GetFileTags(ctx, "f-race-new")
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 0 {
		t.Fatalf("replacement inode tags = %+v, want none", tags)
	}
	sem, err := store.GetSemantic(ctx, "f-race-new")
	if err != nil {
		t.Fatal(err)
	}
	if sem.Description != "replacement description" {
		t.Fatalf("replacement description = %q, want unchanged", sem.Description)
	}
}
