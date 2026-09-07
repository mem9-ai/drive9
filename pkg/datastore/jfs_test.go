package datastore

import (
	"context"
	"encoding/json"
	"syscall"
	"testing"
)

func TestExtentMetaCreateUnlinkRecreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, err := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "type": 1, "mode": 0644, "cumask": 0,
		"inode": 2, "proj_path": "/foo.db", "uid": 0, "gid": 0,
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	body, errno, err := s.RunExtentMetaOp(ctx, "mknod", create)
	if err != nil {
		t.Fatal(err)
	}
	if errno != 0 {
		t.Fatalf("mknod errno=%d body=%s", errno, body)
	}
	proj, err := s.GetExtentProjection(ctx, "/foo.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ContentLayout != ContentLayoutExtent || proj.ExtentIno != 2 {
		t.Fatalf("projection = %+v", proj)
	}

	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "proj_path": "/foo.db", "opened": false,
	})
	if _, errno, err = s.RunExtentMetaOp(ctx, "unlink", unlink); err != nil || errno != 0 {
		t.Fatalf("unlink errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/foo.db"); err == nil {
		t.Fatal("projection still present after unlink")
	}

	create2, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "type": 1, "mode": 0644, "cumask": 0,
		"inode": 3, "proj_path": "/foo.db", "uid": 0, "gid": 0,
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err = s.RunExtentMetaOp(ctx, "mknod", create2); err != nil || errno != 0 {
		t.Fatalf("recreate errno=%d err=%v", errno, err)
	}
	proj, err = s.GetExtentProjection(ctx, "/foo.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 3 {
		t.Fatalf("recreate ino = %d, want 3", proj.ExtentIno)
	}

	if _, errno, err = s.RunExtentMetaOp(ctx, "mknod", create2); err != nil {
		t.Fatal(err)
	}
	if errno != int(syscall.EEXIST) {
		t.Fatalf("exclusive recreate errno=%d, want EEXIST", errno)
	}
}

func TestExtentMetaWriteUpdatesLength(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "blob", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/blob",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 1, Size: 4, Off: 0, Len: 4},
	})
	body, errno, err := s.RunExtentMetaOp(ctx, "write", write)
	if err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v body=%s", errno, err, body)
	}
	getattr, _ := json.Marshal(map[string]any{"inode": 2})
	body, errno, err = s.RunExtentMetaOp(ctx, "getattr", getattr)
	if err != nil || errno != 0 {
		t.Fatalf("getattr errno=%d err=%v", errno, err)
	}
	var out struct {
		Attr ExtentAttr `json:"attr"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Attr.Length != 4 {
		t.Fatalf("length=%d, want 4", out.Attr.Length)
	}
}
