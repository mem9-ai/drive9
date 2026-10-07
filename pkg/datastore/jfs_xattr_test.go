package datastore

import (
	"context"
	"encoding/json"
	"strings"
	"syscall"
	"testing"
	"time"
)

type xattrOpResponse struct {
	Errno int      `json:"errno"`
	Value []byte   `json:"value"`
	Names []string `json:"names"`
	Inode uint64   `json:"inode"`
}

func runXattrMetaOp(t *testing.T, s *Store, op string, request any) xattrOpResponse {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	body, errno, err := s.RunExtentMetaOp(context.Background(), op, raw, nil)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	var response xattrOpResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("%s response %q: %v", op, body, err)
	}
	if response.Errno != errno {
		t.Fatalf("%s response errno=%d, return errno=%d", op, response.Errno, errno)
	}
	return response
}

func createXattrTestNode(t *testing.T, s *Store, ino uint64, name string, typ uint8) {
	t.Helper()
	response := runXattrMetaOp(t, s, "mknod", map[string]any{
		"parent": 1, "name": name, "type": typ, "mode": 0o644,
		"inode": ino, "proj_path": "/" + name,
		"attr": ExtentAttr{Typ: typ, Mode: 0o644, Nlink: 1, Parent: 1, Full: true},
	})
	if response.Errno != 0 {
		t.Fatalf("mknod %q errno=%d", name, response.Errno)
	}
}

func TestExtentXattrCRUDAndValidation(t *testing.T) {
	s := newTestStore(t)
	createXattrTestNode(t, s, 2, "file", jfsTypeFile)

	if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
		"inode": 2, "name": "user.empty", "value": []byte{}, "flags": 0,
	}); got.Errno != 0 {
		t.Fatalf("set empty errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 2, "name": "user.empty",
	}); got.Errno != 0 || len(got.Value) != 0 {
		t.Fatalf("get empty = errno %d value %q", got.Errno, got.Value)
	}
	if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
		"inode": 2, "name": "user.empty", "value": []byte("new"), "flags": jfsXattrCreate,
	}); got.Errno != int(syscall.EEXIST) {
		t.Fatalf("XATTR_CREATE existing errno=%d, want EEXIST", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
		"inode": 2, "name": "user.missing", "value": []byte("new"), "flags": jfsXattrReplace,
	}); got.Errno != int(syscall.ENODATA) {
		t.Fatalf("XATTR_REPLACE missing errno=%d, want ENODATA", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
		"inode": 2, "name": "user.empty", "value": []byte("replaced"), "flags": jfsXattrReplace,
	}); got.Errno != 0 {
		t.Fatalf("replace existing errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
		"inode": 2, "name": "user.alpha", "value": []byte("a"), "flags": 0,
	}); got.Errno != 0 {
		t.Fatalf("set second errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "list_xattr", map[string]any{"inode": 2}); got.Errno != 0 || len(got.Names) != 2 || got.Names[0] != "user.alpha" || got.Names[1] != "user.empty" {
		t.Fatalf("list = errno %d names %v", got.Errno, got.Names)
	}

	invalid := []struct {
		name  string
		value []byte
		flags uint32
	}{
		{name: "", value: nil},
		{name: "user.bad\x00name", value: nil},
		{name: strings.Repeat("a", jfsXattrMaxName+1), value: nil},
		{name: "user.large", value: make([]byte, jfsXattrMaxValue+1)},
		{name: "user.flags", value: nil, flags: jfsXattrCreate | jfsXattrReplace},
		{name: "user.flags", value: nil, flags: 4},
	}
	for _, test := range invalid {
		if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
			"inode": 2, "name": test.name, "value": test.value, "flags": test.flags,
		}); got.Errno != int(syscall.EINVAL) {
			t.Fatalf("invalid name=%q value=%d flags=%d errno=%d, want EINVAL", test.name, len(test.value), test.flags, got.Errno)
		}
	}

	for _, op := range []string{"get_xattr", "list_xattr", "set_xattr", "remove_xattr"} {
		request := map[string]any{"inode": 999, "name": "user.test", "value": []byte("x"), "flags": 0}
		if got := runXattrMetaOp(t, s, op, request); got.Errno != int(syscall.ENOENT) {
			t.Fatalf("%s unknown inode errno=%d, want ENOENT", op, got.Errno)
		}
	}
	if got := runXattrMetaOp(t, s, "remove_xattr", map[string]any{
		"inode": 2, "name": "user.missing",
	}); got.Errno != int(syscall.ENODATA) {
		t.Fatalf("remove missing errno=%d, want ENODATA", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "remove_xattr", map[string]any{
		"inode": 2, "name": "user.empty",
	}); got.Errno != 0 {
		t.Fatalf("remove existing errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 2, "name": "user.empty",
	}); got.Errno != int(syscall.ENODATA) {
		t.Fatalf("get removed errno=%d, want ENODATA", got.Errno)
	}
}

func TestExtentXattrFollowsInodeAcrossLinkRenameAndFinalUnlink(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	createXattrTestNode(t, s, 9, "src", jfsTypeFile)
	if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
		"inode": 9, "name": "user.marker", "value": []byte("durable"), "flags": 0,
	}); got.Errno != 0 {
		t.Fatalf("set errno=%d", got.Errno)
	}
	if err := s.LinkFileNode(ctx, "/src", "/alias", "/", "alias", "xattr-link", time.Now()); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	alias := runXattrMetaOp(t, s, "lookup", map[string]any{"parent": 1, "name": "alias"})
	if alias.Errno != 0 || alias.Inode != 9 {
		t.Fatalf("alias lookup = errno %d inode %d, want 0/9", alias.Errno, alias.Inode)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": alias.Inode, "name": "user.marker",
	}); got.Errno != 0 || string(got.Value) != "durable" {
		t.Fatalf("hardlink xattr = errno %d value %q", got.Errno, got.Value)
	}

	rename := runXattrMetaOp(t, s, "rename", map[string]any{
		"src_parent": 1, "src_name": "src", "dst_parent": 1, "dst_name": "renamed",
		"src_path": "/src", "dst_path": "/renamed",
	})
	if rename.Errno != 0 {
		t.Fatalf("rename errno=%d", rename.Errno)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 9, "name": "user.marker",
	}); got.Errno != 0 || string(got.Value) != "durable" {
		t.Fatalf("renamed xattr = errno %d value %q", got.Errno, got.Value)
	}

	if got := runXattrMetaOp(t, s, "unlink", map[string]any{
		"parent": 1, "name": "alias", "proj_path": "/alias",
	}); got.Errno != 0 {
		t.Fatalf("unlink alias errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 9, "name": "user.marker",
	}); got.Errno != 0 || string(got.Value) != "durable" {
		t.Fatalf("last-link xattr = errno %d value %q", got.Errno, got.Value)
	}
	if got := runXattrMetaOp(t, s, "unlink", map[string]any{
		"parent": 1, "name": "renamed", "proj_path": "/renamed",
	}); got.Errno != 0 {
		t.Fatalf("unlink final errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 9, "name": "user.marker",
	}); got.Errno != int(syscall.ENOENT) {
		t.Fatalf("get reclaimed inode errno=%d, want ENOENT", got.Errno)
	}
	var rows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_xattr WHERE inode = 9`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("xattr rows after final unlink=%d, want 0", rows)
	}

	createXattrTestNode(t, s, 10, "renamed", jfsTypeFile)
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 10, "name": "user.marker",
	}); got.Errno != int(syscall.ENODATA) {
		t.Fatalf("recreated path inherited xattr: errno=%d value=%q", got.Errno, got.Value)
	}
}

func TestExtentXattrRenameReplaceCleansDestinationOnly(t *testing.T) {
	s := newTestStore(t)
	createXattrTestNode(t, s, 2, "src", jfsTypeFile)
	createXattrTestNode(t, s, 3, "dst", jfsTypeFile)
	for ino, value := range map[uint64]string{2: "source", 3: "destination"} {
		if got := runXattrMetaOp(t, s, "set_xattr", map[string]any{
			"inode": ino, "name": "user.marker", "value": []byte(value), "flags": 0,
		}); got.Errno != 0 {
			t.Fatalf("set inode %d errno=%d", ino, got.Errno)
		}
	}
	if got := runXattrMetaOp(t, s, "rename", map[string]any{
		"src_parent": 1, "src_name": "src", "dst_parent": 1, "dst_name": "dst",
		"src_path": "/src", "dst_path": "/dst",
	}); got.Errno != 0 {
		t.Fatalf("rename replace errno=%d", got.Errno)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 2, "name": "user.marker",
	}); got.Errno != 0 || string(got.Value) != "source" {
		t.Fatalf("source xattr = errno %d value %q", got.Errno, got.Value)
	}
	if got := runXattrMetaOp(t, s, "get_xattr", map[string]any{
		"inode": 3, "name": "user.marker",
	}); got.Errno != int(syscall.ENOENT) {
		t.Fatalf("replaced xattr errno=%d, want ENOENT", got.Errno)
	}
}

func TestExtentXattrDatabaseFailureIsExplicitAndAtomic(t *testing.T) {
	s := newTestStore(t)
	createXattrTestNode(t, s, 2, "file", jfsTypeFile)
	if _, err := s.DB().Exec(`RENAME TABLE jfs_xattr TO jfs_xattr_unavailable`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.DB().Exec(`RENAME TABLE jfs_xattr_unavailable TO jfs_xattr`); err != nil {
			t.Errorf("restore jfs_xattr: %v", err)
		}
	})

	raw, err := json.Marshal(map[string]any{
		"inode": 2, "name": "user.marker", "value": []byte("must-not-commit"), "flags": 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, errno, err := s.RunExtentMetaOp(context.Background(), "set_xattr", raw, nil)
	if err != nil || errno != int(syscall.EIO) {
		t.Fatalf("database failure = body %q errno %d err %v, want explicit EIO response", body, errno, err)
	}
	var response xattrOpResponse
	if err := json.Unmarshal(body, &response); err != nil || response.Errno != int(syscall.EIO) {
		t.Fatalf("database failure body=%q decode=%v errno=%d", body, err, response.Errno)
	}
	var rows int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_xattr_unavailable WHERE inode = 2`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("failed xattr transaction left %d rows", rows)
	}
}
