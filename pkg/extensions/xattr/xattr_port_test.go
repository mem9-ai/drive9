// v1 dat9fs_test.go xattr rows, direct ports against the extension
// store (the facade's Get/Set/List/Remove translate 1:1 onto it; the
// facade-level namespace gating is covered by pkg/fuse
// TestErrnoAlignmentXAttrNamespaces).
package xattr

import (
	"strings"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newTestExt(t *testing.T) *Xattr {
	t.Helper()
	x, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	return x
}

// get/list/remove wrap the store the way Do's OpGetXattr/List/Remove
// branches do (the facade maps these onto the same calls).
func (x *Xattr) get(path, name string) ([]byte, error) {
	if v, hit := x.store.Get(path, name); hit {
		return v, nil
	}
	return nil, fs.StatusErrorOf(gofuse.ENOATTR)
}

// v1 TestXAttr_GetMissingReturnsENOATTR.
func TestPortXAttrGetMissingReturnsENOATTR(t *testing.T) {
	x := newTestExt(t)
	if _, err := x.get("/test.txt", "user.test"); err == nil || !isENOATTR(err) {
		t.Fatalf("Get missing err = %v, want ENOATTR", err)
	}
}

// v1 TestXAttr_SetGetRoundTrip.
func TestPortXAttrSetGetRoundTrip(t *testing.T) {
	x := newTestExt(t)
	if errno := x.store.SetWithFlags("/test.txt", "user.test", []byte("hello"), 0); errno != 0 {
		t.Fatal(errno)
	}
	data, err := x.get("/test.txt", "user.test")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("value = %q, want hello", data)
	}
}

// v1 TestXAttr_ListReturnsSetAttrs.
func TestPortXAttrListReturnsSetAttrs(t *testing.T) {
	x := newTestExt(t)
	_ = x.store.SetWithFlags("/test.txt", "user.foo", []byte("1"), 0)
	_ = x.store.SetWithFlags("/test.txt", "user.bar", []byte("2"), 0)
	names := x.store.List("/test.txt")
	joined := strings.Join(names, ",")
	if !strings.Contains(joined, "user.foo") || !strings.Contains(joined, "user.bar") {
		t.Fatalf("names = %v, want both user.foo and user.bar", names)
	}
}

// v1 TestXAttr_RemoveWorks / RemoveMissingReturnsENOATTR.
func TestPortXAttrRemove(t *testing.T) {
	x := newTestExt(t)
	_ = x.store.SetWithFlags("/test.txt", "user.test", []byte("val"), 0)
	if !x.store.Remove("/test.txt", "user.test") {
		t.Fatal("Remove failed")
	}
	if _, err := x.get("/test.txt", "user.test"); err == nil || !isENOATTR(err) {
		t.Fatalf("Get after remove err = %v, want ENOATTR", err)
	}
	if x.store.Remove("/test.txt", "user.test") {
		t.Fatal("Remove missing succeeded")
	}
}

// v1 TestXAttr_RenameMigratesXattrs / RenameMigratesSubtree /
// RemoveAllCleansChildren.
func TestPortXAttrRenameAndRemoveAll(t *testing.T) {
	x := newTestExt(t)
	_ = x.store.SetWithFlags("/old.txt", "user.test", []byte("hello"), 0)
	x.store.Rename("/old.txt", "/new.txt")
	if _, err := x.get("/old.txt", "user.test"); err == nil || !isENOATTR(err) {
		t.Fatalf("old path err = %v, want ENOATTR", err)
	}
	if data, err := x.get("/new.txt", "user.test"); err != nil || string(data) != "hello" {
		t.Fatalf("new path = %q err=%v", data, err)
	}

	// Subtree rename + RemoveAll.
	_ = x.store.SetWithFlags("/dir/a.txt", "user.k", []byte("v"), 0)
	x.store.Rename("/dir", "/moved")
	if _, err := x.get("/moved/a.txt", "user.k"); err != nil {
		t.Fatalf("subtree rename lost xattr: %v", err)
	}
	x.store.RemoveAll("/moved")
	if _, err := x.get("/moved/a.txt", "user.k"); err == nil || !isENOATTR(err) {
		t.Fatalf("RemoveAll left xattr: %v", err)
	}
}

// v1 TestXAttr_SetXAttrCreateFlagFailsIfExists /
// SetXAttrReplaceFlagFailsIfMissing.
func TestPortXAttrFlags(t *testing.T) {
	x := newTestExt(t)
	if errno := x.store.SetWithFlags("/test.txt", "user.test", []byte("a"), 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno := x.store.SetWithFlags("/test.txt", "user.test", []byte("b"), 1); errno == 0 {
		t.Fatal("XATTR_CREATE over existing attr accepted")
	}
	if errno := x.store.SetWithFlags("/test.txt", "user.missing", []byte("b"), 2); errno == 0 {
		t.Fatal("XATTR_REPLACE over missing attr accepted")
	}
}

func isENOATTR(err error) bool {
	return err != nil && fs.StatusOf(err) == gofuse.ENOATTR
}
