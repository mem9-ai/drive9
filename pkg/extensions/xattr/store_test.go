package xattr

import (
	"syscall"
	"testing"
)

func TestStoreFlagsAndLifecycle(t *testing.T) {
	s := NewXAttrStore()
	if errno := s.SetWithFlags("/f", "user.k", []byte("v1"), xattrCreateFlag); errno != 0 {
		t.Fatalf("create: %v", errno)
	}
	if errno := s.SetWithFlags("/f", "user.k", []byte("v2"), xattrCreateFlag); errno != syscall.EEXIST {
		t.Fatalf("re-create = %v, want EEXIST", errno)
	}
	if errno := s.SetWithFlags("/f", "user.none", []byte("v"), xattrReplaceFlag); errno != syscall.ENODATA {
		t.Fatalf("replace-missing = %v, want ENODATA", errno)
	}
	if errno := s.SetWithFlags("/f", "user.k", []byte("v2"), xattrReplaceFlag); errno != 0 {
		t.Fatalf("replace: %v", errno)
	}
	if v, ok := s.Get("/f", "user.k"); !ok || string(v) != "v2" {
		t.Fatalf("get = %q %v", v, ok)
	}
	names := s.List("/f")
	if len(names) != 1 || names[0] != "user.k" {
		t.Fatalf("list = %v", names)
	}
	s.Rename("/f", "/g")
	if _, ok := s.Get("/f", "user.k"); ok {
		t.Fatal("rename kept old path")
	}
	if _, ok := s.Get("/g", "user.k"); !ok {
		t.Fatal("rename lost attr")
	}
	s.RemoveAll("/g")
	if names := s.List("/g"); len(names) != 0 {
		t.Fatal("remove-all left attrs")
	}
}
