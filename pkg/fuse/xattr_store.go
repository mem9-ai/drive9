package fuse

import (
	"strings"
	"sync"
	"syscall"
)

// XATTR_CREATE and XATTR_REPLACE flag values from Linux's setxattr(2).
const (
	xattrCreateFlag  = 1 // XATTR_CREATE
	xattrReplaceFlag = 2 // XATTR_REPLACE
)

// XAttrStore provides in-memory extended attribute (xattr) storage for a
// FUSE mount session. Xattrs are scoped to file paths and are not persisted
// to the backend — they exist only for the lifetime of the mount.
//
// Note: xattrs are keyed by path, not by inode. This means hardlinks (multiple
// paths to the same inode) get independent xattr sets. This is a deliberate
// simplification — the primary use case is macOS Finder/Spotlight compatibility
// and Linux POSIX xattr testing, where per-path semantics are sufficient.
//
// On Darwin (macOS), system-generated xattr prefixes (com.apple.*, system.*)
// are NOT filtered by this store. The FUSE kernel layer handles those before
// they reach the handler. If explicit filtering becomes needed, it should be
// added in the GetXAttr/ListXAttr handlers, not in the store.
type XAttrStore struct {
	mu   sync.RWMutex
	data map[string]map[string][]byte // path -> attrName -> value
}

// NewXAttrStore creates a new empty XAttrStore.
func NewXAttrStore() *XAttrStore {
	return &XAttrStore{
		data: make(map[string]map[string][]byte),
	}
}

// SetWithFlags stores, creates, or replaces the value of attr for the given
// path, honoring the XATTR_CREATE and XATTR_REPLACE flags from setxattr(2):
//   - flags == 0 (neither): set unconditionally (create or replace).
//   - XATTR_CREATE: fail with EEXIST if the attr already exists.
//   - XATTR_REPLACE: fail with ENODATA if the attr does not exist.
func (s *XAttrStore) SetWithFlags(path, attr string, value []byte, flags uint32) syscall.Errno {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[path] == nil {
		s.data[path] = make(map[string][]byte)
	}
	_, exists := s.data[path][attr]
	if flags&xattrCreateFlag != 0 && exists {
		return syscall.EEXIST
	}
	if flags&xattrReplaceFlag != 0 && !exists {
		return syscall.ENODATA
	}
	s.data[path][attr] = append([]byte(nil), value...)
	return 0
}

// Set stores or replaces the value of attr for the given path (no flags).
func (s *XAttrStore) Set(path, attr string, value []byte) {
	_ = s.SetWithFlags(path, attr, value, 0)
}

// Get retrieves the value of attr for the given path.
// Returns the value and true if the attribute exists, or nil and false otherwise.
func (s *XAttrStore) Get(path, attr string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	attrs, ok := s.data[path]
	if !ok {
		return nil, false
	}
	val, ok := attrs[attr]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), val...), true
}

// List returns the names of all xattrs for the given path.
func (s *XAttrStore) List(path string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	attrs, ok := s.data[path]
	if !ok {
		return nil
	}
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	return names
}

// Remove deletes the attr for the given path.
// Returns true if the attribute existed and was removed.
func (s *XAttrStore) Remove(path, attr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	attrs, ok := s.data[path]
	if !ok {
		return false
	}
	if _, ok := attrs[attr]; !ok {
		return false
	}
	delete(attrs, attr)
	if len(attrs) == 0 {
		delete(s.data, path)
	}
	return true
}

// RemoveAll deletes all xattrs for the given path and any paths underneath it
// (i.e., for a directory path "/foo", also clears "/foo/bar", "/foo/baz/qux", etc.).
// This should be called on Unlink (file) and Rmdir (directory) to clean up.
func (s *XAttrStore) RemoveAll(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, path)
	// Clean up any child paths (for rmdir of a directory with xattr'd children).
	prefix := path + "/"
	for p := range s.data {
		if strings.HasPrefix(p, prefix) {
			delete(s.data, p)
		}
	}
}

// Rename moves all xattrs from oldPath to newPath, and migrates any child-path
// xattrs for directory renames (e.g. "/old/" → "/new/" also moves "/old/sub").
// This should be called on Rename to preserve xattrs across moves.
func (s *XAttrStore) Rename(oldPath, newPath string) {
	if oldPath == newPath {
		return // no-op rename; avoid deleting the entry we just assigned
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Move the exact path.
	if attrs, ok := s.data[oldPath]; ok {
		s.data[newPath] = attrs
		delete(s.data, oldPath)
	}
	// Move child paths (directory rename).
	oldPrefix := oldPath + "/"
	newPrefix := newPath + "/"
	for p := range s.data {
		if strings.HasPrefix(p, oldPrefix) {
			child := newPrefix + strings.TrimPrefix(p, oldPrefix)
			s.data[child] = s.data[p]
			delete(s.data, p)
		}
	}
}