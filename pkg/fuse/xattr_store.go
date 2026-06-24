package fuse

import (
	"sync"
)

// XAttrStore provides in-memory extended attribute (xattr) storage for a
// FUSE mount session. Xattrs are scoped to file paths and are not persisted
// to the backend — they exist only for the lifetime of the mount.
//
// On Darwin (macOS), the store still works, but callers should filter out
// system-generated xattr prefixes (com.apple.*, system.*) to avoid
// polluting results with Finder/Spotlight metadata.
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

// Set stores or replaces the value of attr for the given path.
func (s *XAttrStore) Set(path, attr string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[path] == nil {
		s.data[path] = make(map[string][]byte)
	}
	s.data[path][attr] = append([]byte(nil), value...)
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

// RemoveAll deletes all xattrs for the given path.
// This should be called on Unlink/Rmdir to clean up xattr state.
func (s *XAttrStore) RemoveAll(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, path)
}