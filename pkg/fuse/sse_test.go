package fuse

import (
	"path"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestSSEWatcherResetPreservesInodes(t *testing.T) {
	// Set up a Dat9FS with some cached inodes (no real server needed).
	opts := &MountOptions{
		CacheSize: 1 << 20,
		DirTTL:    5 * time.Second,
		AttrTTL:   60 * time.Second,
		EntryTTL:  60 * time.Second,
	}
	opts.setDefaults()
	fs := &Dat9FS{
		inodes:    NewInodeToPath(),
		readCache: NewReadCache(opts.CacheSize, 0),
		dirCache:  NewDirCache(opts.DirTTL),
	}

	// Simulate some looked-up inodes.
	ino1 := fs.inodes.Lookup("/project", true, 0, time.Now())
	ino2 := fs.inodes.Lookup("/project/foo.txt", false, 100, time.Now())
	ino3 := fs.inodes.Lookup("/other/bar.txt", false, 200, time.Now())

	// Put some data in caches.
	fs.readCache.Put("/project/foo.txt", []byte("hello"), 1)
	fs.dirCache.Put("/project", []CachedFileInfo{{Name: "foo.txt", Size: 100}})

	// Create an SSE watcher (no real connection needed for this test).
	w := &SSEWatcher{
		fs:    fs,
		actor: "test-actor",
	}

	// Trigger reset.
	w.handleReset()

	// Verify: InodeToPath entries are still alive.
	if _, ok := fs.inodes.GetPath(ino1); !ok {
		t.Error("inode for /project was lost after reset")
	}
	if _, ok := fs.inodes.GetPath(ino2); !ok {
		t.Error("inode for /project/foo.txt was lost after reset")
	}
	if _, ok := fs.inodes.GetPath(ino3); !ok {
		t.Error("inode for /other/bar.txt was lost after reset")
	}

	// Verify: caches are cleared.
	if _, ok := fs.readCache.Get("/project/foo.txt", 1); ok {
		t.Error("readCache should be empty after reset")
	}
	if _, ok := fs.dirCache.Get("/project"); ok {
		t.Error("dirCache should be empty after reset")
	}
}

func TestSSEWatcherHandleChangeInvalidatesCache(t *testing.T) {
	opts := &MountOptions{
		CacheSize: 1 << 20,
		DirTTL:    5 * time.Second,
	}
	opts.setDefaults()
	fs := &Dat9FS{
		inodes:    NewInodeToPath(),
		readCache: NewReadCache(opts.CacheSize, 0),
		dirCache:  NewDirCache(opts.DirTTL),
	}

	fs.inodes.Lookup("/docs/readme.md", false, 50, time.Now())
	fs.readCache.Put("/docs/readme.md", []byte("old content"), 1)
	fs.dirCache.Put("/docs", []CachedFileInfo{{Name: "readme.md", Size: 50}})

	w := &SSEWatcher{fs: fs, actor: "my-actor"}

	// A change from a different actor should invalidate caches.
	w.handleChange(&client.ChangeEvent{
		Seq:   1,
		Path:  "/docs/readme.md",
		Op:    "write",
		Actor: "other-actor",
	})

	if _, ok := fs.readCache.Get("/docs/readme.md", 1); ok {
		t.Error("readCache entry should be invalidated after change")
	}
	if _, ok := fs.dirCache.Get("/docs"); ok {
		t.Error("dirCache entry for parent dir should be invalidated after change")
	}

	// Inode should still be there.
	if _, ok := fs.inodes.GetInode("/docs/readme.md"); !ok {
		t.Error("inode should survive a change event")
	}
}

func TestSSEWatcherSelfFilterSkipsOwnEvents(t *testing.T) {
	opts := &MountOptions{
		CacheSize: 1 << 20,
		DirTTL:    5 * time.Second,
	}
	opts.setDefaults()
	fs := &Dat9FS{
		inodes:    NewInodeToPath(),
		readCache: NewReadCache(opts.CacheSize, 0),
		dirCache:  NewDirCache(opts.DirTTL),
	}

	fs.readCache.Put("/test.txt", []byte("data"), 1)

	w := &SSEWatcher{fs: fs, actor: "my-actor"}

	// An event from our own actor should be skipped.
	w.handleEvent(
		&client.ChangeEvent{Seq: 1, Path: "/test.txt", Op: "write", Actor: "my-actor"},
		nil,
	)

	// Cache should NOT be invalidated because we skip our own events.
	if _, ok := fs.readCache.Get("/test.txt", 1); !ok {
		t.Error("readCache should NOT be invalidated for own actor's events")
	}
}

func TestSSEWatcherHandleEventReset(t *testing.T) {
	opts := &MountOptions{
		CacheSize: 1 << 20,
		DirTTL:    5 * time.Second,
	}
	opts.setDefaults()
	fs := &Dat9FS{
		inodes:    NewInodeToPath(),
		readCache: NewReadCache(opts.CacheSize, 0),
		dirCache:  NewDirCache(opts.DirTTL),
	}

	fs.readCache.Put("/test.txt", []byte("data"), 1)
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "test.txt", Size: 4}})

	w := &SSEWatcher{fs: fs, actor: "my-actor"}

	// A reset event should clear caches.
	w.handleEvent(nil, &client.ResetEvent{Seq: 5, Reason: "structural_change"})

	if _, ok := fs.readCache.Get("/test.txt", 1); ok {
		t.Error("readCache should be cleared after reset")
	}
	if _, ok := fs.dirCache.Get("/"); ok {
		t.Error("dirCache should be cleared after reset")
	}
}

func TestSSEWatcherHeartbeatDoesNotReset(t *testing.T) {
	opts := &MountOptions{
		CacheSize: 1 << 20,
		DirTTL:    5 * time.Second,
	}
	opts.setDefaults()
	fs := &Dat9FS{
		inodes:    NewInodeToPath(),
		readCache: NewReadCache(opts.CacheSize, 0),
		dirCache:  NewDirCache(opts.DirTTL),
	}

	fs.readCache.Put("/test.txt", []byte("data"), 1)

	w := &SSEWatcher{fs: fs, actor: "my-actor"}

	// A heartbeat (reset with empty reason) should NOT clear caches.
	w.handleEvent(nil, &client.ResetEvent{Seq: 5, Reason: ""})

	if _, ok := fs.readCache.Get("/test.txt", 1); !ok {
		t.Error("readCache should NOT be cleared after heartbeat")
	}
}

func TestParentDir(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"/foo/bar.txt", "/foo"},
		{"/bar.txt", "/"},
		{"/a/b/c", "/a/b"},
		{"/", "/"},
	}
	for _, tt := range tests {
		got := path.Dir(tt.input)
		if got == "." {
			got = "/"
		}
		if got != tt.want {
			t.Errorf("parentDir(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
