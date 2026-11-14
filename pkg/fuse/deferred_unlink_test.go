package fuse

import (
	"testing"
	"time"
)

func TestDeferredUnlinkReadOnlyAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*FS, uint64)
	}{
		{"last-close", func(f *FS, ino uint64) { f.maybeRunDeferredUnlink(ino) }},
		{"same-name", func(f *FS, _ uint64) { f.flushDeferredUnlink("/file") }},
		{"directory", func(f *FS, _ uint64) { f.flushDeferredUnlinksUnder("/") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, readOnly := range []bool{false, true} {
				core := newMemCore()
				core.writeAt("/file", 0, []byte("kept"))
				f := newTestFS(t, core)
				f.validationReadOnly = readOnly
				ino := f.EnsureInodeForTest("/file", false, 4, time.Now())
				// Validation cannot enqueue this through Unlink. Inject a stale
				// entry to ensure internal callers still fail before local effects.
				f.deferredUnlinks[ino] = "/file"
				tc.run(f, ino)
				entry, exists := core.get("/file")
				if exists != readOnly {
					t.Fatalf("readOnly=%v file exists=%v", readOnly, exists)
				}
				if readOnly && (string(entry.data) != "kept" || !f.isDeferredUnlink("/file")) {
					t.Fatal("read-only rejection changed contents or pending state")
				}
				if !readOnly && f.isDeferredUnlink("/file") {
					t.Fatal("writable delete remained pending")
				}
			}
		})
	}
}

func TestDeferredUnlinkPreservesReboundName(t *testing.T) {
	for _, flush := range []bool{false, true} {
		core := newMemCore()
		core.writeAt("/file", 0, []byte("replacement"))
		f := newTestFS(t, core)
		ino := f.EnsureInodeForTest("/file", false, 11, time.Now())
		old := ino + 1
		f.deferredUnlinks[old] = "/file"
		if flush {
			f.flushDeferredUnlink("/file")
		} else {
			f.maybeRunDeferredUnlink(old)
		}
		if entry, ok := core.get("/file"); !ok || string(entry.data) != "replacement" {
			t.Fatal("old deferred unlink deleted the replacement")
		}
	}
}
