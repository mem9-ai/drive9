package fuse

import (
	"os"
	"testing"
)

func TestParseOwner(t *testing.T) {
	for raw, want := range map[string]Owner{"1000:1000": {1000, 1000}, "1000": {1000, 1000}, "0:5": {0, 5}} {
		got, err := ParseOwner(raw)
		if err != nil || *got != want {
			t.Errorf("ParseOwner(%q) = %v, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"", "agent", "1000:x", "-1:1", "1:2:3"} {
		if _, err := ParseOwner(raw); err == nil {
			t.Errorf("ParseOwner(%q) accepted", raw)
		}
	}
}

func TestDefaultOwnerAppliesOnlyToFilesWithoutStoredOwner(t *testing.T) {
	fs := NewDat9FS(nil, &MountOptions{DefaultOwner: &Owner{UID: 1000, GID: 1001}})
	if owner := fs.entryOwner(&InodeEntry{}); owner.Uid != 1000 || owner.Gid != 1001 {
		t.Fatalf("default owner = %+v", owner)
	}
	if owner := fs.entryOwner(&InodeEntry{Uid: 501, HasUID: true, Gid: 20, HasGID: true}); owner.Uid != 501 || owner.Gid != 20 {
		t.Fatalf("stored owner = %+v", owner)
	}
	plain := NewDat9FS(nil, &MountOptions{})
	if owner := plain.entryOwner(&InodeEntry{}); owner.Uid != uint32(os.Getuid()) {
		t.Fatalf("without --owner = %+v", owner)
	}
}
