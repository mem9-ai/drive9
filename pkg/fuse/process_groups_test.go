package fuse

import (
	"os"
	"testing"
)

func TestProcStatusHasGroup(t *testing.T) {
	status := []byte("Name:\tdrive9\nGroups:\t10 20 30\n")
	if !procStatusHasGroup(status, 20) {
		t.Fatal("procStatusHasGroup returned false for a listed group")
	}
	if procStatusHasGroup(status, 2) {
		t.Fatal("procStatusHasGroup returned true for a partial group match")
	}
}

func TestProcessHasSupplementaryGroupConservativeFallbackWhenProcUnavailable(t *testing.T) {
	oldReadProcessStatusFile := readProcessStatusFile
	readProcessStatusFile = func(string) ([]byte, error) {
		return nil, os.ErrNotExist
	}
	t.Cleanup(func() {
		readProcessStatusFile = oldReadProcessStatusFile
	})

	if processHasSupplementaryGroup(uint32(os.Getpid()+1), 1) {
		t.Fatal("non-current process should not be granted supplementary group access without process credentials")
	}

	groups, err := os.Getgroups()
	if err != nil {
		t.Fatalf("get groups: %v", err)
	}
	if len(groups) == 0 {
		t.Skip("current process has no supplementary groups to validate")
	}
	gid := uint32(groups[0])
	if !processHasSupplementaryGroup(uint32(os.Getpid()), gid) {
		t.Fatalf("current process supplementary group %d was not detected by fallback", gid)
	}
}
