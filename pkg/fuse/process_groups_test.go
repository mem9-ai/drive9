package fuse

// Ports of v1 pkg/fuse/process_groups_test.go for the chain facade: FUSE
// requests carry only the caller's primary gid, so gid-change permission
// checks read supplementary groups from /proc/<pid>/status. The facade's
// helper set is procStatusGroups/processSupplementaryGroups (v1's
// procStatusHasGroup/processHasSupplementaryGroup collapsed into the list
// form plus containsUint32 at the call sites).

import (
	"os"
	"testing"
)

func TestProcStatusGroups(t *testing.T) {
	status := []byte("Name:\tdrive9\nGroups:\t10 20 30\n")
	groups := procStatusGroups(status)
	if len(groups) != 3 || groups[0] != 10 || groups[1] != 20 || groups[2] != 30 {
		t.Fatalf("procStatusGroups = %v, want [10 20 30]", groups)
	}
	if containsUint32(groups, 20) != true {
		t.Fatal("supplementary group 20 not detected")
	}
	if containsUint32(groups, 2) {
		t.Fatal("partial group match: 2 matched inside 10 20 30")
	}
	// Missing or malformed Groups lines parse to nil (conservative).
	if got := procStatusGroups([]byte("Name:\tdrive9\n")); got != nil {
		t.Fatalf("status without Groups line = %v, want nil", got)
	}
	if got := procStatusGroups([]byte("Groups:\tnot-a-number\n")); len(got) != 0 {
		t.Fatalf("malformed Groups line = %v, want empty", got)
	}
}

func TestProcessSupplementaryGroupsConservativeFallbackWhenProcUnavailable(t *testing.T) {
	oldReadProcessStatusFile := readProcessStatusFile
	readProcessStatusFile = func(string) ([]byte, error) {
		return nil, os.ErrNotExist
	}
	t.Cleanup(func() {
		readProcessStatusFile = oldReadProcessStatusFile
	})

	// /proc unavailable: no groups are granted (v1 conservative fallback —
	// a non-current process must not gain supplementary-group access).
	if got := processSupplementaryGroups(uint32(os.Getpid() + 1)); got != nil {
		t.Fatalf("groups on missing /proc status = %v, want nil", got)
	}

	// A parseable status supplies exactly its groups.
	readProcessStatusFile = func(string) ([]byte, error) {
		return []byte("Name:\tdrive9\nGroups:\t10 20 30\n"), nil
	}
	if got := processSupplementaryGroups(uint32(os.Getpid())); len(got) != 3 || got[2] != 30 {
		t.Fatalf("groups from stubbed status = %v, want [10 20 30]", got)
	}

	// pid 0 (the kernel / unavailable caller) has none.
	if got := processSupplementaryGroups(0); got != nil {
		t.Fatalf("groups for pid 0 = %v, want nil", got)
	}
}
