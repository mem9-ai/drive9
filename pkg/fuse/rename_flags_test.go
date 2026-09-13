package fuse

import (
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// renameat2 flags, which the kernel forwards on any mount that negotiated
// RENAME2. They are not in syscall on every platform, so name them here.
const (
	renameNoReplace = uint32(0x1)
	renameExchange  = uint32(0x2)
)

// A rename carrying renameat2 flags must not degrade into a plain rename:
// RENAME_NOREPLACE would silently clobber an existing destination and
// RENAME_EXCHANGE would destroy one side instead of swapping the two.
func TestRenameRejectsRenameat2Flags(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	input := &gofuse.RenameIn{}
	input.NodeId = 2
	input.Newdir = 3
	for _, flags := range []uint32{renameNoReplace, renameExchange, renameNoReplace | renameExchange} {
		input.Flags = flags
		if st := fs.Rename(nil, input, "old", "new"); st != gofuse.EINVAL {
			t.Fatalf("Rename flags=%#x status=%v, want EINVAL", flags, st)
		}
	}
}
