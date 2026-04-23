package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Mv renames or moves a remote file/directory. Metadata-only, zero S3 cost.
//
//	drive9 fs mv /old/path /new/path
//	drive9 fs mv :/old/path :/new/path
func Mv(c *client.Client, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs mv <old> <new>")
	}
	oldPath := args[0]
	newPath := args[1]
	// Handle ":" prefixed remote paths like cp command
	if rp, isRemote := ParseRemote(oldPath); isRemote {
		oldPath = rp.Path
	}
	if rp, isRemote := ParseRemote(newPath); isRemote {
		newPath = rp.Path
	}
	return c.Rename(oldPath, newPath)
}
