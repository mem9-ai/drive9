package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Rm removes a remote file or directory.
//
//	drive9 rm /path/to/file
//	drive9 rm :/path/to/file
func Rm(c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 rm <path>")
	}
	path := args[0]
	// Handle ":" prefixed remote paths like cp command
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}
	return c.Delete(path)
}
