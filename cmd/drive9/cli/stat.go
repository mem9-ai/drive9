package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Stat shows metadata for a remote path.
//
//	drive9 stat /path/to/file
//	drive9 stat :/path/to/file
func Stat(c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 stat <path>")
	}
	path := args[0]
	// Handle ":" prefixed remote paths like cp command
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}
	s, err := c.Stat(path)
	if err != nil {
		return err
	}
	fmt.Printf("size:     %d\n", s.Size)
	fmt.Printf("isdir:    %v\n", s.IsDir)
	fmt.Printf("revision: %d\n", s.Revision)
	return nil
}
