package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Mkdir creates a remote directory.
// Parent directories are created automatically.
//
//	drive9 fs mkdir /path/to/dir
//	drive9 fs mkdir :/path/to/dir
func Mkdir(c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 fs mkdir <path>")
	}
	path := args[0]
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}
	if err := c.Mkdir(path); err != nil {
		return err
	}
	fmt.Printf("created %s\n", path)
	return nil
}
