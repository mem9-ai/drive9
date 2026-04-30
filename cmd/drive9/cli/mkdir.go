package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Mkdir creates a remote directory.
//
//	drive9 fs mkdir /path/to/dir
//	drive9 fs mkdir :/path/to/dir
//
// Parent directories are created automatically by the server.
func Mkdir(c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 fs mkdir <path>")
	}

	path := args[0]

	// Handle ":" prefixed remote paths
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}

	if path == "" || path == "/" {
		return fmt.Errorf("drive9 fs mkdir: cannot create root directory")
	}

	return c.Mkdir(path)
}
