package cli

import (
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Mkdir creates a remote directory.
// Parent directories are created automatically.
//
//	drive9 fs mkdir /path/to/dir
//	drive9 fs mkdir :/path/to/dir
func Mkdir(c *client.Client, args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, fsMkdirUsage())
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("%s", fsMkdirUsage())
	}
	path := args[0]
	var err error
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}
	if err := c.Mkdir(path); err != nil {
		return err
	}
	fmt.Printf("created %s\n", path)
	return nil
}
