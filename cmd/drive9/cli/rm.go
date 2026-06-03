package cli

import (
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Rm removes a remote file or directory.
//
//	drive9 fs rm /path/to/file
//	drive9 fs rm -r /path/to/dir/
//	drive9 fs rm --recursive :/path/to/dir/
func Rm(c *client.Client, args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, fsRmUsage())
		return nil
	}
	var (
		path       string
		parseFlags = true
		recursive  bool
	)

	for _, arg := range args {
		if parseFlags {
			switch arg {
			case "-r", "--recursive":
				recursive = true
				continue
			case "--":
				parseFlags = false
				continue
			}
			if len(arg) > 0 && arg[0] == '-' {
				return fmt.Errorf("unknown flag %q", arg)
			}
		}
		if path != "" {
			return fmt.Errorf("%s", fsRmUsage())
		}
		path = arg
	}

	if path == "" {
		return fmt.Errorf("%s", fsRmUsage())
	}

	var err error
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}
	if recursive {
		return c.RemoveAll(path)
	}
	return c.Delete(path)
}
