package cli

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Ls lists directory contents.
//
//	dat9 ls           list /
//	dat9 ls /path/    list /path/
//	dat9 ls -l /path  long format with size
//	dat9 ls :/path    list using remote path prefix
func Ls(c *client.Client, args []string) error {
	long := false
	path := "/"

	for _, arg := range args {
		switch arg {
		case "-l":
			long = true
		default:
			path = arg
		}
	}

	// Handle ":" prefixed remote paths like cp command
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}

	entries, err := c.List(path)
	if err != nil {
		return err
	}

	if long {
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		for _, e := range entries {
			kind := "-"
			if e.IsDir {
				kind = "d"
			}
			_, _ = fmt.Fprintf(w, "%s\t%d\t%s\n", kind, e.Size, e.Name)
		}
		return w.Flush()
	}

	for _, e := range entries {
		name := e.Name
		if e.IsDir {
			name += "/"
		}
		fmt.Println(name)
	}
	return nil
}
