package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Hardlink creates a remote file hard link.
//
//	drive9 fs hardlink /target /link
//	drive9 fs hardlink :/target :/link
//	drive9 fs hardlink ctx:/target ctx:/link
func Hardlink(c *client.Client, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs hardlink <target> <link>")
	}
	srcPath := args[0]
	dstPath := args[1]
	srcRP, srcIsRemote := ParseRemote(srcPath)
	dstRP, dstIsRemote := ParseRemote(dstPath)
	if srcIsRemote {
		srcPath = srcRP.Path
	}
	if dstIsRemote {
		dstPath = dstRP.Path
	}

	switch {
	case srcRP.Context == "" && dstRP.Context == "":
	case srcRP.Context != "" && dstRP.Context != "" && srcRP.Context == dstRP.Context:
		var err error
		c, err = newFSClientForContext(srcRP.Context)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cross-context hardlink not supported: %q -> %q", args[0], args[1])
	}
	return c.Hardlink(srcPath, dstPath)
}
