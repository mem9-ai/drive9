package cli

import (
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Hardlink creates a remote file hard link.
func Hardlink(c *client.Client, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs hardlink <target> <link>")
	}
	srcLoc, err := Parse(args[0])
	if err != nil {
		return err
	}
	dstLoc, err := Parse(args[1])
	if err != nil {
		return err
	}
	srcLoc = promoteBareFSArg(srcLoc)
	dstLoc = promoteBareFSArg(dstLoc)
	if srcLoc.Kind == KindObject || dstLoc.Kind == KindObject {
		return fmt.Errorf("hardlink: object-store URIs are not supported")
	}
	srcRP, _ := locationAsRemotePath(srcLoc)
	dstRP, _ := locationAsRemotePath(dstLoc)
	srcPath := srcLoc.Path
	dstPath := dstLoc.Path
	if srcLoc.Kind == KindLocal {
		srcPath = srcLoc.Raw
		srcRP = RemotePath{}
	}
	if dstLoc.Kind == KindLocal {
		dstPath = dstLoc.Raw
		dstRP = RemotePath{}
	}

	switch {
	case srcRP.Context == "" && dstRP.Context == "":
	case srcRP.Context != "" && dstRP.Context != "" && srcRP.Context == dstRP.Context:
		var err error
		c, err = fsClientInContext(c, srcRP.Context)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cross-context hardlink not supported: %q -> %q", args[0], args[1])
	}
	return c.Hardlink(srcPath, dstPath)
}
