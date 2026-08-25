package cli

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Hardlink creates a remote file hard link.
func Hardlink(c *client.Client, args []string) error {
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs hardlink [--layer <ref>] <target> <link>")
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
	if layerRef != "" {
		if err := requireNoLayerWithRemoteContext(layerRef, srcRP, args[0]); err != nil {
			return err
		}
		if err := requireNoLayerWithRemoteContext(layerRef, dstRP, args[1]); err != nil {
			return err
		}
		return hardlinkLayerPath(context.Background(), c, layerRef, srcPath, dstPath)
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
