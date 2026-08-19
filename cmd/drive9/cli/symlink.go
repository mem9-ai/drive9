package cli

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Symlink creates a remote symbolic link.
//
//	drive9 fs symlink ../target link
//	drive9 fs symlink /target :/link
//	drive9 fs symlink ctx:/target ctx:/link
func Symlink(c *client.Client, args []string) error {
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs symlink [--layer <ref>] <target> <link>")
	}
	target := args[0]
	linkPath := args[1]
	linkLoc, err := Parse(linkPath)
	if err != nil {
		return err
	}
	if linkLoc.Kind == KindObject {
		return fmt.Errorf("symlink: object-store URIs are not supported")
	}
	linkLoc = promoteBareFSArg(linkLoc)
	linkRP, linkIsRemote := locationAsRemotePath(linkLoc)

	target, err = symlinkTargetForCLI(target, linkRP, linkIsRemote)
	if err != nil {
		return err
	}
	c, linkPath, _, _, err = fsClientForRemoteArg(c, linkPath)
	if err != nil {
		return err
	}
	if layerRef != "" {
		return symlinkLayerPath(context.Background(), c, layerRef, target, linkPath)
	}
	return c.Symlink(target, linkPath)
}

func symlinkTargetForCLI(target string, linkRP RemotePath, linkIsRemote bool) (string, error) {
	targetLoc, err := Parse(target)
	if err != nil {
		return "", err
	}
	if targetLoc.Kind == KindObject {
		return "", fmt.Errorf("symlink: object-store target URIs are not supported")
	}
	targetLoc = promoteBareFSArg(targetLoc)
	targetRP, targetIsRemote := locationAsRemotePath(targetLoc)
	if !targetIsRemote {
		if targetLoc.Kind == KindLocal {
			return targetLoc.Raw, nil
		}
		return target, nil
	}
	if targetRP.Context == "" {
		return targetRP.Path, nil
	}
	if !linkIsRemote || linkRP.Context == "" {
		return "", fmt.Errorf("symlink target context %q requires link path to use the same context prefix", targetRP.Context)
	}
	if linkRP.Context != targetRP.Context {
		return "", fmt.Errorf("cross-context symlink not supported: target context %q, link context %q", targetRP.Context, linkRP.Context)
	}
	return targetRP.Path, nil
}
