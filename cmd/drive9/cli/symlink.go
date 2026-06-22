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
	linkRP, linkIsRemote := ParseRemote(linkPath)

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
	targetRP, targetIsRemote := ParseRemote(target)
	if !targetIsRemote {
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
