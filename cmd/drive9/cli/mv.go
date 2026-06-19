package cli

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Mv renames or moves a remote file/directory. Metadata-only, zero S3 cost.
//
//	drive9 fs mv /old/path /new/path
//	drive9 fs mv :/old/path :/new/path
func Mv(c *client.Client, args []string) error {
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs mv [--layer <ref>] <old> <new>")
	}
	oldPath := args[0]
	newPath := args[1]
	oldRP, oldIsRemote := ParseRemote(oldPath)
	newRP, newIsRemote := ParseRemote(newPath)
	if oldIsRemote {
		oldPath = oldRP.Path
	}
	if newIsRemote {
		newPath = newRP.Path
	}
	if layerRef != "" {
		if err := requireNoLayerWithRemoteContext(layerRef, oldRP, args[0]); err != nil {
			return err
		}
		if err := requireNoLayerWithRemoteContext(layerRef, newRP, args[1]); err != nil {
			return err
		}
		return renameLayerPath(context.Background(), c, layerRef, oldPath, newPath)
	}

	switch {
	case oldRP.Context == "" && newRP.Context == "":
	case oldRP.Context != "" && newRP.Context != "" && oldRP.Context == newRP.Context:
		var err error
		c, err = newFSClientForContext(oldRP.Context)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cross-context rename not supported: %q -> %q", args[0], args[1])
	}
	return c.Rename(oldPath, newPath)
}
