package cli

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Mv renames or moves a remote file/directory. Metadata-only, zero S3 cost.
//
//	drive9 fs mv /old/path /new/path
//	drive9 fs mv :/old/path :/new/path
func Mv(c *client.Client, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs mv <old> <new>")
	}
	oldPath := args[0]
	newPath := args[1]
	var (
		oldCtx string
		newCtx string
		err    error
	)
	c, oldPath, oldCtx, _, err = fsClientForRemoteArg(c, oldPath)
	if err != nil {
		return err
	}
	newClient := c
	newClient, newPath, newCtx, _, err = fsClientForRemoteArg(newClient, newPath)
	if err != nil {
		return err
	}
	if oldCtx != "" && newCtx != "" && oldCtx != newCtx {
		return fmt.Errorf("cross-context rename not supported: %s -> %s", oldCtx, newCtx)
	}
	if oldCtx == "" && newCtx != "" {
		c = newClient
	}
	return c.Rename(oldPath, newPath)
}
