package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Chmod updates the permission bits of a remote file.
func Chmod(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	layerRef, fence, args, err := ParseLayerViewFlags(args)
	if err != nil {
		return err
	}
	if len(args) != 2 {
		return fmt.Errorf("usage: drive9 fs chmod [--layer <ref> [--fence <N>]] <mode> <path>")
	}
	modeStr, path := args[0], args[1]
	mode64, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		return fmt.Errorf("invalid mode %q: %w", modeStr, err)
	}
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if err := requireCapOnHandle(h, CapChmod, "chmod"); err != nil {
		return err
	}
	if h.Loc.Kind == KindDrive9 && h.Loc.Context != "" {
		if err := requireNoLayerWithRemoteContext(layerRef, c, RemotePath{Context: h.Loc.Context, Path: h.Path}, args[1]); err != nil {
			return err
		}
	}
	c, path = h.Client, h.Path
	if layerRef != "" {
		c, err = BindCLIViewClient(context.Background(), c, layerRef, fence)
		if err != nil {
			return err
		}
	}
	return c.Chmod(path, uint32(mode64))
}
