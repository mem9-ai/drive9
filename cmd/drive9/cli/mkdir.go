package cli

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Mkdir creates a remote directory.
// Parent directories are created automatically.
//
//	drive9 fs mkdir /path/to/dir
//	drive9 fs mkdir :/path/to/dir
func Mkdir(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	layerRef, fence, args, err := ParseLayerViewFlags(args)
	if err != nil {
		return err
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 fs mkdir [--layer <ref> [--fence <N>]] [--auth=local|server] <path>")
	}
	path := args[0]
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if h.Loc.Kind == KindObject {
		if layerRef != "" || (c != nil && c.ViewBinding().LayerID != "") {
			return fmt.Errorf("--layer is drive9-only")
		}
		ctx, cancel := withObjectOpTimeout(context.Background())
		defer cancel()
		if err := h.Backend.Mkdir(ctx, h.Loc); err != nil {
			return err
		}
		fmt.Printf("created %s\n", path)
		return nil
	}
	if h.Loc.Kind == KindDrive9 && h.Loc.Context != "" {
		if err := requireNoLayerWithRemoteContext(layerRef, c, RemotePath{Context: h.Loc.Context, Path: h.Path}, args[0]); err != nil {
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
	if err := c.Mkdir(path); err != nil {
		return err
	}
	fmt.Printf("created %s\n", path)
	return nil
}
