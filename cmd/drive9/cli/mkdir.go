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
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 fs mkdir [--layer <ref>] <path>")
	}
	path := args[0]
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if h.Loc.Kind == KindObject {
		if layerRef != "" {
			return fmt.Errorf("--layer is drive9-only")
		}
		if err := h.Backend.Mkdir(context.Background(), h.Loc); err != nil {
			return err
		}
		fmt.Printf("created %s\n", path)
		return nil
	}
	c, path = h.Client, h.Path
	if layerRef != "" {
		if err := mkdirLayerPath(context.Background(), c, layerRef, path, 0o755); err != nil {
			return err
		}
		fmt.Printf("created %s\n", path)
		return nil
	}
	if err := c.Mkdir(path); err != nil {
		return err
	}
	fmt.Printf("created %s\n", path)
	return nil
}
