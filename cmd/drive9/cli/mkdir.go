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
	if len(args) != 1 {
		return fmt.Errorf("usage: drive9 fs mkdir [--auth=local|server] <path>")
	}
	path := args[0]
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if h.Loc.Kind == KindObject {
		ctx, cancel := withObjectOpTimeout(context.Background())
		defer cancel()
		if err := h.Backend.Mkdir(ctx, h.Loc); err != nil {
			return err
		}
		fmt.Printf("created %s\n", path)
		return nil
	}
	c, path = h.Client, h.Path
	if err := c.Mkdir(path); err != nil {
		return err
	}
	fmt.Printf("created %s\n", path)
	return nil
}
