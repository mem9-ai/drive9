package cli

import (
	"fmt"
	"os"
	"strconv"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Chmod updates the permission bits of a remote file.
func Chmod(c *client.Client, args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, fsChmodUsage())
		return nil
	}
	if len(args) != 2 {
		return fmt.Errorf("%s", fsChmodUsage())
	}
	modeStr, path := args[0], args[1]
	mode64, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		return fmt.Errorf("invalid mode %q: %w", modeStr, err)
	}
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}
	return c.Chmod(path, uint32(mode64))
}
