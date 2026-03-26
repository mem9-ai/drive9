package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Cp copies files between local and remote.
//
// Remote paths use a ":" prefix to distinguish from local paths:
//
//	dat9 cp local.txt :/remote/path    upload
//	dat9 cp :/remote/path local.txt    download
//	dat9 cp :/remote/a :/remote/b      server-side copy (zero-copy)
//	dat9 cp - :/remote/path            upload from stdin
//	dat9 cp :/remote/path -            download to stdout
func Cp(c *client.Client, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: dat9 cp <src> <dst>")
	}
	src, dst := args[0], args[1]

	srcRemote := isRemote(src)
	dstRemote := isRemote(dst)

	// Strip ":" prefix from remote paths for API calls
	if srcRemote {
		src = src[1:]
	}
	if dstRemote {
		dst = dst[1:]
	}

	switch {
	case src == "-" && dstRemote:
		// stdin → remote
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		return c.Write(dst, data)

	case srcRemote && dst == "-":
		// remote → stdout
		data, err := c.Read(src)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(data)
		return err

	case !srcRemote && dstRemote:
		// local → remote (upload)
		data, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("read %s: %w", src, err)
		}
		return c.Write(dst, data)

	case srcRemote && !dstRemote:
		// remote → local (download)
		data, err := c.Read(src)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0644)

	case srcRemote && dstRemote:
		// remote → remote (server-side copy, zero-copy in inode model)
		return c.Copy(src, dst)

	default:
		return fmt.Errorf("at least one path must be remote (use : prefix, e.g. :/path)")
	}
}

// isRemote returns true if a path refers to a remote dat9 path.
// Remote paths use a ":" prefix (e.g., ":/data/file.txt").
func isRemote(path string) bool {
	if path == "-" {
		return false
	}
	return strings.HasPrefix(path, ":")
}
