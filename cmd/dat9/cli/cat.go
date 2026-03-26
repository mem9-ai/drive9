package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Cat reads a remote file and writes it to stdout.
// Uses ReadStream to handle both small files (direct) and large files (presigned URL).
//
//	dat9 cat /path/to/file
func Cat(c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: dat9 cat <path>")
	}
	rc, err := c.ReadStream(context.Background(), args[0])
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(os.Stdout, rc)
	return err
}
