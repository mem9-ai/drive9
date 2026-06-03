package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Cat reads a remote file and writes it to stdout.
// Uses ReadStream to handle both small files (direct) and large files (presigned URL).
//
//	drive9 fs cat /path/to/file
//	drive9 fs cat :/path/to/file
func Cat(c *client.Client, args []string) error {
	return catWithWriter(c, args, os.Stdout)
}

func catWithWriter(c *client.Client, args []string, out io.Writer) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(out, fsCatUsage())
		return nil
	}
	fs := flag.NewFlagSet("fs cat", flag.ContinueOnError)
	offset := fs.Int64("offset", 0, "byte offset for a positional read; requires --length")
	length := fs.Int64("length", 0, "byte length for a positional read; requires --offset")
	if err := fs.Parse(args); err != nil {
		return err
	}
	offsetSet := false
	lengthSet := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "offset":
			offsetSet = true
		case "length":
			lengthSet = true
		}
	})

	if fs.NArg() != 1 {
		return fmt.Errorf("%s", fsCatUsage())
	}
	if offsetSet != lengthSet {
		return fmt.Errorf("--offset and --length must be provided together")
	}
	if *offset < 0 {
		return fmt.Errorf("--offset must be >= 0")
	}
	if *length < 0 {
		return fmt.Errorf("--length must be >= 0")
	}
	path := fs.Arg(0)
	var err error
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}
	var (
		rc io.ReadCloser
	)
	if offsetSet {
		rc, err = c.ReadStreamRange(context.Background(), path, *offset, *length)
	} else {
		rc, err = c.ReadStream(context.Background(), path)
	}
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	_, err = io.Copy(out, rc)
	return err
}
