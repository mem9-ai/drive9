package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Ls lists directory contents.
//
//	drive9 fs ls           list /
//	drive9 fs ls /path/    list /path/
//	drive9 fs ls -l /path  long format with size
//	drive9 fs ls :/path    list using remote path prefix
//	drive9 fs ls [--auth=local|server] s3://bucket/prefix/
func Ls(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	long := false
	path := "/"

	for _, arg := range args {
		switch arg {
		case "-l":
			long = true
		default:
			path = arg
		}
	}

	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if h.Loc.Kind == KindObject {
		var cancel context.CancelFunc
		ctx, cancel = withObjectOpTimeout(ctx)
		defer cancel()
	}
	page, err := h.Backend.List(ctx, h.Loc, ListOpts{})
	if err != nil {
		return err
	}
	for page.NextCursor != "" {
		more, err := h.Backend.List(ctx, h.Loc, ListOpts{Cursor: page.NextCursor})
		if err != nil {
			return err
		}
		page.Entries = append(page.Entries, more.Entries...)
		page.NextCursor = more.NextCursor
	}

	if long {
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		for _, e := range page.Entries {
			kind := "-"
			if e.IsDir {
				kind = "d"
			}
			_, _ = fmt.Fprintf(w, "%s\t%d\t%s\n", kind, e.Size, e.Name)
		}
		return w.Flush()
	}

	for _, e := range page.Entries {
		name := e.Name
		if e.IsDir {
			name += "/"
		}
		fmt.Println(name)
	}
	return nil
}
