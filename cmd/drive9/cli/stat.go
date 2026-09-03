package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Stat shows metadata for a remote path.
//
//	drive9 fs stat /path/to/file
//	drive9 fs stat -o json /path/to/file
//	drive9 fs stat :/path/to/file
func Stat(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	outputFormat := "text"
	path := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "-o", "--output":
			if i+1 >= len(args) {
				return fmt.Errorf("usage: drive9 fs stat [-o text|json] [--auth=local|server] <path>")
			}
			i++
			outputFormat = args[i]
			if outputFormat != "text" && outputFormat != "json" {
				return fmt.Errorf("unsupported output format %q (want text or json)", outputFormat)
			}
		default:
			if strings.HasPrefix(arg, "-") {
				return fmt.Errorf("usage: drive9 fs stat [-o text|json] [--auth=local|server] <path>")
			}
			if path != "" {
				return fmt.Errorf("usage: drive9 fs stat [-o text|json] [--auth=local|server] <path>")
			}
			path = arg
		}
	}
	if path == "" {
		return fmt.Errorf("usage: drive9 fs stat [-o text|json] [--auth=local|server] <path>")
	}
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if h.Loc.Kind == KindObject {
		ctx, cancel := withObjectOpTimeout(context.Background())
		defer cancel()
		info, err := h.Backend.Stat(ctx, h.Loc)
		if err != nil {
			return err
		}
		if outputFormat == "json" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(info)
		}
		kind := "file"
		if info.IsDir {
			kind = "dir"
		}
		fmt.Printf("path:\t%s\nkind:\t%s\nsize:\t%d\n", h.Loc.Raw, kind, info.Size)
		if !info.Mtime.IsZero() {
			fmt.Printf("mtime:\t%s\n", info.Mtime.Format(time.RFC3339))
		}
		if info.ETag != "" {
			fmt.Printf("etag:\t%s\n", info.ETag)
		}
		if info.ContentType != "" {
			fmt.Printf("content-type:\t%s\n", info.ContentType)
		}
		return nil
	}
	c, path = h.Client, h.Path
	m, err := c.StatMetadataCompat(path)
	if err != nil {
		return err
	}
	if outputFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(m)
	}

	fmt.Printf("size: %d\n", m.Size)
	fmt.Printf("isdir: %v\n", m.IsDir)
	if m.ResourceID != "" {
		fmt.Printf("resource_id: %s\n", m.ResourceID)
	}
	if m.Nlink > 0 {
		fmt.Printf("nlink: %d\n", m.Nlink)
	}
	fmt.Printf("revision: %d\n", m.Revision)
	if m.Mtime != nil {
		fmt.Printf("mtime: %s\n", time.Unix(*m.Mtime, 0).UTC().Format(time.RFC3339))
	}
	fmt.Printf("content_type: %s\n", m.ContentType)
	fmt.Printf("semantic_text: %s\n", m.SemanticText)
	if m.Description != "" {
		fmt.Printf("description: %s\n", m.Description)
	}
	if m.Degraded {
		fmt.Printf("degraded: true\n")
	}

	keys := make([]string, 0, len(m.Tags))
	for k := range m.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("tags.%s: %s\n", k, m.Tags[k])
	}
	return nil
}
