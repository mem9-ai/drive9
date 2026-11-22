package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func Grep(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	layerRef, _, args, err := ParseLayerViewFlags(args)
	if err != nil {
		return err
	}
	if layerRef != "" || (c != nil && c.ViewBinding().LayerID != "") {
		return fmt.Errorf("grep: %w (--layer is not supported on search endpoints)", viewprotocol.ErrUnsupportedInLayerView)
	}
	jsonMode := false
	remaining := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "--json" {
			jsonMode = true
		} else {
			remaining = append(remaining, arg)
		}
	}
	if len(remaining) < 1 {
		return fmt.Errorf("usage: drive9 fs grep [--json] <pattern> [path]")
	}
	query := remaining[0]
	path := "/"
	if len(remaining) > 1 {
		path = remaining[1]
	}

	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if err := requireCapOnHandle(h, CapSearch, "grep"); err != nil {
		return err
	}
	c, path = h.Client, h.Path

	results, err := c.Grep(query, path, 20)
	if err != nil {
		return err
	}
	if jsonMode {
		if results == nil {
			results = []client.SearchResult{}
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(results)
	}
	if len(results) == 0 {
		return nil
	}
	for _, r := range results {
		if r.Score != nil {
			_, _ = fmt.Fprintf(os.Stdout, "%s\t%.2f\n", r.Path, *r.Score)
		} else {
			_, _ = fmt.Fprintln(os.Stdout, r.Path)
		}
	}
	return nil
}
