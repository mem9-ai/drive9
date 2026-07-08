package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
)

func Grep(c *client.Client, args []string) error {
	layerRef, jsonMode, args, err := parseGrepFlags(args)
	if err != nil {
		return err
	}
	if len(args) < 1 {
		return fmt.Errorf("usage: drive9 fs grep [--layer <ref>] [--json] <pattern> [path]")
	}
	query := args[0]
	path := "/"
	if len(args) > 1 {
		path = args[1]
	}

	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}

	results, err := c.GrepWithLayer(query, path, 20, layerRef)
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

// parseGrepFlags strips --layer <ref> and --json from args, returning the
// layer reference, whether JSON output mode is requested, and the remaining
// positional args. It extends parseLayerFlag with grep-specific --json.
func parseGrepFlags(args []string) (string, bool, []string, error) {
	layerRef, filtered, err := parseLayerFlag(args)
	if err != nil {
		return "", false, nil, err
	}
	jsonMode := false
	remaining := make([]string, 0, len(filtered))
	for _, arg := range filtered {
		if arg == "--json" {
			jsonMode = true
		} else {
			remaining = append(remaining, arg)
		}
	}
	return layerRef, jsonMode, remaining, nil
}

func parseLayerFlag(args []string) (string, []string, error) {
	layerRef := ""
	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--layer":
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("--layer requires argument")
			}
			i++
			layerRef = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--layer="):
			layerRef = strings.TrimSpace(strings.TrimPrefix(arg, "--layer="))
		default:
			filtered = append(filtered, arg)
		}
	}
	return layerRef, filtered, nil
}
