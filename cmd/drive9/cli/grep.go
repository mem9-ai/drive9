package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

func Grep(c *client.Client, args []string) error {
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) < 1 {
		return fmt.Errorf("usage: drive9 fs grep [--layer <ref>] <pattern> [path]")
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

func GrepJSON(c *client.Client, args []string) error {
	layerRef, args, err := parseLayerFlag(args)
	if err != nil {
		return err
	}
	if len(args) < 1 {
		return fmt.Errorf("usage: drive9 fs grep [--layer <ref>] <pattern> [path]")
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
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
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
