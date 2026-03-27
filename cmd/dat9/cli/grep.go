package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func Grep(c *client.Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: dat9 fs grep <pattern> [path]")
	}
	query := args[0]
	path := "/"
	if len(args) > 1 {
		path = args[1]
	}

	results, err := c.Grep(query, path, 20)
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
	if len(args) < 1 {
		return fmt.Errorf("usage: dat9 fs grep <pattern> [path]")
	}
	query := args[0]
	path := "/"
	if len(args) > 1 {
		path = args[1]
	}
	results, err := c.Grep(query, path, 20)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
