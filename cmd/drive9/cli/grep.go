package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func Grep(c *client.Client, args []string) error {
	if IsHelpArgs(args) {
		_, _ = fmt.Fprintln(os.Stdout, fsGrepUsage())
		return nil
	}
	if len(args) < 1 {
		return fmt.Errorf("%s", fsGrepUsage())
	}
	query := args[0]
	path := "/"
	if len(args) > 1 {
		path = args[1]
	}

	var err error
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
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
		return fmt.Errorf("%s", fsGrepUsage())
	}
	query := args[0]
	path := "/"
	if len(args) > 1 {
		path = args[1]
	}

	var err error
	c, path, _, _, err = fsClientForRemoteArg(c, path)
	if err != nil {
		return err
	}

	results, err := c.Grep(query, path, 20)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
