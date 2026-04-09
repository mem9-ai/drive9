package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func SQL(c *client.Client, args []string) error {
	var query string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-q", "--query":
			if i+1 >= len(args) {
				return fmt.Errorf("-q requires a SQL query argument")
			}
			i++
			query = args[i]
		case "-f", "--file":
			if i+1 >= len(args) {
				return fmt.Errorf("-f requires a file path argument")
			}
			i++
			data, err := os.ReadFile(args[i])
			if err != nil {
				return fmt.Errorf("read sql file: %w", err)
			}
			query = string(data)
		default:
			return fmt.Errorf("unknown flag %q\nusage: drive9 db sql -q \"SELECT ...\"", args[i])
		}
	}

	if query == "" {
		return fmt.Errorf("usage: drive9 db sql -q \"SELECT ...\" or drive9 db sql -f query.sql")
	}

	rows, err := c.SQL(query)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}
