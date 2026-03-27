package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

func SQL(c *client.Client, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: dat9 sql \"SELECT ...\"")
	}
	query := strings.Join(args, " ")

	rows, err := c.SQL(query)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}
