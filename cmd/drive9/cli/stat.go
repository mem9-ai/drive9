package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Stat shows metadata for a remote path.
//
//	drive9 fs stat /path/to/file
//	drive9 fs stat :/path/to/file
func Stat(c *client.Client, args []string) error {
	jsonOutput := false
	path := ""
	for _, arg := range args {
		switch arg {
		case "--json":
			jsonOutput = true
		default:
			if strings.HasPrefix(arg, "-") {
				return fmt.Errorf("usage: drive9 fs stat [--json] <path>")
			}
			if path != "" {
				return fmt.Errorf("usage: drive9 fs stat [--json] <path>")
			}
			path = arg
		}
	}
	if path == "" {
		return fmt.Errorf("usage: drive9 fs stat [--json] <path>")
	}
	// Handle ":" prefixed remote paths like cp command
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}
	m, err := c.StatMetadataCompat(path)
	if err != nil {
		return err
	}
	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(m)
	}

	fmt.Printf("size: %d\n", m.Size)
	fmt.Printf("isdir: %v\n", m.IsDir)
	fmt.Printf("revision: %d\n", m.Revision)
	fmt.Printf("content_type: %s\n", m.ContentType)
	fmt.Printf("semantic_text: %s\n", m.SemanticText)

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
