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
//	drive9 fs stat -o json /path/to/file
//	drive9 fs stat :/path/to/file
func Stat(c *client.Client, args []string) error {
	outputFormat := "text"
	path := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "-o", "--output":
			if i+1 >= len(args) {
				return fmt.Errorf("usage: drive9 fs stat [-o text|json] <path>")
			}
			i++
			outputFormat = args[i]
			if outputFormat != "text" && outputFormat != "json" {
				return fmt.Errorf("unsupported output format %q (want text or json)", outputFormat)
			}
		default:
			if strings.HasPrefix(arg, "-") {
				return fmt.Errorf("usage: drive9 fs stat [-o text|json] <path>")
			}
			if path != "" {
				return fmt.Errorf("usage: drive9 fs stat [-o text|json] <path>")
			}
			path = arg
		}
	}
	if path == "" {
		return fmt.Errorf("usage: drive9 fs stat [-o text|json] <path>")
	}
	// Handle ":" prefixed remote paths like cp command
	if rp, isRemote := ParseRemote(path); isRemote {
		path = rp.Path
	}
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
