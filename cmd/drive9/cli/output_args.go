package cli

import (
	"fmt"
	"strings"
)

// parseOutputFormatAndPath parses the shared `[-o text|json] <path>` argument
// shape used by the read-only fs commands. usage is the full usage string
// echoed on any parse error, so each caller reports its own command name.
func parseOutputFormatAndPath(args []string, usage string) (outputFormat, path string, err error) {
	outputFormat = "text"
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "-o", "--output":
			if i+1 >= len(args) {
				return "", "", fmt.Errorf("usage: %s", usage)
			}
			i++
			outputFormat = args[i]
			if outputFormat != "text" && outputFormat != "json" {
				return "", "", fmt.Errorf("unsupported output format %q (want text or json)", outputFormat)
			}
		default:
			if strings.HasPrefix(arg, "-") {
				return "", "", fmt.Errorf("usage: %s", usage)
			}
			if path != "" {
				return "", "", fmt.Errorf("usage: %s", usage)
			}
			path = arg
		}
	}
	if path == "" {
		return "", "", fmt.Errorf("usage: %s", usage)
	}
	return outputFormat, path, nil
}
