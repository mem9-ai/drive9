package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
)

// BindFilesystemView binds ordinary file commands to an explicit work generation.
// It never creates work or changes a process-global client. Control commands own
// their flags separately. The remaining command arguments use the main API path.
func BindFilesystemView(c *client.Client, command string, args []string) (*client.Client, []string, error) {
	if command == "layer" {
		return c, args, nil
	}
	var b client.ViewBinding
	seen := map[string]bool{}
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			rest = append(rest, args[i:]...)
			break
		}
		key, value, hasValue := strings.Cut(arg, "=")
		if key != "--layer" && key != "--work" && key != "--fence" && key != "--validation" {
			rest = append(rest, arg)
			// Values of existing file-command flags are literal, even when
			// they look like a view option. Their own parsers validate them.
			if !hasValue && i+1 < len(args) {
				switch key {
				case "--auth", "--offset", "--length", "-o", "--description", "--tag":
					i++
					rest = append(rest, args[i])
				}
			}
			continue
		}
		if seen[key] {
			return nil, nil, fmt.Errorf("duplicate %s", key)
		}
		seen[key] = true
		if key == "--validation" && !hasValue {
			value = "true"
		} else if !hasValue {
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("%s requires a value", key)
			}
			value = args[i]
		}
		switch key {
		case "--layer":
			b.LayerID = value
		case "--work":
			b.WorkID = value
		case "--fence":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n <= 0 || strconv.FormatInt(n, 10) != value {
				return nil, nil, fmt.Errorf("invalid --fence %q", value)
			}
			b.Fence = n
		case "--validation":
			if value != "true" && value != "false" {
				return nil, nil, fmt.Errorf("invalid --validation %q", value)
			}
			b.Validation = value == "true"
		}
	}
	if len(seen) == 0 {
		return c, args, nil
	}
	switch command {
	case "cat", "ls", "stat", "cp", "mkdir", "rm", "mv", "chmod", "hardlink", "symlink":
	default:
		return nil, nil, fmt.Errorf("fixed view binding is not supported for fs %s", command)
	}
	if !seen["--layer"] || b.LayerID == "" {
		return nil, nil, fmt.Errorf("--layer is required with work binding flags")
	}
	if b.LayerID == "main" {
		if len(seen) != 1 {
			return nil, nil, fmt.Errorf("main does not accept work binding flags")
		}
		b = client.ViewBinding{}
	}
	bound, err := c.WithView(b)
	if err != nil {
		return nil, nil, fmt.Errorf("fixed Layer requires its exact --layer, --work and --fence: %w", err)
	}
	return bound, rest, nil
}

func fsClientInContext(current *client.Client, name string) (*client.Client, error) {
	if current.ViewBinding().LayerID != "" {
		return nil, fmt.Errorf("a fixed Layer belongs to the selected workspace; named context %q cannot replace its client", name)
	}
	return newFSClientForContext(name)
}
