package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Tasks shows the extract/embed task status for a file's current revision,
// including the failure reason when a task failed.
//
//	drive9 fs tasks /path/to/file
//	drive9 fs tasks -o json /path/to/file
//	drive9 fs tasks :/path/to/file
func Tasks(c *client.Client, args []string) error {
	authLocal, args, err := peelObjectAuth(args)
	if err != nil {
		return err
	}
	defer withObjectAuthLocal(authLocal)()
	outputFormat := "text"
	path := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "-o", "--output":
			if i+1 >= len(args) {
				return fmt.Errorf("usage: drive9 fs tasks [-o text|json] [--auth=local|server] <path>")
			}
			i++
			outputFormat = args[i]
			if outputFormat != "text" && outputFormat != "json" {
				return fmt.Errorf("unsupported output format %q (want text or json)", outputFormat)
			}
		default:
			if strings.HasPrefix(arg, "-") {
				return fmt.Errorf("usage: drive9 fs tasks [-o text|json] [--auth=local|server] <path>")
			}
			if path != "" {
				return fmt.Errorf("usage: drive9 fs tasks [-o text|json] [--auth=local|server] <path>")
			}
			path = arg
		}
	}
	if path == "" {
		return fmt.Errorf("usage: drive9 fs tasks [-o text|json] [--auth=local|server] <path>")
	}
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
	}
	if h.Loc.Kind == KindObject {
		return fmt.Errorf("drive9 fs tasks: only available on drive9 paths")
	}
	c, path = h.Client, h.Path
	resp, err := c.FileTasksCtx(context.Background(), path)
	if err != nil {
		return err
	}
	if outputFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp)
	}
	if len(resp.Tasks) == 0 {
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TASK_TYPE\tSTATUS\tLAST_ERROR")
	for _, task := range resp.Tasks {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", task.TaskType, task.Status, task.LastError)
	}
	return w.Flush()
}
