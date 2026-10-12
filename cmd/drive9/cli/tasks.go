package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Tasks shows the extract/embed task status for a file's current revision,
// including the failure reason when a task failed and the LLM tokens, calls,
// and audio duration the task consumed.
//
//	drive9 fs tasks /path/to/file
//	drive9 fs tasks -o json /path/to/file
//	drive9 fs tasks :/path/to/file
func Tasks(c *client.Client, args []string) error {
	const usage = "drive9 fs tasks [-o text|json] <path>"
	outputFormat, path, err := parseOutputFormatAndPath(args, usage)
	if err != nil {
		return err
	}
	// Task status is a drive9 concept, so reject object-store URIs before
	// fsHandleForArg opens an object backend and mints credentials for a
	// command that can never use them. Stdin ("-") is already rejected as a
	// flag by parseOutputFormatAndPath.
	loc, err := Parse(path)
	if err != nil {
		return err
	}
	if loc = promoteBareFSArg(loc); loc.Kind == KindObject {
		return fmt.Errorf("drive9 fs tasks: only available on drive9 paths")
	}
	h, err := fsHandleForArg(c, path)
	if err != nil {
		return err
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
	// Always print the header so an empty result is distinguishable from a
	// no-op; JSON already encodes an empty result as {"tasks": []}.
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TASK_TYPE\tSTATUS\tUSAGE\tPENDING_REASON\tLAST_ERROR")
	for _, task := range resp.Tasks {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			taskCell(task.TaskType), taskCell(task.Status), taskUsageCell(task.Usage),
			taskCell(task.PendingReason), taskCell(task.LastError))
	}
	return w.Flush()
}

// taskUsageCell renders one task's LLM consumption. A reported-but-empty usage
// stays visible: it tells a parked or skipped task apart from a server that
// does not report usage at all, which renders as "-".
func taskUsageCell(u *client.FileTaskUsage) string {
	if u == nil {
		return "-"
	}
	calls := fmt.Sprintf("%d calls", u.LLMCalls)
	if u.LLMCalls == 1 {
		calls = "1 call"
	}
	cell := fmt.Sprintf("%s, %d/%d tok", calls, u.InputTokens, u.OutputTokens)
	if u.AudioDurationSeconds > 0 {
		cell += fmt.Sprintf(", %ss audio", strconv.FormatFloat(u.AudioDurationSeconds, 'f', -1, 64))
	}
	return cell
}

// taskCell replaces control characters so a server-supplied value cannot break
// the column layout of the text table. tabwriter treats \t, \v, and \f as cell
// or line boundaries, so map every C0 control and DEL to a space.
func taskCell(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
}
