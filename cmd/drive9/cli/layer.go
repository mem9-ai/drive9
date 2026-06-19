package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

func Layer(c *client.Client, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", layerUsage())
	}
	if IsHelpArg(args[0]) {
		_, _ = fmt.Fprintln(os.Stdout, layerUsage())
		return nil
	}
	switch args[0] {
	case "create":
		return LayerCreate(c, args[1:])
	case "list", "ls":
		return LayerList(c, args[1:])
	case "status", "get":
		return LayerStatus(c, args[1:])
	case "diff":
		return LayerDiff(c, args[1:])
	case "checkpoint":
		return LayerCheckpoint(c, args[1:])
	case "rollback":
		return LayerRollback(c, args[1:])
	case "commit":
		return LayerCommit(c, args[1:])
	default:
		return fmt.Errorf("unknown fs layer command %q", args[0])
	}
}

func layerUsage() string {
	return "usage: drive9 fs layer <create|list|status|diff|checkpoint|rollback|commit>"
}

func LayerCreate(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var tags repeatStrings
	layerID := fs.String("id", "", "layer id")
	name := fs.String("name", "", "layer name")
	durability := fs.String("durability", "", "restore-safe, write-through, or local-fast (V1 records the mode; FUSE uses restore-safe behavior)")
	actor := fs.String("actor", "", "actor id")
	asJSON := fs.Bool("json", false, "print JSON")
	fs.Var(&tags, "tag", "layer tag key=value (repeatable)")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer create [flags] <base-root>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer create [flags] <base-root>")
	}
	parsedTags, err := parseLayerTags(tags)
	if err != nil {
		return err
	}
	baseRoot := fs.Arg(0)
	if rp, ok := ParseRemote(baseRoot); ok {
		if rp.Context != "" {
			return fmt.Errorf("fs layer create: context-scoped remote sources (e.g. %s:/path) are not yet supported", rp.Context)
		}
		baseRoot = rp.Path
	}
	req := client.FSLayerCreateRequest{
		LayerID:        *layerID,
		BaseRootPath:   baseRoot,
		Name:           *name,
		Tags:           parsedTags,
		DurabilityMode: *durability,
		ActorID:        *actor,
	}
	layer, err := c.CreateFSLayer(context.Background(), req)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(layer)
	}
	_, _ = fmt.Fprintln(os.Stdout, layer.LayerID)
	return nil
}

func LayerList(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer list [flags]")
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: drive9 fs layer list [flags]")
	}
	layers, err := c.ListFSLayers(context.Background())
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(map[string]any{"layers": layers})
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, layer := range layers {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", layer.LayerID, layer.State, layer.DurabilityMode, layer.BaseRootPath, layer.Name, formatLayerTags(layer.Tags))
	}
	return tw.Flush()
}

func LayerStatus(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer status [flags] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer status [flags] <layer>")
	}
	layer, err := c.GetFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(layer)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "layer_id\t%s\n", layer.LayerID)
	_, _ = fmt.Fprintf(tw, "state\t%s\n", layer.State)
	_, _ = fmt.Fprintf(tw, "durability\t%s\n", layer.DurabilityMode)
	_, _ = fmt.Fprintf(tw, "base_root\t%s\n", layer.BaseRootPath)
	_, _ = fmt.Fprintf(tw, "name\t%s\n", layer.Name)
	_, _ = fmt.Fprintf(tw, "durable_seq\t%d\n", layer.DurableSeq)
	if layer.ActorID != "" {
		_, _ = fmt.Fprintf(tw, "actor\t%s\n", layer.ActorID)
	}
	if tags := formatLayerTags(layer.Tags); tags != "" {
		_, _ = fmt.Fprintf(tw, "tags\t%s\n", tags)
	}
	return tw.Flush()
}

func LayerDiff(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer diff", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer diff [flags] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer diff [flags] <layer>")
	}
	entries, err := c.DiffFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(map[string]any{"entries": entries})
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, entry := range entries {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%04o\t%s\n", entry.EntrySeq, entry.Op, entry.Kind, entry.Mode, entry.Path)
	}
	return tw.Flush()
}

func LayerCheckpoint(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer checkpoint", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	checkpointID := fs.String("id", "", "checkpoint id")
	label := fs.String("label", "", "checkpoint label")
	_ = fs.Bool("wait", false, "wait for durable checkpoint")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer checkpoint [flags] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer checkpoint [flags] <layer>")
	}
	checkpoint, err := c.CheckpointFSLayer(context.Background(), fs.Arg(0), client.FSLayerCheckpointRequest{
		CheckpointID: *checkpointID,
		Label:        *label,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(checkpoint)
	}
	_, _ = fmt.Fprintln(os.Stdout, checkpoint.CheckpointID)
	return nil
}

func LayerRollback(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer rollback", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer rollback <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer rollback <layer>")
	}
	if err := c.RollbackFSLayer(context.Background(), fs.Arg(0)); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(os.Stdout, "ok")
	return nil
}

func LayerCommit(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer commit", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer commit <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer commit <layer>")
	}
	result, err := c.CommitFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s layer=%s applied=%d\n", result.Status, result.LayerID, result.Applied)
	return nil
}

func printLayerHelp(fs *flag.FlagSet, lines ...string) error {
	for _, line := range lines {
		_, _ = fmt.Fprintln(os.Stderr, line)
	}
	_, _ = fmt.Fprintln(os.Stderr, "flags:")
	fs.SetOutput(os.Stderr)
	fs.PrintDefaults()
	return nil
}

func parseLayerTags(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(values))
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("invalid layer tag %q (expected key=value)", raw)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("invalid layer tag %q (empty key)", raw)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate layer tag %q", key)
		}
		out[key] = strings.TrimSpace(value)
	}
	return out, nil
}

func formatLayerTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+tags[key])
	}
	return strings.Join(parts, ",")
}
