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
	"time"

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
	case "ls", "list":
		return LayerList(c, args[1:])
	case "inspect", "get", "status":
		return LayerInspect(c, args[1:])
	case "fork":
		return LayerFork(c, args[1:])
	case "merge":
		return LayerMerge(c, args[1:])
	case "rm", "delete":
		return LayerDelete(c, args[1:])
	default:
		return fmt.Errorf("unknown fs layer command %q", args[0])
	}
}

func layerUsage() string {
	return "usage: drive9 fs layer <create|ls|inspect|fork|merge|rm>"
}

func LayerCreate(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var tags repeatStrings
	name := fs.String("name", "", "layer name")
	ttl := fs.String("ttl", "", "optional layer TTL in seconds or duration (e.g. 3600 or 1h)")
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
	ttlSecs, err := parseLayerTTLSeconds(*ttl)
	if err != nil {
		return err
	}
	baseRoot := fs.Arg(0)
	if loc, err := Parse(baseRoot); err != nil {
		return err
	} else if loc.Kind == KindObject {
		return fmt.Errorf("fs layer create: object-store URIs are not supported")
	} else if loc.Kind == KindDrive9 {
		if loc.Context != "" {
			return fmt.Errorf("fs layer create: context-scoped remote sources (e.g. %s:/path) are not yet supported", loc.Context)
		}
		baseRoot = loc.Path
	}
	layer, err := c.CreateLayer(context.Background(), client.LayerCreateRequest{
		BaseRootPath: baseRoot,
		Name:         *name,
		Tags:         parsedTags,
		TTLSeconds:   ttlSecs,
		ActorID:      *actor,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(layer)
	}
	_, _ = fmt.Fprintln(os.Stdout, layer.LayerID)
	return nil
}

func LayerList(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer ls", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	after := fs.String("after", "", "cursor layer_id for pagination")
	limit := fs.Int("limit", 0, "maximum layers per page (1..500)")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer ls [flags]")
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: drive9 fs layer ls [flags]")
	}
	explicitPage := flagProvided(fs, "after") || flagProvided(fs, "limit")
	page, err := c.ListLayers(context.Background(), client.LayerListOptions{
		After: *after,
		Limit: *limit,
	})
	if err != nil {
		return err
	}
	layers := append([]client.LayerInfo(nil), page.Layers...)
	nextAfter := page.NextAfter
	for !explicitPage && nextAfter != "" {
		more, merr := c.ListLayers(context.Background(), client.LayerListOptions{After: nextAfter})
		if merr != nil {
			return merr
		}
		layers = append(layers, more.Layers...)
		nextAfter = more.NextAfter
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(client.LayerListPage{
			Layers:    layers,
			NextAfter: nextAfter,
		})
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, layer := range layers {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\tfence=%d\t%s\t%s\n",
			layer.LayerID, layer.State, layer.BaseRootPath, layer.Fence, layer.Name, formatLayerTags(layer.Tags))
	}
	return tw.Flush()
}

func LayerInspect(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer inspect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer inspect [flags] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer inspect [flags] <layer>")
	}
	layer, err := c.GetLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(layer)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "layer_id\t%s\n", layer.LayerID)
	_, _ = fmt.Fprintf(tw, "state\t%s\n", layer.State)
	_, _ = fmt.Fprintf(tw, "base_root\t%s\n", layer.BaseRootPath)
	_, _ = fmt.Fprintf(tw, "name\t%s\n", layer.Name)
	_, _ = fmt.Fprintf(tw, "snap_gen\t%d\n", layer.SnapGen)
	_, _ = fmt.Fprintf(tw, "fence\t%d\n", layer.Fence)
	if layer.ExclusiveMountID != "" {
		_, _ = fmt.Fprintf(tw, "exclusive_mount_id\t%s\n", layer.ExclusiveMountID)
		_, _ = fmt.Fprintf(tw, "mount_lease_ttl_ms\t%d\n", layer.MountLeaseTTLMS)
		_, _ = fmt.Fprintf(tw, "mount_expired\t%t\n", layer.MountExpired)
	}
	if layer.ParentLayerID != "" {
		_, _ = fmt.Fprintf(tw, "parent_layer_id\t%s\n", layer.ParentLayerID)
	}
	if layer.ActorID != "" {
		_, _ = fmt.Fprintf(tw, "actor\t%s\n", layer.ActorID)
	}
	if layer.ExpiresAt != nil && !layer.ExpiresAt.IsZero() {
		_, _ = fmt.Fprintf(tw, "expires_at\t%s\n", layer.ExpiresAt.UTC().Format(time.RFC3339))
	}
	if tags := formatLayerTags(layer.Tags); tags != "" {
		_, _ = fmt.Fprintf(tw, "tags\t%s\n", tags)
	}
	return tw.Flush()
}

func LayerFork(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer fork", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var tags repeatStrings
	name := fs.String("name", "", "child layer name")
	ttl := fs.String("ttl", "", "optional child layer TTL in seconds or duration (e.g. 3600 or 1h)")
	actor := fs.String("actor", "", "actor id")
	asJSON := fs.Bool("json", false, "print JSON")
	fs.Var(&tags, "tag", "child layer tag key=value (repeatable)")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer fork [flags] <parent-layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer fork [flags] <parent-layer>")
	}
	parsedTags, err := parseLayerTags(tags)
	if err != nil {
		return err
	}
	ttlSecs, err := parseLayerTTLSeconds(*ttl)
	if err != nil {
		return err
	}
	child, err := c.ForkLayer(context.Background(), fs.Arg(0), client.LayerForkRequest{
		Name:       *name,
		Tags:       parsedTags,
		TTLSeconds: ttlSecs,
		ActorID:    *actor,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(child)
	}
	_, _ = fmt.Fprintln(os.Stdout, child.LayerID)
	return nil
}

func LayerMerge(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer merge", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer merge [--json] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer merge [--json] <layer>")
	}
	result, err := c.MergeLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s layer=%s applied_paths=%d\n", result.Status, result.LayerID, result.AppliedPaths)
	return nil
}

func LayerDelete(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer rm", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	force := fs.Bool("force", false, "delete even when exclusively mounted")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer rm [--force] [--json] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer rm [--force] [--json] <layer>")
	}
	result, err := c.DeleteLayer(context.Background(), fs.Arg(0), *force)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s layer=%s\n", result.Status, result.LayerID)
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
		parts = append(parts, fmt.Sprintf("%s=%s", key, tags[key]))
	}
	return strings.Join(parts, ",")
}
