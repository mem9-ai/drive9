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
	"github.com/mem9-ai/drive9/pkg/journal"
	"github.com/mem9-ai/drive9/pkg/layerfs"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
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
	case "fork":
		return LayerFork(c, args[1:])
	case "chain":
		return LayerChain(c, args[1:])
	case "delete", "rm":
		return LayerDelete(c, args[1:])
	case "checkout":
		return LayerCheckout(c, args[1:])
	default:
		return fmt.Errorf("unknown fs layer command %q", args[0])
	}
}

func layerUsage() string {
	return "usage: drive9 fs layer <create|list|status|diff|checkpoint|rollback|commit|fork|chain|delete|checkout>"
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
	if layer.ParentLayerID != "" {
		_, _ = fmt.Fprintf(tw, "parent_layer_id\t%s\n", layer.ParentLayerID)
		_, _ = fmt.Fprintf(tw, "origin_seq\t%d\n", layer.OriginSeq)
		_, _ = fmt.Fprintf(tw, "depth\t%d\n", layer.Depth)
		_, _ = fmt.Fprintf(tw, "root_layer_id\t%s\n", layer.RootLayerID)
	}
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
	layer, err := c.GetFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	snap, err := layerfs.LoadSnapshot(context.Background(), c, layerfs.LayerRoot(layer.LayerID))
	if err != nil {
		return err
	}
	type diffRow struct {
		Seq     int    `json:"seq"`
		Op      string `json:"op"`
		Kind    string `json:"kind"`
		Mode    uint32 `json:"mode"`
		Size    int64  `json:"size,omitempty"`
		Path    string `json:"path"`
		BaseRev int64  `json:"base_revision,omitempty"`
	}
	rows := make([]diffRow, 0, len(snap.Tree)+len(snap.Blackouts)+len(snap.Attrs))
	seq := 0
	addRow := func(op, kind string, mode uint32, size, baseRev int64, path string) {
		seq++
		rows = append(rows, diffRow{Seq: seq, Op: op, Kind: kind, Mode: mode, Size: size, BaseRev: baseRev, Path: path})
	}
	paths := make([]string, 0, len(snap.Tree))
	for p := range snap.Tree {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		e := snap.Tree[p]
		kind := "file"
		if e.IsDir {
			kind = "dir"
		} else if e.IsSymlink {
			kind = "symlink"
		}
		baseRev := int64(0)
		if a, ok := snap.Cas[p]; ok && a.HasBase {
			baseRev = a.BaseRev
		}
		addRow("upsert", kind, e.Mode, e.Size, baseRev, p)
	}
	attrPaths := make([]string, 0, len(snap.Attrs))
	for p := range snap.Attrs {
		attrPaths = append(attrPaths, p)
	}
	sort.Strings(attrPaths)
	for _, p := range attrPaths {
		addRow("chmod", "file", snap.Attrs[p].Mode, 0, 0, p)
	}
	blackPaths := make([]string, 0, len(snap.Blackouts))
	for p := range snap.Blackouts {
		blackPaths = append(blackPaths, p)
	}
	sort.Strings(blackPaths)
	for _, p := range blackPaths {
		b := snap.Blackouts[p]
		kind := "file"
		if b.IsDir {
			kind = "dir"
		}
		baseRev := int64(0)
		if b.HasBase {
			baseRev = b.BaseRev
		}
		addRow("whiteout", kind, 0, 0, baseRev, p)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(map[string]any{"entries": rows})
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, row := range rows {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%04o\t%s\n", row.Seq, row.Op, row.Kind, row.Mode, row.Path)
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
	layer, err := c.GetFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	cpID := strings.TrimSpace(*checkpointID)
	if cpID == "" {
		cpID = journal.NewID("ckpt")
	}
	// Snapshot first, pin second: a crash leaves an invisible orphan
	// directory, never a checkpoint row without content.
	if err := layerfs.CopySnapshot(context.Background(), c, layerfs.LayerRoot(layer.LayerID), layerfs.CheckpointRoot(layer.LayerID, cpID)); err != nil {
		return fmt.Errorf("snapshot layer tree: %w", err)
	}
	checkpoint, err := c.CheckpointFSLayer(context.Background(), layer.LayerID, client.FSLayerCheckpointRequest{
		CheckpointID: cpID,
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
	layer, err := c.GetFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	result, err := layerfs.Commit(context.Background(), c, layer)
	if err != nil {
		return err
	}
	if len(result.Conflicts) > 0 {
		for _, conflict := range result.Conflicts {
			_, _ = fmt.Fprintf(os.Stderr, "conflict %s: %s (base rev %d, want %d)\n", conflict.Path, conflict.Reason, conflict.BaseRevision, conflict.WantRevision)
		}
		_, _ = fmt.Fprintln(os.Stdout, "conflicted")
		return fmt.Errorf("fs layer commit: %d conflict(s)", len(result.Conflicts))
	}
	_, _ = fmt.Fprintf(os.Stdout, "committed layer=%s applied=%d\n", layer.LayerID, result.Applied)
	return nil
}

func LayerFork(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer fork", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	layerID := fs.String("id", "", "child layer id")
	name := fs.String("name", "", "child layer name")
	checkpoint := fs.String("checkpoint", "", "pin to parent checkpoint id")
	actor := fs.String("actor", "", "actor id")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer fork [flags] <parent-layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer fork [flags] <parent-layer>")
	}
	parent, err := c.GetFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	childID := strings.TrimSpace(*layerID)
	if childID == "" {
		childID = journal.NewID("layer")
	}
	srcBase := layerfs.LayerRoot(parent.LayerID)
	if cp := strings.TrimSpace(*checkpoint); cp != "" {
		pin, err := c.GetFSLayerCheckpoint(context.Background(), cp)
		if err != nil {
			return err
		}
		if pin.LayerID != parent.LayerID {
			return fmt.Errorf("checkpoint %s belongs to layer %s, not %s", pin.CheckpointID, pin.LayerID, parent.LayerID)
		}
		srcBase = layerfs.CheckpointRoot(parent.LayerID, pin.CheckpointID)
	}
	// Copy first, registry row second: a crash leaves an orphan namespace
	// directory (invisible), never a half-forked layer.
	if err := layerfs.CopySnapshot(context.Background(), c, srcBase, layerfs.LayerRoot(childID)); err != nil {
		return fmt.Errorf("fork layer tree: %w", err)
	}
	child, err := c.ForkFSLayer(context.Background(), fs.Arg(0), client.FSLayerForkRequest{
		LayerID:      childID,
		Name:         *name,
		ActorID:      *actor,
		CheckpointID: *checkpoint,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(child)
	}
	_, _ = fmt.Fprintln(os.Stdout, child.LayerID)
	return nil
}

func LayerChain(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer chain", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer chain [flags] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer chain [flags] <layer>")
	}
	chain, err := c.ListFSLayerChain(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(map[string]any{"chain": chain})
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, frame := range chain {
		_, _ = fmt.Fprintf(tw, "%s\tdepth=%d\torigin_seq=%d\tlimit=%d\t%s\t%s\n",
			frame.LayerID, frame.Depth, frame.OriginSeq, frame.LimitSeq, frame.State, frame.Name)
	}
	return tw.Flush()
}

func LayerDelete(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer delete", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cascade := fs.Bool("cascade", false, "abandon descendant layers first")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer delete [flags] <layer>")
		}
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 fs layer delete [flags] <layer>")
	}
	layer, err := c.GetFSLayer(context.Background(), fs.Arg(0))
	if err != nil {
		return err
	}
	if err := c.DeleteFSLayer(context.Background(), fs.Arg(0), *cascade); err != nil {
		return err
	}
	// The registry row is gone; sweep the namespace tree best-effort (a
	// failure leaves an invisible orphan, nothing consumes it).
	if rerr := c.RemoveAllCtx(context.Background(), layerfs.LayerRoot(layer.LayerID)); rerr != nil && !client.IsNotFound(rerr) {
		_, _ = fmt.Fprintf(os.Stderr, "warning: layer tree cleanup failed: %v\n", rerr)
	}
	_, _ = fmt.Fprintln(os.Stdout, "ok")
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

// LayerCheckout hot-switches a live mount's shadow view through its mount
// control socket — no umount required.
func LayerCheckout(c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fs layer checkout", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	checkpoint := fs.String("checkpoint", "", "checkout a checkpoint of the layer (read-only view)")
	live := fs.Bool("live", false, "return the mount to the classic view")
	mountPoint := fs.String("mount", "", "mount point of the live mount to switch (required)")
	timeout := fs.Duration("timeout", mountcontrol.DefaultDrainTimeout, "maximum time to wait for the switch")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return printLayerHelp(fs, "usage: drive9 fs layer checkout [flags] <layer>")
		}
		return err
	}
	layerRef := ""
	if fs.NArg() == 1 {
		layerRef = strings.TrimSpace(fs.Arg(0))
	} else if fs.NArg() != 0 {
		return fmt.Errorf("usage: drive9 fs layer checkout [flags] <layer>")
	}
	if strings.TrimSpace(*mountPoint) == "" {
		return fmt.Errorf("fs layer checkout: --mount is required (the live mount to switch)")
	}
	if !*live && layerRef == "" {
		return fmt.Errorf("fs layer checkout: provide a layer ref or --live")
	}
	if *live && (layerRef != "" || strings.TrimSpace(*checkpoint) != "") {
		return fmt.Errorf("fs layer checkout: --live cannot be combined with a layer or checkpoint ref")
	}
	stateMountPoint := backgroundMountStatePoint(strings.TrimSpace(*mountPoint))
	state, _, err := mountstate.ReadProcessState(stateMountPoint)
	if err != nil {
		return fmt.Errorf("fs layer checkout: read mount state for %s: %w", *mountPoint, err)
	}
	socketPath := strings.TrimSpace(state.ControlSocket)
	if socketPath == "" {
		return fmt.Errorf("fs layer checkout: mount %s does not expose a control socket", *mountPoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
	defer cancel()
	resp, err := mountcontrol.RequestLayerCheckout(ctx, socketPath, *timeout, layerRef, strings.TrimSpace(*checkpoint), *live)
	if err != nil {
		return fmt.Errorf("fs layer checkout: %w", err)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		return enc.Encode(resp)
	}
	_, _ = fmt.Fprintf(os.Stdout, "checkout %s view=%s", resp.MountPoint, resp.View)
	if resp.LayerID != "" {
		_, _ = fmt.Fprintf(os.Stdout, " layer=%s", resp.LayerID)
	}
	if resp.CheckpointID != "" {
		_, _ = fmt.Fprintf(os.Stdout, " checkpoint=%s", resp.CheckpointID)
	}
	if resp.ReadOnly {
		_, _ = fmt.Fprint(os.Stdout, " read-only")
	}
	_, _ = fmt.Fprintln(os.Stdout)
	return nil
}
