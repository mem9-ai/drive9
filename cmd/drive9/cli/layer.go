package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func layerUsage() string {
	return "usage: drive9 fs layer <create|list|status|work|renew|seal|merge|cancel|delete|operation> [flags]"
}

type fixedLayerOptions struct {
	action, layer, operation, parent, name, work, after string
	fence                                               int64
	ttl                                                 time.Duration
	validation                                          bool
	limit                                               int
}

// Layer uses fixed-view controls exclusively. A work claim is printed, never
// executed here; query/replay cannot accidentally launch an agent a second time.
func Layer(c *client.Client, args []string) error {
	if len(args) == 0 {
		return errors.New(layerUsage())
	}
	if IsHelpArg(args[0]) {
		_, err := fmt.Fprintln(os.Stdout, layerUsage())
		return err
	}
	opts, help, err := parseFixedLayerOptions(args)
	if err != nil || help {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := runFixedLayerControl(ctx, c, opts)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return err
	}
	if outcome, ok := result.(client.FixedViewOutcome); ok && outcome.State != "DONE" {
		return fmt.Errorf("layer %s operation %s: %s (%s)", opts.layer, opts.operation, outcome.State, outcome.Code)
	}
	return nil
}

func parseFixedLayerOptions(args []string) (fixedLayerOptions, bool, error) {
	o := fixedLayerOptions{action: args[0]}
	f := flag.NewFlagSet("fs layer "+o.action, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	operated := false
	if o.action == "merge" {
		o.validation = true
	}
	switch o.action {
	case "create":
		operated = true
		f.StringVar(&o.parent, "parent", "", "derive from this Layer; omitted captures main")
		f.StringVar(&o.name, "name", "", "display name")
	case "work":
		operated = true
		f.DurationVar(&o.ttl, "ttl", 0, "required work lifetime, 1ms through 1h")
		f.BoolVar(&o.validation, "validation", false, "begin read-only validation after seal")
	case "renew":
		f.DurationVar(&o.ttl, "ttl", 0, "required renewed lifetime, 1ms through 1h")
		f.BoolVar(&o.validation, "validation", false, "renew the validation generation")
	case "seal", "merge", "cancel", "delete":
		operated = true
	case "list":
		f.StringVar(&o.after, "after", "", "continue after the previous next_after value")
		f.IntVar(&o.limit, "limit", 100, "maximum Layers in this page, 1 through 256")
	case "status", "get", "operation":
	default:
		return o, false, fmt.Errorf("unsupported fixed Layer command %q; %s", o.action, layerUsage())
	}
	if operated {
		f.StringVar(&o.operation, "operation", "", "required stable operation ID; retain it after timeout")
	}
	if o.action == "renew" || o.action == "seal" || o.action == "merge" {
		f.StringVar(&o.work, "work", "", "work_id returned by the work claim")
		f.Int64Var(&o.fence, "fence", 0, "fence returned by the work claim")
	}
	if err := f.Parse(normalizeHelpFlags(args[1:], f)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintln(os.Stderr, layerUsage()+"; results are JSON")
			f.SetOutput(os.Stderr)
			f.PrintDefaults()
			return o, true, nil
		}
		return o, false, err
	}
	want := 1
	if o.action == "list" {
		want = 0
	}
	if o.action == "operation" {
		want = 2
	}
	if f.NArg() != want {
		return o, false, fmt.Errorf("layer %s expects %d positional arguments", o.action, want)
	}
	if want > 0 {
		o.layer = f.Arg(0)
	}
	if o.action == "operation" {
		o.operation = f.Arg(1)
	}
	if operated || o.action == "operation" {
		if err := viewprotocol.ValidateOperationID(o.operation); err != nil {
			return o, false, fmt.Errorf("operation ID: %w", err)
		}
	}
	if o.action == "work" || o.action == "renew" {
		if o.ttl < time.Millisecond || o.ttl > time.Hour {
			return o, false, errors.New("--ttl is required and must be between 1ms and 1h")
		}
	}
	if o.action == "renew" || o.action == "seal" || o.action == "merge" {
		if err := (client.ViewBinding{LayerID: o.layer, WorkID: o.work, Fence: o.fence, Validation: o.validation}).Validate(); err != nil || o.layer == "" {
			return o, false, errors.New("--work and --fence must identify the claimed Layer generation")
		}
	}
	return o, false, nil
}

func runFixedLayerControl(ctx context.Context, c *client.Client, o fixedLayerOptions) (any, error) {
	work := client.ViewWork{LayerID: o.layer, ID: o.work, Fence: o.fence, Validation: o.validation}
	switch o.action {
	case "create":
		return c.CreateFixedView(ctx, client.FixedViewCreate{OperationID: o.operation, LayerID: o.layer, ParentLayerID: o.parent, Name: o.name})
	case "list":
		return c.ListFixedViews(ctx, o.after, o.limit)
	case "status", "get":
		return c.GetFixedView(ctx, o.layer)
	case "work":
		return c.BeginViewWork(ctx, o.layer, o.operation, o.ttl, o.validation)
	case "renew":
		return c.RenewViewWork(ctx, work, o.ttl)
	case "seal":
		return c.SealFixedView(ctx, work, o.operation)
	case "merge":
		return c.MergeFixedView(ctx, work, o.operation)
	case "cancel", "delete":
		return c.EndFixedView(ctx, o.layer, o.operation, o.action == "delete")
	case "operation":
		return c.GetFixedViewOperation(ctx, o.layer, o.operation)
	}
	return nil, errors.New("unsupported fixed Layer control")
}
