package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

type mountSwitchOptions struct {
	request    mountcontrol.SwitchRequest
	mountPoint string
	timeout    time.Duration
	query      bool
}

type mountSwitchDeps struct {
	readState  func(string) (mountstate.ProcessState, string, error)
	switchView func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error)
	queryView  func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error)
}

func mountSwitchCmd(args []string, query bool) error {
	return runMountSwitch(args, query, os.Stdout, mountSwitchDeps{
		readState: mountstate.ReadProcessState, switchView: mountcontrol.RequestSwitch, queryView: mountcontrol.QuerySwitch,
	})
}

func parseMountSwitchOptions(args []string, query bool) (mountSwitchOptions, error) {
	o := mountSwitchOptions{query: query}
	name := "mount switch"
	if query {
		name = "mount switch-operation"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(os.Stderr)
	f.StringVar(&o.request.MountID, "mount-id", "", "original mount identity from mount status")
	f.StringVar(&o.request.OperationID, "operation", "", "required stable operation ID; retain it after timeout")
	f.Uint64Var(&o.request.ExpectedGeneration, "generation", 0, "original mount generation before this switch")
	layer := f.String("layer", "", "required target Layer ID, or main")
	f.StringVar(&o.request.Target.WorkID, "work", "", "work_id from the target Layer's work claim")
	f.Int64Var(&o.request.Target.Fence, "fence", 0, "fence from the same work claim")
	f.BoolVar(&o.request.Target.Validation, "validation", false, "select the read-only validation work generation")
	f.DurationVar(&o.timeout, "timeout", mountcontrol.DefaultDrainTimeout, "maximum request wait; timeout does not undo a switch")
	f.Usage = func() {
		_, _ = fmt.Fprintf(f.Output(), "usage: drive9 %s [flags] <mountpoint>\nResults are JSON. Query with the original IDs, generation and target.\n", name)
		f.PrintDefaults()
	}
	if err := f.Parse(normalizeHelpFlags(args, f)); err != nil {
		return o, err
	}
	if f.NArg() != 1 || *layer == "" || o.timeout <= 0 {
		return o, fmt.Errorf("%s requires one mountpoint, --layer and a positive --timeout", name)
	}
	if *layer == "main" {
		if o.request.Target.WorkID != "" || o.request.Target.Fence != 0 || o.request.Target.Validation {
			return o, fmt.Errorf("main does not accept work binding flags")
		}
	} else {
		o.request.Target.LayerID = *layer
	}
	if err := o.request.Validate(); err != nil {
		return o, fmt.Errorf("%s requires the original --mount-id, --operation, --generation and complete target binding: %w", name, err)
	}
	o.mountPoint = f.Arg(0)
	return o, nil
}

// runMountSwitch never discovers a replacement generation or creates new work.
// The exact original request is required for query and retry, so an ambiguous
// response cannot silently become another switch on another mount incarnation.
func runMountSwitch(args []string, query bool, output io.Writer, deps mountSwitchDeps) error {
	o, err := parseMountSwitchOptions(args, query)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	state, _, err := deps.readState(backgroundMountStatePoint(o.mountPoint))
	if err != nil {
		return fmt.Errorf("mount switch %s/%s: read mount state: %w", o.request.MountID, o.request.OperationID, err)
	}
	if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE {
		return fmt.Errorf("mount switch is supported only for FUSE mounts")
	}
	if strings.TrimSpace(state.ControlSocket) == "" {
		return fmt.Errorf("mount does not expose a control socket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	exchange := deps.switchView
	if o.query {
		exchange = deps.queryView
	}
	resp, exchangeErr := exchange(ctx, state.ControlSocket, o.request)
	result := struct {
		Request  mountcontrol.SwitchRequest   `json:"request"`
		Response *mountcontrol.SwitchResponse `json:"response,omitempty"`
		Unknown  bool                         `json:"unknown"`
		Error    string                       `json:"error,omitempty"`
	}{Request: o.request, Response: resp}
	if exchangeErr != nil {
		result.Error = exchangeErr.Error()
		result.Unknown = true
		var switchErr *mountcontrol.SwitchError
		if errors.As(exchangeErr, &switchErr) {
			result.Unknown = switchErr.Unknown
		}
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return errors.Join(exchangeErr, fmt.Errorf("mount switch %s/%s: write result: %w", o.request.MountID, o.request.OperationID, err))
	}
	return exchangeErr
}
