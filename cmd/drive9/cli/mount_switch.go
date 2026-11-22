package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
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
	asJSON     bool
	autoInit   bool
}

type mountSwitchDeps struct {
	goos        string
	readState   func(string) (mountstate.ProcessState, string, error)
	queryStatus func(context.Context, string, time.Duration) (*mountcontrol.StatusResponse, error)
	switchView  func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error)
	queryView   func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error)
	newOpID     func() (string, error)
}

func mountSwitchCmd(args []string, query bool) error {
	return runMountSwitch(args, query, os.Stdout, mountSwitchDeps{
		goos:        runtime.GOOS,
		readState:   mountstate.ReadProcessState,
		queryStatus: mountcontrol.RequestStatus,
		switchView:  mountcontrol.RequestSwitch,
		queryView:   mountcontrol.QuerySwitch,
		newOpID:     generateSwitchOperationID,
	})
}

func generateSwitchOperationID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "op_" + hex.EncodeToString(b[:]), nil
}

func parseMountSwitchOptions(args []string, query bool) (mountSwitchOptions, error) {
	o := mountSwitchOptions{query: query}
	name := "mount switch"
	if query {
		name = "mount switch-operation"
	}
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(os.Stderr)
	f.StringVar(&o.request.MountID, "mount-id", "", "mount identity from mount status (required for retry/query)")
	f.StringVar(&o.request.OperationID, "operation", "", "stable operation ID (required for retry/query; auto-generated when starting a fresh switch)")
	f.Uint64Var(&o.request.ExpectedGeneration, "generation", 0, "expected mount generation before this switch (required for retry/query)")
	f.StringVar(&o.request.Layer, "layer", "", "required target Layer ref (id, name, tag:k=v, or main)")
	if !query {
		f.BoolVar(&o.request.Recover, "recover", false, "recover from a TARGET_REVOKED pending switch decision")
	}
	f.DurationVar(&o.timeout, "timeout", mountcontrol.DefaultDrainTimeout, "maximum request wait; timeout does not undo a decided switch")
	f.BoolVar(&o.asJSON, "json", true, "emit JSON result")
	f.Usage = func() {
		_, _ = fmt.Fprintf(f.Output(), "usage: drive9 %s [flags] <mountpoint>\n", name)
		f.PrintDefaults()
	}
	if err := f.Parse(normalizeHelpFlags(args, f)); err != nil {
		return o, err
	}
	if f.NArg() != 1 || strings.TrimSpace(o.request.Layer) == "" || o.timeout <= 0 {
		return o, fmt.Errorf("%s requires one mountpoint, --layer, and a positive --timeout", name)
	}
	o.mountPoint = f.Arg(0)

	if query {
		if err := o.request.Validate(); err != nil {
			return o, fmt.Errorf("%s requires --mount-id, --generation, --operation, and --layer: %w", name, err)
		}
		return o, nil
	}
	if o.request.OperationID != "" {
		if o.request.MountID == "" || o.request.ExpectedGeneration == 0 {
			return o, fmt.Errorf("retrying with --operation requires explicit --mount-id and --generation from the original switch")
		}
		if err := o.request.Validate(); err != nil {
			return o, fmt.Errorf("%s invalid request: %w", name, err)
		}
		return o, nil
	}
	if (o.request.MountID == "") != (o.request.ExpectedGeneration == 0) {
		return o, fmt.Errorf("%s requires both --mount-id and --generation when either is set", name)
	}
	if _, _, err := o.request.NormalizedTarget(); err != nil {
		return o, fmt.Errorf("%s invalid --layer: %w", name, err)
	}
	o.autoInit = true
	return o, nil
}

func runMountSwitch(args []string, query bool, output io.Writer, deps mountSwitchDeps) error {
	o, err := parseMountSwitchOptions(args, query)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if deps.goos == "windows" {
		return fmt.Errorf("drive9 mount switch is not supported on windows")
	}
	state, _, err := deps.readState(backgroundMountStatePoint(o.mountPoint))
	if err != nil {
		return fmt.Errorf("mount switch: read mount state: %w", err)
	}
	if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE {
		return fmt.Errorf("mount switch is supported only for FUSE mounts")
	}
	sock := strings.TrimSpace(state.ControlSocket)
	if sock == "" {
		return fmt.Errorf("mount does not expose a control socket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()

	if o.autoInit {
		if o.request.MountID == "" {
			st, serr := deps.queryStatus(ctx, sock, min(o.timeout, 5*time.Second))
			if serr != nil {
				return fmt.Errorf("mount switch: query current mount status: %w", serr)
			}
			if st == nil || !st.OK || st.MountID == "" || st.Generation == 0 {
				return fmt.Errorf("mount does not expose a switchable view identity")
			}
			o.request.MountID = st.MountID
			o.request.ExpectedGeneration = st.Generation
		}
		opID, gerr := deps.newOpID()
		if gerr != nil {
			return fmt.Errorf("mount switch: generate operation id: %w", gerr)
		}
		o.request.OperationID = opID
		if verr := o.request.Validate(); verr != nil {
			return fmt.Errorf("mount switch: invalid resolved request: %w", verr)
		}
	}

	exchange := deps.switchView
	if o.query {
		exchange = deps.queryView
	}
	resp, exchangeErr := exchange(ctx, sock, o.request)
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
