package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

type mountCheckpointDeps struct {
	readProcessState  func(string) (mountstate.ProcessState, string, error)
	requestCheckpoint func(context.Context, string, string, time.Duration) (*mountcontrol.DrainResponse, error)
}

func defaultMountCheckpointDeps() mountCheckpointDeps {
	return mountCheckpointDeps{
		readProcessState:  mountstate.ReadProcessState,
		requestCheckpoint: mountcontrol.RequestCheckpoint,
	}
}

func MountCheckpointCmd(args []string) error {
	return runMountCheckpoint(args, defaultMountCheckpointDeps())
}

func runMountCheckpoint(args []string, deps mountCheckpointDeps) error {
	fs := flag.NewFlagSet("mount checkpoint", flag.ExitOnError)
	checkpointID := fs.String("checkpoint-id", "", "stable identity for the LayerFS checkpoint")
	timeout := fs.Duration("timeout", mountcontrol.DefaultDrainTimeout, "maximum time to quiesce, drain, create, and verify the checkpoint")
	jsonOutput := fs.Bool("json", false, "output checkpoint result as JSON")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: drive9 mount checkpoint --checkpoint-id ID [--timeout duration] [--json] <mountpoint>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("drive9 mount checkpoint: expected exactly one mountpoint")
	}
	*checkpointID = strings.TrimSpace(*checkpointID)
	if *checkpointID == "" {
		return fmt.Errorf("drive9 mount checkpoint: --checkpoint-id is required")
	}
	if *timeout <= 0 {
		return fmt.Errorf("drive9 mount checkpoint: --timeout must be > 0")
	}
	if deps.readProcessState == nil {
		deps.readProcessState = mountstate.ReadProcessState
	}
	if deps.requestCheckpoint == nil {
		deps.requestCheckpoint = mountcontrol.RequestCheckpoint
	}

	mountPoint := fs.Arg(0)
	stateMountPoint := backgroundMountStatePoint(mountPoint)
	state, _, err := deps.readProcessState(stateMountPoint)
	if err != nil {
		return fmt.Errorf("drive9 mount checkpoint: read mount state for %s: %w", mountPoint, err)
	}
	if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE {
		return fmt.Errorf("drive9 mount checkpoint: %s is a %s mount; checkpoint is only supported for FUSE mounts", mountPoint, state.MountKind)
	}
	socketPath := strings.TrimSpace(state.ControlSocket)
	if socketPath == "" {
		return fmt.Errorf("drive9 mount checkpoint: mount %s does not expose a control socket; remount with a drive9 version that supports checkpoint control", mountPoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
	defer cancel()
	resp, err := deps.requestCheckpoint(ctx, socketPath, *checkpointID, *timeout)
	if err != nil {
		return fmt.Errorf("drive9 mount checkpoint: %w", err)
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(resp); err != nil {
			return err
		}
	} else {
		if _, err := printMountCheckpointResponse(resp); err != nil {
			return err
		}
	}
	if !resp.OK {
		if resp.Error != "" {
			return fmt.Errorf("drive9 mount checkpoint: %s", resp.Error)
		}
		return fmt.Errorf("drive9 mount checkpoint: failed")
	}
	if resp.Checkpoint == nil {
		return fmt.Errorf("drive9 mount checkpoint: mount returned success without a checkpoint")
	}
	return nil
}

func printMountCheckpointResponse(resp *mountcontrol.DrainResponse) (int, error) {
	if resp == nil {
		return 0, nil
	}
	if !resp.OK || resp.Checkpoint == nil {
		return printMountDrainResponse(resp)
	}
	return fmt.Fprintf(
		os.Stdout,
		"checkpoint ok: %s layer=%s durable_seq=%d mount=%s duration=%dms\n",
		resp.Checkpoint.CheckpointID,
		resp.Checkpoint.LayerID,
		resp.Checkpoint.DurableSeq,
		resp.MountPoint,
		resp.DurationMS,
	)
}
