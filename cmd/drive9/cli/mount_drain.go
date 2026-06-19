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

type mountDrainDeps struct {
	readProcessState func(string) (mountstate.ProcessState, string, error)
	requestDrain     func(context.Context, string, time.Duration) (*mountcontrol.DrainResponse, error)
}

func defaultMountDrainDeps() mountDrainDeps {
	return mountDrainDeps{
		readProcessState: mountstate.ReadProcessState,
		requestDrain:     mountcontrol.RequestDrain,
	}
}

func MountDrainCmd(args []string) error {
	return runMountDrain(args, defaultMountDrainDeps())
}

func runMountDrain(args []string, deps mountDrainDeps) error {
	fs := flag.NewFlagSet("mount drain", flag.ExitOnError)
	timeout := fs.Duration("timeout", mountcontrol.DefaultDrainTimeout, "maximum time to wait for pending writes to drain")
	jsonOutput := fs.Bool("json", false, "output drain result as JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 mount drain [--timeout duration] [--json] <mountpoint>\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("drive9 mount drain: expected exactly one mountpoint")
	}
	if *timeout <= 0 {
		return fmt.Errorf("drive9 mount drain: --timeout must be > 0")
	}
	if deps.readProcessState == nil {
		deps.readProcessState = mountstate.ReadProcessState
	}
	if deps.requestDrain == nil {
		deps.requestDrain = mountcontrol.RequestDrain
	}

	mountPoint := fs.Arg(0)
	stateMountPoint := backgroundMountStatePoint(mountPoint)
	state, _, err := deps.readProcessState(stateMountPoint)
	if err != nil {
		return fmt.Errorf("drive9 mount drain: read mount state for %s: %w", mountPoint, err)
	}
	if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE {
		return fmt.Errorf("drive9 mount drain: %s is a %s mount; drain is only supported for FUSE mounts", mountPoint, state.MountKind)
	}
	socketPath := strings.TrimSpace(state.ControlSocket)
	if socketPath == "" {
		return fmt.Errorf("drive9 mount drain: mount %s does not expose a control socket; remount with a drive9 version that supports drain", mountPoint)
	}

	clientTimeout := *timeout + 5*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	resp, err := deps.requestDrain(ctx, socketPath, *timeout)
	if err != nil {
		return fmt.Errorf("drive9 mount drain: %w", err)
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(resp); err != nil {
			return err
		}
	} else {
		if _, err := printMountDrainResponse(resp); err != nil {
			return err
		}
	}
	if !resp.OK {
		if resp.Error != "" {
			return fmt.Errorf("drive9 mount drain: %s", resp.Error)
		}
		return fmt.Errorf("drive9 mount drain: failed")
	}
	return nil
}

func printMountDrainResponse(resp *mountcontrol.DrainResponse) (int, error) {
	if resp == nil {
		return 0, nil
	}
	total := 0
	status := "ok"
	if !resp.OK {
		status = "failed"
	}
	n, err := fmt.Fprintf(os.Stdout, "drain %s: %s duration=%dms\n", status, resp.MountPoint, resp.DurationMS)
	total += n
	if err != nil {
		return total, err
	}
	if !resp.OK {
		n, err = fmt.Fprintf(os.Stdout, "error_kind=%s", resp.ErrorKind)
		total += n
		if err != nil {
			return total, err
		}
		if resp.FailedPath != "" {
			n, err = fmt.Fprintf(os.Stdout, " failed_path=%s", resp.FailedPath)
			total += n
			if err != nil {
				return total, err
			}
		}
		if resp.Error != "" {
			n, err = fmt.Fprintf(os.Stdout, " error=%s", resp.Error)
			total += n
			if err != nil {
				return total, err
			}
		}
		n, err = fmt.Fprintln(os.Stdout)
		total += n
		if err != nil {
			return total, err
		}
	}
	p := resp.Pending
	n, err = fmt.Fprintf(os.Stdout,
		"pending: open_handles=%d dirty_handles=%d commit_queue=%d commit_bytes=%d commit_in_flight=%d commit_delayed=%d commit_conflicts=%d uploader_queued=%d uploader_in_flight=%d uploader_cached=%d\n",
		p.OpenHandles,
		p.DirtyHandles,
		p.CommitQueuePending,
		p.CommitQueueBytes,
		p.CommitQueueInFlight,
		p.CommitQueueDelayed,
		p.CommitQueueConflicts,
		p.UploaderQueued,
		p.UploaderInFlight,
		p.UploaderCached,
	)
	total += n
	if err != nil {
		return total, err
	}
	if resp.FUSEProtocolMajor != 0 {
		n, err = fmt.Fprintf(os.Stdout,
			"native_syncfs_supported=%t fuse_protocol=%d.%d\n",
			resp.NativeSyncFSSupported,
			resp.FUSEProtocolMajor,
			resp.FUSEProtocolMinor,
		)
		total += n
	}
	return total, err
}
