package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountpath"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// execLocalSyncTimeout bounds how long `drive9 exec` waits for a local mount
// to upload pending writes before running the command remotely.
const execLocalSyncTimeout = 2 * time.Minute

// localMount is the drive9 FUSE mount that contains the current directory.
type localMount struct {
	MountPoint string
	RemoteRoot string
	Layer      string
	Server     string
	// Rel is the current directory relative to MountPoint ("." at its root).
	Rel           string
	ControlSocket string
}

// findLocalMount walks up from dir to the nearest live drive9 FUSE mount.
// It returns nil when dir is not inside one.
func findLocalMount(dir string) (*localMount, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	candidate := filepath.Clean(absDir)
	for {
		state, _, err := mountstate.ReadProcessState(backgroundMountStatePoint(candidate))
		if err == nil && mountLooksLive(state) {
			if state.MountKind != "" && state.MountKind != mountstate.MountKindFUSE {
				return nil, nil
			}
			if strings.TrimSpace(state.RemoteRoot) == "" {
				return nil, nil
			}
			rel, relErr := filepath.Rel(candidate, absDir)
			if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return nil, nil
			}
			return &localMount{
				MountPoint: candidate, RemoteRoot: state.RemoteRoot, Layer: layerFromMountArgs(state.Args),
				Server: state.Server, Rel: filepath.ToSlash(rel), ControlSocket: strings.TrimSpace(state.ControlSocket),
			}, nil
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return nil, nil
		}
		candidate = parent
	}
}

func mountLooksLive(state mountstate.ProcessState) bool {
	for _, pid := range []int{state.PID, state.WorkerPID, state.SupervisorPID} {
		if pid > 0 && processAlive(pid) {
			return true
		}
	}
	return false
}

func layerFromMountArgs(args []string) string {
	for index, arg := range args {
		if value, ok := strings.CutPrefix(arg, "--layer="); ok {
			return value
		}
		if arg == "--layer" && index+1 < len(args) {
			return args[index+1]
		}
	}
	return ""
}

// remoteCwd maps the current directory inside the mount to a sandbox working
// directory under /workspace.
func (m *localMount) remoteCwd() string {
	if m.Rel == "." || m.Rel == "" {
		return ""
	}
	return m.Rel
}

// workspace is the Drive9 directory mounted at /workspace.
func (m *localMount) workspace() string {
	return mountpath.ToRemote(m.RemoteRoot, "/")
}

// syncLocalMount uploads writes still pending in the local mount so the
// remote command sees exactly what the user sees locally.
func syncLocalMount(ctx context.Context, mount *localMount) error {
	if mount.ControlSocket == "" {
		return errors.New("the mount does not support drain; remount with a newer drive9")
	}
	ctx, cancel := context.WithTimeout(ctx, execLocalSyncTimeout+5*time.Second)
	defer cancel()
	response, err := mountcontrol.RequestDrain(ctx, mount.ControlSocket, execLocalSyncTimeout)
	if err != nil {
		return err
	}
	if !response.OK {
		if response.Error != "" {
			return errors.New(response.Error)
		}
		return fmt.Errorf("pending writes did not finish uploading within %s", execLocalSyncTimeout)
	}
	return nil
}
