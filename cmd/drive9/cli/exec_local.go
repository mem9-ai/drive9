package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	// Credential fingerprints the credential the mount uses; empty when the
	// mount does not record it (for example --no-persist-credentials).
	Credential string
}

// credentialFingerprint identifies a credential without keeping it around.
// Two equal fingerprints mean the same API key, hence the same tenant.
func credentialFingerprint(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(digest[:])
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
			layer := strings.TrimSpace(state.Layer)
			if layer == "" {
				// Supervisor state records the original arguments instead.
				layer = layerFromMountArgs(state.Args)
			}
			return &localMount{
				MountPoint: candidate, RemoteRoot: state.RemoteRoot, Layer: layer,
				Server: state.Server, Rel: filepath.ToSlash(rel), ControlSocket: strings.TrimSpace(state.ControlSocket),
				Credential: credentialFingerprint(firstNonEmpty(state.APIKey, state.Token)),
			}, nil
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return nil, nil
		}
		candidate = parent
	}
}

// mountLooksLive requires the recorded process identity (PID plus creation
// time), so a stale state file whose PID was reused is not mistaken for a
// live mount. State without a creation time is not trusted.
func mountLooksLive(state mountstate.ProcessState) bool {
	return processMatchesIdentity(state.PID, state.CreationTime) ||
		processMatchesIdentity(state.SupervisorPID, state.SupervisorCreationTime)
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
