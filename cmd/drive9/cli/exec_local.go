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

// execSymlinkResolveTimeout bounds resolving the working directory: a wedged
// FUSE endpoint must not hang `drive9 exec`.
const execSymlinkResolveTimeout = time.Second

// findLocalMount walks up from dir to the nearest live drive9 FUSE mount.
// It returns nil when dir is not inside one. A directory reached through a
// symlink is resolved first, so the walk finds the real mountpoint.
func findLocalMount(dir string) (*localMount, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	paths := []string{filepath.Clean(absDir)}
	if resolved, ok := resolveSymlinksBounded(paths[0]); ok && resolved != paths[0] {
		paths = []string{resolved, paths[0]}
	}
	for _, path := range paths {
		mount, err := findLocalMountFrom(path)
		if mount != nil || err != nil {
			return mount, err
		}
	}
	return nil, nil
}

func resolveSymlinksBounded(path string) (string, bool) {
	type result struct {
		path string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resolved, err := filepath.EvalSymlinks(path)
		done <- result{resolved, err}
	}()
	select {
	case r := <-done:
		return r.path, r.err == nil
	case <-time.After(execSymlinkResolveTimeout):
		return "", false
	}
}

func findLocalMountFrom(absDir string) (*localMount, error) {
	candidate := absDir
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
				MountPoint: candidate, RemoteRoot: state.RemoteRoot,
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

// execMountStatusTimeout bounds asking a local mount which layer it shows.
const execMountStatusTimeout = 5 * time.Second

// localMountLayer asks the running mount for the resolved layer ID it serves
// ("" for the base workspace). A mount too old to report it is an error.
func localMountLayer(ctx context.Context, mount *localMount) (string, error) {
	if mount.ControlSocket == "" {
		return "", errors.New("the mount has no control socket")
	}
	status, err := mountcontrol.RequestStatus(ctx, mount.ControlSocket, execMountStatusTimeout)
	if err != nil {
		return "", err
	}
	if status.Layer == nil {
		return "", errors.New("the mount does not report its layer")
	}
	return *status.Layer, nil
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
