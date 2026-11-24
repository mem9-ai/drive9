//go:build !windows

package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestUmountAndPackUnpackFollowPostSwitchControlStatus(t *testing.T) {
	shortDir, err := os.MkdirTemp("/tmp", "d9swp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	t.Setenv("XDG_RUNTIME_DIR", shortDir)
	t.Setenv("HOME", shortDir)

	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a"
	var (
		srvMu        sync.Mutex
		archives     = map[string][]byte{}
		uploadPaths  []string
		downloadPath string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_, _ = io.WriteString(w, `{"inline_threshold":1048576,"max_upload_bytes":10485760}`)
		case r.Method == http.MethodPost && r.URL.Query().Has("mkdir"):
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"status":"created"}`)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			srvMu.Lock()
			archives[r.URL.Path] = body
			uploadPaths = append(uploadPaths, r.URL.Path)
			srvMu.Unlock()
			w.Header().Set("ETag", `"1"`)
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"revision":1}`)
		case r.Method == http.MethodHead, r.Method == http.MethodGet:
			srvMu.Lock()
			data, ok := archives[r.URL.Path]
			if r.Method == http.MethodGet && ok {
				downloadPath = r.URL.Path
			}
			srvMu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				_, _ = w.Write(data)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	mp := filepath.Join(shortDir, "mnt")
	baseRoot := filepath.Join(shortDir, "local-base")
	require.NoError(t, os.MkdirAll(mp, 0o755))
	require.NoError(t, os.MkdirAll(baseRoot, 0o755))

	layerRoot, err := mountstate.LayerLocalRoot(baseRoot, layerID)
	require.NoError(t, err)
	mustWriteFile(t, filepath.Join(baseRoot, "overlay", "dist/out.txt"), []byte("from-main"), 0o600)
	mustWriteFile(t, filepath.Join(layerRoot, "overlay", "dist/out.txt"), []byte("from-layer"), 0o600)

	var (
		statusMu sync.Mutex
		binding  = viewprotocol.SwitchBinding{LayerID: layerID, BaseRootPath: "/repo", Fence: 1}
		remRoot  = "/repo"
	)
	sockPath := startFakeMountStatusSocket(t, func() mountcontrol.StatusResponse {
		statusMu.Lock()
		defer statusMu.Unlock()
		b := binding
		return mountcontrol.StatusResponse{
			OK:         true,
			MountPoint: mp,
			RemoteRoot: remRoot,
			ViewSwitch: true,
			MountID:    "mnt_live",
			Generation: 2,
			Binding:    &b,
		}
	})

	procState := mountstate.ProcessState{
		PID:            os.Getpid(),
		MountPoint:     mp,
		MountKind:      mountstate.MountKindFUSE,
		Server:         srv.URL,
		CredentialKind: mountstate.CredentialKindAPIKey,
		APIKey:         "test-key-placeholder",
		RemoteRoot:     "/",
		Profile:        "coding-agent",
		LocalRoot:      baseRoot,
		PackPaths:      []string{"dist"},
		ControlSocket:  sockPath,
	}
	_, err = mountstate.WriteProcessState(mp, procState)
	require.NoError(t, err)

	baseClient := client.New(srv.URL, "test-key-placeholder")
	baseClient.Warm(context.Background())
	_, err = resolveMountedGitTarget(mp)
	require.ErrorContains(t, err, "main-view mount")
	require.ErrorContains(t, Pack(baseClient, []string{"--mount", mp, ":/repo/layer.pack", "dist"}), "active fence")
	fencedClient, err := baseClient.WithView(client.ViewBinding{LayerID: layerID, Fence: 1})
	require.NoError(t, err)
	require.NoError(t, Pack(fencedClient, []string{"--mount", mp, ":/repo/layer.pack", "dist"}))
	srvMu.Lock()
	require.Equal(t, []string{"/v1/views/" + layerID + "/v1/fs/repo/layer.pack"}, uploadPaths)
	srvMu.Unlock()

	require.NoError(t, os.RemoveAll(filepath.Join(layerRoot, "overlay", "dist/out.txt")))
	require.NoError(t, Unpack(baseClient, []string{"--mount", mp, ":/repo/layer.pack"}))
	assertFileContent(t, filepath.Join(layerRoot, "overlay", "dist/out.txt"), "from-layer")
	srvMu.Lock()
	require.Equal(t, "/v1/views/"+layerID+"/v1/fs/repo/layer.pack", downloadPath)
	srvMu.Unlock()

	statusMu.Lock()
	binding = viewprotocol.SwitchBinding{LayerID: layerID, BaseRootPath: "/repo", ReadOnly: true}
	statusMu.Unlock()
	err = Pack(baseClient, []string{"--mount", mp, ":/repo/ro.pack", "dist"})
	require.ErrorContains(t, err, "selected layer mount is read-only")

	detached := false
	roDeps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func([]string) error { detached = true; return nil },
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ReadProcessState(mp)
		},
		requestStatus: mountcontrol.RequestStatus,
		packAfterUnmount: func(context.Context, mountstate.ProcessState, []string, []string) error {
			t.Fatal("must not pack read-only post-switch layer")
			return nil
		},
		now: time.Now, sleep: time.Sleep, printErrf: func(string, ...any) {},
	}
	err = runUmount([]string{"--timeout", "0", "--pack", ":/repo/ro.pack", mp}, roDeps)
	require.ErrorContains(t, err, "read-only layer")
	require.False(t, detached)

	require.NoError(t, runUmount([]string{"--timeout", "0", mp}, roDeps))
	require.True(t, detached)
	_, err = mountstate.WriteProcessState(mp, procState)
	require.NoError(t, err)

	statusMu.Lock()
	binding = viewprotocol.SwitchBinding{}
	remRoot = "/"
	statusMu.Unlock()
	srvMu.Lock()
	uploadPaths = nil
	srvMu.Unlock()
	gitTarget, err := resolveMountedGitTarget(mp)
	require.NoError(t, err)
	require.Equal(t, "/", gitTarget.RemoteRoot)
	require.NoError(t, Pack(baseClient, []string{"--mount", mp, ":/main.pack", "dist"}))
	srvMu.Lock()
	require.Equal(t, []string{"/v1/fs/main.pack"}, uploadPaths)
	srvMu.Unlock()
}

func TestMountSwitchCLIPlainTextOutputWhenJSONFalse(t *testing.T) {
	deps := mountSwitchDeps{
		goos: "linux",
		readState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/tmp/fake.sock"}, "/tmp/pid", nil
		},
		queryStatus: func(context.Context, string, time.Duration) (*mountcontrol.StatusResponse, error) {
			b := viewprotocol.SwitchBinding{}
			return &mountcontrol.StatusResponse{OK: true, ViewSwitch: true, MountID: "mnt_txt", Generation: 1, Binding: &b}, nil
		},
		newOpID: func() (string, error) { return "op_txt_1", nil },
		switchView: func(_ context.Context, _ string, req mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
			rcpt := mountstate.SwitchReceipt{
				OperationID:    req.OperationID,
				State:          mountstate.SwitchDone,
				FromGeneration: req.ExpectedGeneration,
				Generation:     req.ExpectedGeneration + 1,
				Target:         viewprotocol.SwitchBinding{LayerID: "layer_01j00000000000000000000001", BaseRootPath: "/", Fence: 1},
			}
			return &mountcontrol.SwitchResponse{MountID: req.MountID, OperationID: req.OperationID, Receipt: &rcpt}, nil
		},
	}

	var out bytes.Buffer
	require.NoError(t, runMountSwitch([]string{"--json=false", "--layer", "L1", "/mnt/ws"}, false, &out, deps))
	text := out.String()
	require.False(t, strings.HasPrefix(strings.TrimSpace(text), "{"))
	require.Contains(t, text, "mount_id: mnt_txt")
	require.Contains(t, text, "state: DONE")
	require.Contains(t, text, "view: layer_01j00000000000000000000001")
}

func TestArchiveCommandsRequireAConfirmedMountView(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", root)
	t.Setenv("HOME", root)
	for _, tc := range []struct {
		name   string
		status mountcontrol.StatusResponse
		want   string
	}{
		{"uncertain", mountcontrol.StatusResponse{Error: "journal uncertain"}, "journal uncertain"},
		{"recovering", mountcontrol.StatusResponse{OK: true, Recovering: true}, "recovering"},
		{"missing_binding", mountcontrol.StatusResponse{OK: true, ViewSwitch: true}, "cannot identify"},
		{"empty_status", mountcontrol.StatusResponse{OK: true}, "cannot identify"},
		{"missing_socket", mountcontrol.StatusResponse{}, "control socket"},
		{"unavailable", mountcontrol.StatusResponse{}, "read mount view"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := t.TempDir()
			socket := filepath.Join(root, "missing.sock")
			if tc.name == "missing_socket" {
				socket = ""
			} else if tc.name != "unavailable" {
				socket = startFakeMountStatusSocket(t, func() mountcontrol.StatusResponse { return tc.status })
			}
			state := mountstate.ProcessState{PID: os.Getpid(), MountKind: mountstate.MountKindFUSE, MountPoint: mp, LayerID: "layer_01j00000000000000000000001", RemoteRoot: "/", LocalRoot: root, ControlSocket: socket, PackPaths: []string{"dist"}}
			_, err := mountstate.WriteProcessState(mp, state)
			require.NoError(t, err)
			c := client.New("http://127.0.0.1:1", "unused")
			require.ErrorContains(t, Pack(c, []string{"--mount", mp, ":/file.pack", "dist"}), tc.want)
			require.ErrorContains(t, Unpack(c, []string{"--mount", mp, ":/file.pack"}), tc.want)
			_, gitErr := resolveMountedGitTarget(mp)
			require.ErrorContains(t, gitErr, tc.want)
			deps := umountDeps{goos: "linux", readProcessState: mountstate.ReadProcessState, requestStatus: mountcontrol.RequestStatus}
			require.ErrorContains(t, runUmount([]string{mp}, deps), tc.want)
		})
	}
}
