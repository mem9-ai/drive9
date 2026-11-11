package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func mountSwitchTestArgs() []string {
	return []string{"--mount-id", "mount-a", "--operation", "op-a", "--generation", "8", "--layer", "layer-a", "--work", "work-a", "--fence", "3", "/mount"}
}

func mountSwitchTestRequest() mountcontrol.SwitchRequest {
	return mountcontrol.SwitchRequest{MountID: "mount-a", OperationID: "op-a", ExpectedGeneration: 8, Target: viewprotocol.Binding{LayerID: "layer-a", WorkID: "work-a", Fence: 3}}
}

func mountSwitchTestDeps(t *testing.T) mountSwitchDeps {
	t.Helper()
	unexpected := func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
		t.Error("unexpected control request")
		return nil, errors.New("unexpected request")
	}
	return mountSwitchDeps{readState: func(string) (mountstate.ProcessState, string, error) {
		return mountstate.ProcessState{MountKind: mountstate.MountKindFUSE, ControlSocket: "/socket"}, "", nil
	}, switchView: unexpected, queryView: unexpected}
}

func TestMountSwitchPreservesExactIdentityAndSeparatesQuery(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(map[bool]string{false: "switch", true: "query"}[query], func(t *testing.T) {
			deps := mountSwitchTestDeps(t)
			calls := 0
			exchange := func(ctx context.Context, socket string, request mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
				calls++
				if socket != "/socket" || request != mountSwitchTestRequest() {
					t.Fatalf("request changed: %q %+v", socket, request)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing timeout")
				}
				return &mountcontrol.SwitchResponse{MountID: request.MountID, OperationID: request.OperationID, Receipt: &mountstate.SwitchReceipt{OperationID: request.OperationID, FromGeneration: 8, Generation: 9, Target: request.Target, State: mountstate.SwitchDone}}, nil
			}
			if query {
				deps.queryView = exchange
			} else {
				deps.switchView = exchange
			}
			var output bytes.Buffer
			if err := runMountSwitch(mountSwitchTestArgs(), query, &output, deps); err != nil {
				t.Fatal(err)
			}
			var result struct {
				Request  mountcontrol.SwitchRequest
				Response mountcontrol.SwitchResponse
				Unknown  bool
			}
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || result.Request != mountSwitchTestRequest() || result.Unknown || result.Response.Receipt == nil || result.Response.Receipt.State != mountstate.SwitchDone {
				t.Fatalf("result: %+v calls=%d", result, calls)
			}
		})
	}
}

func TestMountSwitchErrorsKeepOriginalRequestAndDoNotRetry(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "lost-reply"}[unknown], func(t *testing.T) {
			deps := mountSwitchTestDeps(t)
			cause := errors.New("control failure")
			calls := 0
			deps.switchView = func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
				calls++
				return nil, &mountcontrol.SwitchError{MountID: "mount-a", OperationID: "op-a", Unknown: unknown, Cause: cause}
			}
			var output bytes.Buffer
			if err := runMountSwitch(mountSwitchTestArgs(), false, &output, deps); !errors.Is(err, cause) {
				t.Fatal(err)
			}
			var result struct {
				Request mountcontrol.SwitchRequest
				Unknown bool
				Error   string
			}
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || result.Request != mountSwitchTestRequest() || result.Unknown != unknown || result.Error == "" {
				t.Fatalf("failure lost identity: %+v calls=%d", result, calls)
			}
		})
	}
}

func TestMountSwitchRecoveringIsPrintedWithoutClaimingSuccess(t *testing.T) {
	deps := mountSwitchTestDeps(t)
	deps.switchView = func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
		return &mountcontrol.SwitchResponse{MountID: "mount-a", OperationID: "op-a", Receipt: &mountstate.SwitchReceipt{State: mountstate.SwitchRecovering}}, &mountcontrol.SwitchError{MountID: "mount-a", OperationID: "op-a", Unknown: true, Cause: mountcontrol.ErrSwitchRecovering}
	}
	var output bytes.Buffer
	if err := runMountSwitch(mountSwitchTestArgs(), false, &output, deps); !errors.Is(err, mountcontrol.ErrSwitchRecovering) || !strings.Contains(output.String(), `"state":"RECOVERING"`) {
		t.Fatalf("result=%s error=%v", &output, err)
	}
}

func TestMountSwitchParsingAndMainBinding(t *testing.T) {
	args := []string{"--mount-id", "mount-a", "--operation", "op-a", "--generation", "8", "--layer", "main", "/mount"}
	o, err := parseMountSwitchOptions(args, false)
	if err != nil || o.request.Target != (viewprotocol.Binding{}) {
		t.Fatalf("main: %+v %v", o, err)
	}
	for _, bad := range [][]string{
		{}, {"/mount"},
		{"--mount-id", "mount-a", "--operation", "op-a", "--generation", "8", "/mount"},
		append([]string{"--timeout", "0s"}, args...),
		append([]string{"--work", "unwanted"}, args...),
		append([]string{"--validation"}, args...),
		append([]string{"--fence", "1"}, args...),
		append(append([]string(nil), args...), "/another"),
		{"--generation", "-1", "/mount"},
		{"--generation", "18446744073709551615", "--mount-id", "mount-a", "--operation", "op-a", "--layer", "main", "/mount"},
		{"--generation", "8", "--mount-id", "mount-a", "--operation", "op-a", "--layer", "layer-a", "/mount"},
	} {
		if _, err := parseMountSwitchOptions(bad, false); err == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
}

func TestMountSwitchStateErrorsNeverSendRequest(t *testing.T) {
	for _, state := range []mountstate.ProcessState{{MountKind: "webdav", ControlSocket: "/socket"}, {MountKind: mountstate.MountKindFUSE}} {
		deps := mountSwitchTestDeps(t)
		deps.readState = func(string) (mountstate.ProcessState, string, error) { return state, "", nil }
		if err := runMountSwitch(mountSwitchTestArgs(), false, io.Discard, deps); err == nil {
			t.Fatal("invalid mount state accepted")
		}
	}
	deps := mountSwitchTestDeps(t)
	cause := errors.New("missing pid file")
	deps.readState = func(string) (mountstate.ProcessState, string, error) { return mountstate.ProcessState{}, "", cause }
	if err := runMountSwitch(mountSwitchTestArgs(), false, io.Discard, deps); !errors.Is(err, cause) || !strings.Contains(err.Error(), "mount-a/op-a") {
		t.Fatal(err)
	}
}

type mountSwitchErrorWriter struct{}

func (mountSwitchErrorWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestMountSwitchOutputFailureRetainsIdentity(t *testing.T) {
	deps := mountSwitchTestDeps(t)
	deps.switchView = func(context.Context, string, mountcontrol.SwitchRequest) (*mountcontrol.SwitchResponse, error) {
		return nil, context.DeadlineExceeded
	}
	err := runMountSwitch(mountSwitchTestArgs(), false, mountSwitchErrorWriter{}, deps)
	if !errors.Is(err, io.ErrClosedPipe) || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "mount-a/op-a") {
		t.Fatal(err)
	}
}

func TestMountSwitchDispatchesHelpWithoutMounting(t *testing.T) {
	for _, command := range []string{"switch", "switch-operation"} {
		if err := MountCmd([]string{command, "--help"}); err != nil {
			t.Fatal(err)
		}
	}
	args := mountSwitchTestArgs()
	original := append([]string(nil), args...)
	if _, err := parseMountSwitchOptions(args, true); err != nil || !reflect.DeepEqual(args, original) {
		t.Fatalf("parser mutated retry input: %v %v", args, err)
	}
}
