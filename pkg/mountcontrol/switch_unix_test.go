//go:build !windows

package mountcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func switchFixture() (SwitchRequest, SwitchResponse) {
	req := SwitchRequest{MountID: "mount-1", OperationID: "operation-1", ExpectedGeneration: 1,
		Target: viewprotocol.Binding{LayerID: "layer-1", WorkID: "work-1", Fence: 1}}
	resp := SwitchResponse{MountID: req.MountID, OperationID: req.OperationID,
		Receipt: &mountstate.SwitchReceipt{OperationID: req.OperationID, FromGeneration: 1, Generation: 2, Target: req.Target, State: "DONE"}}
	return req, resp
}

// socketSwitchFixture exercises actual JSON and local stream failures without
// starting a FUSE mount, backend server or database.
func socketSwitchFixture(t *testing.T, handler func(net.Conn)) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "d9switch-")
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); defer func() { _ = conn.Close() }(); handler(conn) }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); wg.Wait(); _ = os.RemoveAll(dir) })
	return socket
}

func TestSwitchControlReplies(t *testing.T) {
	for _, name := range []string{"done", "main", "recovering", "busy", "unknown", "wrong-mount", "wrong-op", "wrong-target", "wrong-generation", "missing", "truncated", "oversized", "second-json", "tail-garbage", "tail-oversized", "invalid-state"} {
		t.Run(name, func(t *testing.T) {
			req, resp := switchFixture()
			switch name {
			case "main":
				req.Target = viewprotocol.Binding{}
				resp.Receipt.Target = req.Target
			case "recovering":
				resp.Receipt.State = "RECOVERING"
			case "busy":
				resp.Receipt = nil
				resp.ErrorKind = "BUSY"
			case "unknown":
				resp.Receipt = nil
				resp.ErrorKind = "UNKNOWN"
			case "wrong-mount":
				resp.MountID = "other"
			case "wrong-op":
				resp.Receipt.OperationID = "other"
			case "wrong-target":
				resp.Receipt.Target = viewprotocol.Binding{}
			case "wrong-generation":
				resp.Receipt.Generation++
			case "missing":
				resp.Receipt = nil
			case "invalid-state":
				resp.Receipt.State = "FAILED"
			}
			ops := make(chan string, 4)
			socket := socketSwitchFixture(t, func(conn net.Conn) {
				var wire struct {
					Op string
					SwitchRequest
				}
				if err := json.NewDecoder(conn).Decode(&wire); err != nil {
					t.Error(err)
					return
				}
				ops <- wire.Op
				if wire.Op == "ping" {
					_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: true, MountID: req.MountID})
					if err := json.NewDecoder(conn).Decode(&wire); err != nil {
						t.Error(err)
						return
					}
					ops <- wire.Op
				}
				if wire.Op != "switch" || wire.SwitchRequest != req {
					t.Errorf("changed request: %+v", wire)
				}
				switch name {
				case "truncated":
					_, _ = conn.Write([]byte(`{"mount_id":`))
				case "oversized":
					_, _ = conn.Write([]byte(`{"error":"` + strings.Repeat("x", 65<<10)))
				case "second-json", "tail-garbage", "tail-oversized":
					_ = json.NewEncoder(conn).Encode(resp)
					tail := "{}"
					if name == "tail-garbage" {
						tail = "garbage"
					}
					if name == "tail-oversized" {
						tail = strings.Repeat(" ", 65<<10)
					}
					_, _ = conn.Write([]byte(tail))
				default:
					_ = json.NewEncoder(conn).Encode(resp)
				}
			})
			got, err := RequestSwitch(t.Context(), socket, req)
			if name == "done" || name == "main" {
				if err != nil || got == nil || got.Receipt.State != "DONE" {
					t.Fatalf("reply=%+v err=%v", got, err)
				}
			} else {
				var failure *SwitchError
				if !errors.As(err, &failure) || failure.MountID != req.MountID || failure.OperationID != req.OperationID || failure.Unknown != (name != "busy") {
					t.Fatalf("lost identity/outcome: %+v %v", got, err)
				}
				if name == "recovering" && (!errors.Is(err, ErrSwitchRecovering) || got == nil || got.Receipt.State != "RECOVERING") {
					t.Fatal("lost durable target")
				}
			}
			if len(ops) != 2 || <-ops != "ping" || <-ops != "switch" {
				t.Fatal("unexpected retry or operation")
			}
		})
	}
}

func TestSwitchControlUnknownThenQueryOriginal(t *testing.T) {
	req, resp := switchFixture()
	ops := make(chan string, 8)
	socket := socketSwitchFixture(t, func(conn net.Conn) {
		var wire struct {
			Op string
			SwitchRequest
		}
		if err := json.NewDecoder(conn).Decode(&wire); err != nil {
			t.Error(err)
			return
		}
		ops <- wire.Op
		if wire.Op == "ping" {
			_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: true, MountID: req.MountID})
			if err := json.NewDecoder(conn).Decode(&wire); err != nil {
				t.Error(err)
				return
			}
			ops <- wire.Op
		}
		if wire.SwitchRequest != req {
			t.Errorf("new identity on recovery: %+v", wire)
		}
		if wire.Op == "switch" {
			return
		} // decision made, response lost
		_ = json.NewEncoder(conn).Encode(resp)
	})
	if _, err := RequestSwitch(t.Context(), socket, req); err == nil {
		t.Fatal("lost response reported success")
	}
	got, err := QuerySwitch(t.Context(), socket, req)
	if err != nil || got.Receipt.State != "DONE" {
		t.Fatalf("query=%+v %v", got, err)
	}
	for _, want := range []string{"ping", "switch", "ping", "switch-operation"} {
		if got := <-ops; got != want {
			t.Fatalf("op=%s want=%s", got, want)
		}
	}
	if len(ops) != 0 {
		t.Fatal("recovery retried a mutation")
	}
}

func TestSwitchControlRejectsUnsupportedAndWrongMountBeforeMutation(t *testing.T) {
	for _, supports := range []bool{false, true} {
		req, _ := switchFixture()
		ops := make(chan string, 4)
		socket := socketSwitchFixture(t, func(conn net.Conn) {
			var wire struct{ Op string }
			if err := json.NewDecoder(conn).Decode(&wire); err != nil {
				t.Error(err)
				return
			}
			ops <- wire.Op
			_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: supports, MountID: "new-incarnation"})
		})
		if _, err := RequestSwitch(t.Context(), socket, req); err == nil {
			t.Fatal("unsupported/wrong mount accepted")
		}
		if len(ops) != 1 || <-ops != "ping" {
			t.Fatal("mutation sent to wrong mount")
		}
	}
}

func TestSwitchControlCancellationPreservesIdentity(t *testing.T) {
	req, _ := switchFixture()
	received, release := make(chan struct{}), make(chan struct{})
	socket := socketSwitchFixture(t, func(conn net.Conn) {
		var wire struct{ Op string }
		if err := json.NewDecoder(conn).Decode(&wire); err != nil {
			t.Error(err)
			return
		}
		if wire.Op == "ping" {
			_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: true, MountID: req.MountID})
			if err := json.NewDecoder(conn).Decode(&wire); err != nil {
				t.Error(err)
				return
			}
		}
		close(received)
		<-release
	})
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := RequestSwitch(ctx, socket, req); done <- err }()
	<-received
	cancel()
	select {
	case err := <-done:
		var failure *SwitchError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &failure) || !failure.Unknown || failure.OperationID != req.OperationID {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt response read")
	}
}

func TestSwitchControlInvalidInputBeforeDial(t *testing.T) {
	for _, name := range []string{"mount", "operation", "generation", "overflow", "target"} {
		req, _ := switchFixture()
		switch name {
		case "mount":
			req.MountID = ""
		case "operation":
			req.OperationID = ""
		case "generation":
			req.ExpectedGeneration = 0
		case "overflow":
			req.ExpectedGeneration = math.MaxUint64
		case "target":
			req.Target.Fence = 0
		}
		_, err := RequestSwitch(t.Context(), "/does-not-exist", req)
		var failure *SwitchError
		if !errors.As(err, &failure) || failure.Unknown {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestSwitchControlCancellationDuringStatus(t *testing.T) {
	req, _ := switchFixture()
	received, release := make(chan struct{}), make(chan struct{})
	socket := socketSwitchFixture(t, func(conn net.Conn) {
		var wire struct{ Op string }
		if err := json.NewDecoder(conn).Decode(&wire); err != nil {
			t.Error(err)
			return
		}
		if wire.Op != "ping" {
			t.Errorf("op=%s", wire.Op)
		}
		close(received)
		<-release
	})
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := RequestSwitch(ctx, socket, req); done <- err }()
	<-received
	cancel()
	select {
	case err := <-done:
		var failure *SwitchError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &failure) || !failure.Unknown || failure.OperationID != req.OperationID {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt status read")
	}
}

func TestSwitchControlKeepsNegotiatedConnectionAfterSocketReplacement(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(map[bool]string{false: "switch", true: "query"}[query], func(t *testing.T) {
			req, resp := switchFixture()
			legacyCalls := make(chan string, 4)
			var socket string
			socket = socketSwitchFixture(t, func(conn net.Conn) {
				var wire struct {
					Op string
					SwitchRequest
				}
				decoder := json.NewDecoder(conn)
				if err := decoder.Decode(&wire); err != nil {
					t.Error(err)
					return
				}
				if wire.Op != "ping" {
					t.Errorf("initial op=%s", wire.Op)
					return
				}
				// A replacement daemon owns this path before negotiation returns.
				// It implements the old fallthrough-to-drain behavior.
				if err := os.Remove(socket); err != nil {
					t.Error(err)
					return
				}
				legacy, err := net.Listen("unix", socket)
				if err != nil {
					t.Error(err)
					return
				}
				stopped := make(chan struct{})
				t.Cleanup(func() { _ = legacy.Close(); <-stopped })
				go func() {
					defer close(stopped)
					c, err := legacy.Accept()
					if err != nil {
						return
					}
					defer func() { _ = c.Close() }()
					var old DrainRequest
					if err := json.NewDecoder(c).Decode(&old); err != nil {
						t.Error(err)
						return
					}
					legacyCalls <- old.Op
					_ = json.NewEncoder(c).Encode(DrainResponse{OK: true})
				}()
				_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: true, MountID: req.MountID})
				if err := decoder.Decode(&wire); err != nil {
					// A regressed client closes this negotiated connection and
					// sends the operation to the replacement listener instead.
					return
				}
				want := "switch"
				if query {
					want = "switch-operation"
				}
				if wire.Op != want || wire.SwitchRequest != req {
					t.Errorf("request=%+v", wire)
				}
				_ = json.NewEncoder(conn).Encode(resp)
			})
			var got *SwitchResponse
			var err error
			if query {
				got, err = QuerySwitch(t.Context(), socket, req)
			} else {
				got, err = RequestSwitch(t.Context(), socket, req)
			}
			if err != nil || got == nil || got.Receipt.State != "DONE" {
				t.Fatalf("reply=%+v err=%v", got, err)
			}
			if len(legacyCalls) != 0 {
				t.Fatalf("legacy daemon received %s", <-legacyCalls)
			}
		})
	}
}

func TestSwitchControlCancellationAfterValidPrefix(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(map[bool]string{false: "switch", true: "query"}[query], func(t *testing.T) {
			req, resp := switchFixture()
			prefixSent, release := make(chan struct{}), make(chan struct{})
			socket := socketSwitchFixture(t, func(conn net.Conn) {
				var wire struct{ Op string }
				decoder := json.NewDecoder(conn)
				if err := decoder.Decode(&wire); err != nil {
					t.Error(err)
					return
				}
				_ = json.NewEncoder(conn).Encode(StatusResponse{OK: true, ViewSwitch: true, MountID: req.MountID})
				if err := decoder.Decode(&wire); err != nil {
					t.Error(err)
					return
				}
				_ = json.NewEncoder(conn).Encode(resp)
				close(prefixSent)
				<-release // no EOF; the supposedly complete JSON is only a prefix
			})
			t.Cleanup(func() { close(release) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if query {
					_, err = QuerySwitch(ctx, socket, req)
				} else {
					_, err = RequestSwitch(ctx, socket, req)
				}
				done <- err
			}()
			<-prefixSent
			cancel()
			select {
			case err := <-done:
				var failure *SwitchError
				if !errors.Is(err, context.Canceled) || !errors.As(err, &failure) || !failure.Unknown || failure.OperationID != req.OperationID {
					t.Fatalf("prefix reported complete: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancel did not interrupt final framing")
			}
		})
	}
}
