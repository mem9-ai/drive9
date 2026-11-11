//go:build !windows

package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func startViewControlFixture(t *testing.T, s *mountViewSwitch) *mountControlServer {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "d9vc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("XDG_RUNTIME_DIR", dir)
	t.Setenv("TMPDIR", dir)
	server, err := startMountViewControlServer(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server
}

func TestMountViewControlSwitchStatusAndOriginalQuery(t *testing.T) {
	s, probe, req, _ := viewSwitchFixture(t)
	var failed atomic.Bool
	failed.Store(true)
	probe.inode = func(uint64) gofuse.Status {
		if s.adapter.currentTarget().generation == 2 && failed.Load() {
			return gofuse.EIO
		}
		return gofuse.OK
	}
	server := startViewControlFixture(t, s)
	status, err := mountcontrol.RequestStatus(t.Context(), server.SocketPath(), time.Second)
	if err != nil || !status.ViewSwitch || status.MountID != req.MountID || status.Generation != 1 || status.Binding == nil || status.Binding.LayerID != "" {
		t.Fatalf("status: %+v %v", status, err)
	}
	got, err := mountcontrol.RequestSwitch(t.Context(), server.SocketPath(), req)
	if err == nil || got == nil || got.Receipt == nil || got.Receipt.State != mountstate.SwitchRecovering {
		t.Fatalf("pending: %+v %v", got, err)
	}
	status, err = mountcontrol.RequestStatus(t.Context(), server.SocketPath(), time.Second)
	if err != nil || status.Pending == nil || status.Pending.OperationID != req.OperationID || status.Generation != 2 || *status.Layer != "a" || *status.Binding != req.Target {
		t.Fatalf("pending status: %+v %v", status, err)
	}
	if drained, err := mountcontrol.RequestDrain(t.Context(), server.SocketPath(), time.Second); err != nil || drained.OK || drained.ErrorKind != "RECOVERING" {
		t.Fatalf("drain: %+v %v", drained, err)
	}
	queried, err := mountcontrol.QuerySwitch(t.Context(), server.SocketPath(), req)
	if err != nil || queried.Receipt.State != mountstate.SwitchRecovering {
		t.Fatalf("query advanced work: %+v %v", queried, err)
	}
	failed.Store(false)
	got, err = mountcontrol.RequestSwitch(t.Context(), server.SocketPath(), req)
	if err != nil || got.Receipt.State != mountstate.SwitchDone {
		t.Fatalf("complete: %+v %v", got, err)
	}
	status, err = mountcontrol.RequestStatus(t.Context(), server.SocketPath(), time.Second)
	if err != nil || status.Pending != nil || status.Generation != 2 || status.Binding.LayerID != "a" {
		t.Fatalf("completed status: %+v %v", status, err)
	}
	s.mu.Lock()
	s.current.fs.mountPt = "selected-view"
	s.mu.Unlock()
	if drained, err := mountcontrol.RequestDrain(t.Context(), server.SocketPath(), time.Second); err != nil || !drained.OK || drained.MountPoint != "selected-view" {
		t.Fatalf("wrong drain runtime: %+v %v", drained, err)
	}
}

func TestMountViewControlLostResponseCanQueryOriginal(t *testing.T) {
	s, _, req, _ := viewSwitchFixture(t)
	server := startViewControlFixture(t, s)
	conn, err := net.Dial("unix", server.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(conn).Encode(mountcontrol.DrainRequest{Op: "ping"}); err != nil {
		t.Fatal(err)
	}
	var status mountcontrol.StatusResponse
	if err := json.NewDecoder(conn).Decode(&status); err != nil || !status.ViewSwitch {
		t.Fatalf("ping: %v %+v", err, status)
	}
	// Wait until the handler has recorded DONE, then deliberately abandon
	// its reply. This query never initiates a new switch.
	returned := make(chan struct{})
	originalBuild := s.build
	s.build = func(ctx context.Context, binding viewprotocol.Binding, g mountRuntimeGeneration) (*mountRuntime, error) {
		rt, err := originalBuild(ctx, binding, g)
		close(returned)
		return rt, err
	}
	if err := json.NewEncoder(conn).Encode(struct {
		Op string `json:"op"`
		mountcontrol.SwitchRequest
	}{"switch", req}); err != nil {
		t.Fatal(err)
	}
	<-returned
	_ = conn.Close()
	// Serialize with the actual handler, rather than infer completion from a delay.
	s.mu.Lock()
	pending := s.pending != nil
	s.mu.Unlock()
	if pending {
		t.Fatal("handler did not complete before query")
	}
	got, err := mountcontrol.QuerySwitch(t.Context(), server.SocketPath(), req)
	if err != nil || got.Receipt == nil || got.Receipt.State != mountstate.SwitchDone {
		t.Fatalf("lost reply: %+v %v", got, err)
	}
}

func TestMountViewControlCloseCancelsAndJoinsHandlers(t *testing.T) {
	s, _, req, _ := viewSwitchFixture(t)
	entered, exited := make(chan struct{}), make(chan struct{})
	canceled, release := make(chan struct{}), make(chan struct{})
	s.validate = func(ctx context.Context, _ viewprotocol.Binding) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		close(exited)
		return ctx.Err()
	}
	server := startViewControlFixture(t, s)
	requestDone := make(chan error, 1)
	go func() {
		_, err := mountcontrol.RequestSwitch(t.Context(), server.SocketPath(), req)
		requestDone <- err
	}()
	<-entered
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	<-canceled
	listener, lock, listenErr := listenMountControl(server.SocketPath())
	if listener != nil {
		_ = listener.Close()
	}
	if lock != nil {
		_ = lock.Close()
	}
	close(release)
	<-closed
	if listenErr == nil {
		t.Fatal("control lock released before old handler finished")
	}
	select {
	case <-exited:
	default:
		t.Fatal("Close returned before active handler")
	}
	if err := <-requestDone; err == nil {
		t.Fatal("closed exchange succeeded")
	}
	server.Close()
	snapshot, err := s.journal.Snapshot()
	if err != nil || snapshot.Pending != nil || snapshot.Generation != 1 {
		t.Fatalf("close decided: %+v %v", snapshot, err)
	}
}

func TestMountViewControlRequiresNegotiation(t *testing.T) {
	s, _, req, _ := viewSwitchFixture(t)
	server := startViewControlFixture(t, s)
	conn, err := net.Dial("unix", server.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := json.NewEncoder(conn).Encode(struct {
		Op string `json:"op"`
		mountcontrol.SwitchRequest
	}{"switch", req}); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	var resp mountcontrol.DrainResponse
	if err := json.Unmarshal(data, &resp); err != nil || resp.OK || resp.ErrorKind != "unsupported_operation" {
		t.Fatalf("unnegotiated: %s %v", data, err)
	}
	if snapshot, err := s.journal.Snapshot(); err != nil || snapshot.Generation != 1 || snapshot.Pending != nil {
		t.Fatalf("mutated: %+v %v", snapshot, err)
	}
}

func TestMountControlFrameBoundaries(t *testing.T) {
	for _, value := range []string{strings.Repeat("x", 64<<10) + "\n", "{}{}\n", "{\n"} {
		if _, err := readMountControlFrame(bufio.NewReaderSize(strings.NewReader(value), 64<<10)); err == nil {
			t.Fatal("accepted invalid frame")
		}
	}
	if _, err := readMountControlFrame(bufio.NewReaderSize(strings.NewReader("{}"), 64<<10)); !errors.Is(err, io.EOF) {
		t.Fatalf("missing newline: %v", err)
	}
}
