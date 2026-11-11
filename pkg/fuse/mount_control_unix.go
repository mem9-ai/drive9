//go:build !windows

package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

type mountControlServer struct {
	lock       *os.File
	view       *mountViewSwitch
	closeOnce  sync.Once
	connMu     sync.Mutex
	conns      map[net.Conn]context.CancelFunc
	handlers   sync.WaitGroup
	fs         *FS
	listener   net.Listener
	socketPath string
	layer      string
	startedAt  time.Time
	done       chan struct{}
	drainMu    sync.Mutex
}

func startMountControlServer(mountPoint, layer string, fs *FS) (*mountControlServer, error) {
	return startMountControlServerWithView(mountPoint, layer, fs, nil)
}

// startMountViewControlServer is used only after the live mount owner has
// assembled the complete switch coordinator and bound its kernel connection.
func startMountViewControlServer(mountPoint string, view *mountViewSwitch) (*mountControlServer, error) {
	if view == nil || view.journal == nil || view.adapter == nil || view.current == nil || view.current.fs == nil || view.validate == nil || view.build == nil || view.notifier == nil || view.fence == nil {
		return nil, errors.New("incomplete mount view control")
	}
	return startMountControlServerWithView(mountPoint, "", view.current.fs, view)
}

func startMountControlServerWithView(mountPoint, layer string, fs *FS, view *mountViewSwitch) (*mountControlServer, error) {
	socketPath := mountstate.ControlSocketPath(mountPoint)
	ln, lock, err := listenMountControl(socketPath)
	if err != nil {
		return nil, err
	}
	s := &mountControlServer{
		view: view, lock: lock, conns: make(map[net.Conn]context.CancelFunc),
		fs:         fs,
		listener:   ln,
		socketPath: socketPath,
		layer:      layer,
		startedAt:  time.Now().UTC(),
		done:       make(chan struct{}),
	}
	go s.serve()
	return s, nil
}

func (s *mountControlServer) SocketPath() string {
	if s == nil {
		return ""
	}
	return s.socketPath
}

func (s *mountControlServer) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
		<-s.done // No Add may race the handlers.Wait below.
		s.connMu.Lock()
		for conn, cancel := range s.conns {
			cancel()
			_ = conn.Close()
		}
		s.connMu.Unlock()
		s.handlers.Wait()
		if s.socketPath != "" {
			_ = os.Remove(s.socketPath)
		}
		if s.lock != nil {
			_ = s.lock.Close()
		}
	})
}

func (s *mountControlServer) serve() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Warn(context.Background(), "mount_control_accept_failed", zap.Error(err))
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.connMu.Lock()
		s.conns[conn] = cancel
		s.connMu.Unlock()
		s.handlers.Add(1)
		go func() {
			defer cancel()
			defer s.handlers.Done()
			defer func() { s.connMu.Lock(); delete(s.conns, conn); s.connMu.Unlock() }()
			s.handleConn(ctx, conn)
		}()
	}
}

func (s *mountControlServer) handleConn(parent context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(mountcontrol.DefaultDrainTimeout + 5*time.Second))
	reader := bufio.NewReaderSize(conn, 64<<10)
	req, err := readMountControlFrame(reader)
	if err != nil {
		s.writeControlError(conn, "bad_request", err)
		return
	}
	op := strings.ToLower(strings.TrimSpace(req.Op))
	if op == "status" || op == "ping" {
		status := s.status(op)
		if err := json.NewEncoder(conn).Encode(status); err != nil {
			return
		}
		if op != "ping" || !status.ViewSwitch {
			return
		}
		req, err = readMountControlFrame(reader)
		if err != nil {
			return
		} // A one-shot liveness client closes here.
		if req.Op != "switch" && req.Op != "switch-operation" {
			s.writeControlError(conn, "unsupported_operation", errors.New("expected negotiated view operation"))
			return
		}
		ctx, cancel := context.WithTimeout(parent, mountcontrol.DefaultDrainTimeout)
		defer cancel()
		var response mountcontrol.SwitchResponse
		if req.Op == "switch" {
			response = s.view.apply(ctx, req.SwitchRequest)
		} else {
			response = s.view.query(req.SwitchRequest)
		}
		_ = json.NewEncoder(conn).Encode(response)
		return // EOF is part of the final switch response protocol.
	}
	if op != "" && op != "drain" {
		s.writeControlError(conn, "unsupported_operation", fmt.Errorf("unsupported mount operation %q", op))
		return
	}
	timeout := req.Timeout()
	_ = conn.SetDeadline(time.Now().Add(timeout + 5*time.Second))
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	var resp mountcontrol.DrainResponse
	if s.view != nil {
		resp = s.view.drain(ctx)
	} else {
		s.drainMu.Lock()
		resp = s.fs.Drain(ctx)
		s.drainMu.Unlock()
	}
	if resp.MountPoint == "" {
		resp.MountPoint = s.mountPoint()
	}
	_ = json.NewEncoder(conn).Encode(resp)
}

type mountControlFrame struct {
	mountcontrol.DrainRequest
	mountcontrol.SwitchRequest
}

func readMountControlFrame(reader *bufio.Reader) (mountControlFrame, error) {
	var req mountControlFrame
	frame, err := reader.ReadSlice('\n')
	if err != nil {
		return req, err
	}
	err = json.Unmarshal(frame, &req)
	return req, err
}

func (s *mountControlServer) writeControlError(conn net.Conn, kind string, err error) {
	resp := mountcontrol.NewDrainResponse(s.mountPoint(), time.Now().UTC())
	resp.Fail(kind, "", err)
	resp.Finish(time.Now().UTC())
	_ = json.NewEncoder(conn).Encode(resp)
}

func (s *mountControlServer) status(op string) mountcontrol.StatusResponse {
	layer := s.layer
	resp := mountcontrol.StatusResponse{OK: true, MountPoint: s.mountPoint(), Op: op, PID: os.Getpid(), StartedAt: s.startedAt, Layer: &layer}
	if s.view == nil {
		return resp
	}
	snapshot, err := s.view.journal.Snapshot()
	if err != nil {
		resp.OK = false
		resp.Layer = nil
		resp.Error = err.Error()
		return resp
	}
	resp.ViewSwitch = true
	resp.MountID = snapshot.MountID
	resp.Generation = snapshot.Generation
	resp.Binding = &snapshot.Binding
	resp.Pending = snapshot.Pending
	layer = snapshot.Binding.LayerID
	return resp
}

// drain serializes with switching and always addresses the selected runtime.
// A recovering mount cannot claim a successful ordinary drain of its old FS.
func (s *mountViewSwitch) drain(ctx context.Context) mountcontrol.DrainResponse {
	resp := mountcontrol.NewDrainResponse("", time.Now().UTC())
	if !s.mu.TryLock() {
		resp.Fail("BUSY", "", errors.New("mount switch in progress"))
		resp.Finish(time.Now().UTC())
		return resp
	}
	defer s.mu.Unlock()
	snapshot, err := s.journal.Snapshot()
	if err != nil || snapshot.Pending != nil || s.pending != nil {
		if err == nil {
			err = mountstate.ErrSwitchPending
		}
		resp.Fail("RECOVERING", "", err)
		resp.Finish(time.Now().UTC())
		return resp
	}
	return s.current.fs.Drain(ctx)
}

func (s *mountControlServer) mountPoint() string {
	if s == nil || s.fs == nil {
		return ""
	}
	return s.fs.mountPoint()
}
