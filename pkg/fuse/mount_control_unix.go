//go:build !windows

package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"time"

	"github.com/mem9-ai/dat9/pkg/mountcontrol"
	"github.com/mem9-ai/dat9/pkg/mountstate"
)

type mountControlServer struct {
	fs         *Dat9FS
	listener   net.Listener
	socketPath string
	done       chan struct{}
}

func startMountControlServer(mountPoint string, fs *Dat9FS) (*mountControlServer, error) {
	socketPath := mountstate.ControlSocketPath(mountPoint)
	_ = os.Remove(socketPath)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	_ = os.Chmod(socketPath, 0o600)
	s := &mountControlServer{
		fs:         fs,
		listener:   ln,
		socketPath: socketPath,
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
	_ = s.listener.Close()
	<-s.done
	if s.socketPath != "" {
		_ = os.Remove(s.socketPath)
	}
}

func (s *mountControlServer) serve() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *mountControlServer) handleConn(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(mountcontrol.DefaultDrainTimeout + 5*time.Second))
	var req mountcontrol.DrainRequest
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&req); err != nil {
		resp := mountcontrol.NewDrainResponse(s.mountPoint(), time.Now().UTC())
		resp.Fail("bad_request", "", err)
		resp.Finish(time.Now().UTC())
		_ = json.NewEncoder(conn).Encode(resp)
		return
	}
	timeout := req.Timeout()
	_ = conn.SetDeadline(time.Now().Add(timeout + 5*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp := s.fs.Drain(ctx)
	if resp.MountPoint == "" {
		resp.MountPoint = s.mountPoint()
	}
	_ = json.NewEncoder(conn).Encode(resp)
}

func (s *mountControlServer) mountPoint() string {
	if s == nil || s.fs == nil || s.fs.opts == nil {
		return ""
	}
	return s.fs.opts.MountPoint
}
