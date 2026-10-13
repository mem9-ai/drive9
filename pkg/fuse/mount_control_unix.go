//go:build !windows

package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/extensions/layer"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

type mountControlServer struct {
	fs         *FS
	layerCtl   *layer.Layer
	listener   net.Listener
	socketPath string
	startedAt  time.Time
	done       chan struct{}
	drainMu    sync.Mutex
}

func startMountControlServer(mountPoint string, fs *FS, layerCtl *layer.Layer) (*mountControlServer, error) {
	socketPath := mountstate.ControlSocketPath(mountPoint)
	socketDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDir, 0o700); err != nil {
		return nil, err
	}
	_ = os.Chmod(socketDir, 0o700)
	_ = os.Remove(socketPath)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	_ = os.Chmod(socketPath, 0o600)
	s := &mountControlServer{
		fs:         fs,
		layerCtl:   layerCtl,
		listener:   ln,
		socketPath: socketPath,
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
			logger.Warn(context.Background(), "mount_control_accept_failed", zap.Error(err))
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
	ctx0 := context.Background()
	op := strings.ToLower(strings.TrimSpace(req.Op))
	if op == "status" || op == "ping" {
		started := s.startedAt
		if started.IsZero() {
			started = time.Now().UTC()
		}
		_ = json.NewEncoder(conn).Encode(mountcontrol.StatusResponse{
			OK:         true,
			MountPoint: s.mountPoint(),
			Op:         op,
			PID:        os.Getpid(),
			StartedAt:  started,
		})
		return
	}
	if op == "layer_checkout" {
		s.handleLayerCheckout(conn, ctx0, &req)
		return
	}
	timeout := req.Timeout()
	_ = conn.SetDeadline(time.Now().Add(timeout + 5*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	s.drainMu.Lock()
	resp := s.fs.Drain(ctx)
	s.drainMu.Unlock()
	if resp.MountPoint == "" {
		resp.MountPoint = s.mountPoint()
	}
	_ = resp
	_ = json.NewEncoder(conn).Encode(resp)
}

func (s *mountControlServer) mountPoint() string {
	if s == nil || s.fs == nil {
		return ""
	}
	return s.fs.mountPoint()
}

// handleLayerCheckout hot-switches the mounted shadow (layer, checkpoint,
// or live) through the layer extension.
func (s *mountControlServer) handleLayerCheckout(conn net.Conn, ctx context.Context, req *mountcontrol.DrainRequest) {
	resp := mountcontrol.LayerCheckoutResponse{
		OK:         true,
		MountPoint: s.mountPoint(),
		Op:         "layer_checkout",
	}
	defer func() {
		resp.CompletedAt = time.Now().UTC()
		_ = json.NewEncoder(conn).Encode(resp)
	}()
	if s.layerCtl == nil {
		resp.OK = false
		resp.Error = "layer extension not wired on this mount"
		return
	}
	timeout := req.Timeout()
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > timeout+5*time.Second {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout+5*time.Second)
		defer cancel()
	}
	_ = conn.SetDeadline(time.Now().Add(timeout + 5*time.Second))
	target := layer.CheckoutTarget{
		LayerRef:      strings.TrimSpace(req.LayerRef),
		CheckpointRef: strings.TrimSpace(req.CheckpointRef),
		Live:          req.Live,
	}
	if !target.Live && target.LayerRef == "" {
		resp.OK = false
		resp.Error = "layer checkout: provide a layer ref or --live"
		return
	}
	if target.Live && (target.LayerRef != "" || target.CheckpointRef != "") {
		resp.OK = false
		resp.Error = "layer checkout: --live cannot be combined with a layer or checkpoint ref"
		return
	}
	if err := s.layerCtl.Checkout(ctx, target); err != nil {
		resp.OK = false
		resp.Error = err.Error()
		return
	}
	mode, layerID, checkpointID, readOnly := s.layerCtl.CurrentView()
	// An active shadow view must route directory attrs through the chain;
	// returning to live restores the mount-time default.
	if s.fs != nil {
		s.fs.SetDirAttrsFromInode(mode == "live")
	}
	resp.View = mode
	resp.LayerID = layerID
	resp.CheckpointID = checkpointID
	resp.ReadOnly = readOnly
}
