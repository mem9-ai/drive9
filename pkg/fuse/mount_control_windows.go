//go:build windows

package fuse

import "github.com/mem9-ai/drive9/pkg/extensions/layer"

type mountControlServer struct{}

func startMountControlServer(mountPoint string, fs *FS, layerCtl *layer.Layer) (*mountControlServer, error) {
	return nil, nil
}

func (s *mountControlServer) SocketPath() string {
	return ""
}

func (s *mountControlServer) Close() {}
