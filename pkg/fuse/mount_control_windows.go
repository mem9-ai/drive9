//go:build windows

package fuse

type mountControlServer struct{}

func startMountControlServer(mountPoint, layer string, fs *FS) (*mountControlServer, error) {
	return nil, nil
}

func (s *mountControlServer) SocketPath() string {
	return ""
}

func (s *mountControlServer) Close() {}
