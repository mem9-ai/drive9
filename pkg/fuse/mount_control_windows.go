//go:build windows

package fuse

type mountControlServer struct{}

func startMountControlServer(mountPoint string, fs *FS) (*mountControlServer, error) {
	return nil, nil
}

func (s *mountControlServer) SocketPath() string {
	return ""
}

func (s *mountControlServer) Close() {}
