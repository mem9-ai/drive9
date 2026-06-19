//go:build windows

package fuse

type mountControlServer struct{}

func startMountControlServer(mountPoint string, fs *Dat9FS) (*mountControlServer, error) {
	return nil, nil
}

func (s *mountControlServer) SocketPath() string {
	return ""
}

func (s *mountControlServer) Close() {}
