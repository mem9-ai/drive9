//go:build windows

package fuse

type mountControlServer struct{}

func startMountControlServer(
	mountPoint string,
	fs *Dat9FS,
	gate *workspaceMutationGate,
	checkpoint mountCheckpointFunc,
) (*mountControlServer, error) {
	return nil, nil
}

func (s *mountControlServer) SocketPath() string {
	return ""
}

func (s *mountControlServer) Close() {}
