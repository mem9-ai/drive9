//go:build windows

package cli

func prepareSupervisedMountView(req mountSuperviseStartRequest) (mountSuperviseStartRequest, error) {
	return req, nil // FUSE is rejected by the Windows bridge.
}
