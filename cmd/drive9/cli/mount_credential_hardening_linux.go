//go:build linux

package cli

import "golang.org/x/sys/unix"

func hardenMountCredentialProcess() error {
	if err := scrubMountCredentialEnvironment(); err != nil {
		return err
	}
	return unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0)
}
