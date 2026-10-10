//go:build !linux && !windows

package cli

func hardenMountCredentialProcess() error { return scrubMountCredentialEnvironment() }
