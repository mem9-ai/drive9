//go:build !linux

package cli

func syncMountFileSystem(string) error { return nil }
