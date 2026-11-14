//go:build windows

package cli

// The Windows mount bridge does not advertise fixed-view switching.
func pinMainMount(string) (func(), error) { return func() {}, nil }
