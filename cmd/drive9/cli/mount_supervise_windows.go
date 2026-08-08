//go:build windows

package cli

import "fmt"

func runMountSupervise(args []string) error {
	return fmt.Errorf("drive9 mount supervise is not supported on windows")
}

func runMountStatus(args []string) error {
	return fmt.Errorf("drive9 mount status is not supported on windows")
}

func runMountHealth(args []string) error {
	return fmt.Errorf("drive9 mount health is not supported on windows")
}

func runMountEnsure(args []string) error {
	return fmt.Errorf("drive9 mount ensure is not supported on windows")
}

func runMountSystemdUnit(args []string) error {
	return fmt.Errorf("drive9 mount systemd-unit is not supported on windows")
}

type cliExitError struct {
	code int
	msg  string
}

func (e cliExitError) Error() string  { return e.msg }
func (e cliExitError) ExitCode() int { return e.code }
