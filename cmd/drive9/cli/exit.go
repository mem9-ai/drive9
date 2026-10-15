package cli

import "os"

// exitHook is the process exit path used by command handlers. main installs a
// hook so that process-level observers (CLI telemetry) can act on the real exit
// code; the default keeps the package usable on its own, e.g. in tests.
var exitHook = os.Exit

// SetExitHook installs fn as the exit path for command handlers and returns a
// function that restores the previous hook.
func SetExitHook(fn func(code int)) func() {
	previous := exitHook
	exitHook = fn
	return func() { exitHook = previous }
}

// exitProcess terminates the process through the installed hook. Handlers must
// use it instead of os.Exit so that no exit path bypasses process observers.
func exitProcess(code int) {
	exitHook(code)
}

// UsageError reports a command-line usage failure that the flag package already
// reported to stderr. Reported() is true so the top-level handler exits with
// the requested code without printing the message a second time.
type UsageError struct {
	Err  error
	Code int
}

func (e UsageError) Error() string {
	if e.Err == nil {
		return "invalid usage"
	}
	return e.Err.Error()
}

func (e UsageError) Unwrap() error { return e.Err }

func (e UsageError) Reported() bool { return true }

func (e UsageError) ExitCode() int {
	if e.Code <= 0 {
		return 2
	}
	return e.Code
}
