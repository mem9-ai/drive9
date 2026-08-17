package mountsupervisor

import "fmt"

// SupervisorExitError carries the supervisor process exit code.
type SupervisorExitError struct {
	Code   int
	Detail string
}

func (e *SupervisorExitError) Error() string {
	if e == nil {
		return "supervisor exit"
	}
	if e.Detail != "" {
		return fmt.Sprintf("supervisor exit %d: %s", e.Code, e.Detail)
	}
	return fmt.Sprintf("supervisor exit %d", e.Code)
}

func (e *SupervisorExitError) ExitCode() int {
	if e == nil || e.Code <= 0 {
		return 1
	}
	return e.Code
}

func giveUpError(detail string) error {
	return &SupervisorExitError{Code: 3, Detail: detail}
}
