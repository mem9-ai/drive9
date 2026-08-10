package fuse

import (
	"fmt"
	"strings"
)

// Mount exit codes (worker process). See docs/design/fuse-mount-supervision.md.
const (
	ExitOK               = 0
	ExitForceQuit        = 1
	ExitUsage            = 2
	ExitServeAbnormal    = 3
	ExitUnhealthy        = 4
	ExitStartupPermanent = 5
	ExitStartupTransient = 6
)

// MountExitReason classifies why a mount process ended.
type MountExitReason string

const (
	ExitReasonSignal           MountExitReason = "signal"
	ExitReasonExternalUnmount  MountExitReason = "external_unmount"
	ExitReasonServeAbnormal    MountExitReason = "serve_abnormal"
	ExitReasonPanic            MountExitReason = "panic"
	ExitReasonUnhealthy        MountExitReason = "unhealthy"
	ExitReasonStartupPermanent MountExitReason = "startup_permanent"
	ExitReasonStartupTransient MountExitReason = "startup_transient"
)

// MountExitError is a typed mount failure that carries a process exit code
// for cmd/drive9/main.go's exitCoder path.
type MountExitError struct {
	Reason MountExitReason
	Detail string
	Code   int
	Cause  error
}

func (e *MountExitError) Error() string {
	if e == nil {
		return "mount exit"
	}
	parts := []string{fmt.Sprintf("mount exit code=%d reason=%s", e.Code, e.Reason)}
	if detail := strings.TrimSpace(e.Detail); detail != "" {
		parts = append(parts, detail)
	}
	if e.Cause != nil {
		parts = append(parts, e.Cause.Error())
	}
	return strings.Join(parts, ": ")
}

func (e *MountExitError) ExitCode() int {
	if e == nil || e.Code <= 0 {
		return 1
	}
	return e.Code
}

func (e *MountExitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newMountExit(code int, reason MountExitReason, detail string, cause error) *MountExitError {
	return &MountExitError{
		Reason: reason,
		Detail: detail,
		Code:   code,
		Cause:  cause,
	}
}

func ExitServeAbnormalErr(detail string) *MountExitError {
	return newMountExit(ExitServeAbnormal, ExitReasonServeAbnormal, detail, nil)
}

func ExitUnhealthyErr(detail string) *MountExitError {
	return newMountExit(ExitUnhealthy, ExitReasonUnhealthy, detail, nil)
}

func ExitPanicErr(detail string, cause error) *MountExitError {
	return newMountExit(ExitUnhealthy, ExitReasonPanic, detail, cause)
}

func ExitStartupPermanentErr(detail string, cause error) *MountExitError {
	return newMountExit(ExitStartupPermanent, ExitReasonStartupPermanent, detail, cause)
}

func ExitStartupTransientErr(detail string, cause error) *MountExitError {
	return newMountExit(ExitStartupTransient, ExitReasonStartupTransient, detail, cause)
}
