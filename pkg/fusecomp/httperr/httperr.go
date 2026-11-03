// Package httperr maps drive9 backend HTTP failures onto kernel errnos
// for FUSE-facing callers. It is terminal-side machinery: the handlers
// that talk to the backend through pkg/client convert SDK errors into
// kernel-shaped errors here, at the boundary where those errors first
// appear; the rest of the chain and the facade never see HTTP.
package httperr

import (
	"context"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// statusClientClosedRequest is nginx's "Client Closed Request": the tenant
// auth middleware emits it when the request context cancels before auth
// completes. Treated like context.Canceled so FUSE callers see retryable
// EAGAIN instead of opaque EIO.
const statusClientClosedRequest = 499

// HTTPStatusOf maps backend HTTP failures onto kernel errnos: revision
// conflicts are EIO, "directory not empty" ENOTEMPTY, other 409s EEXIST,
// quota 507s EDQUOT/EFBIG, 5xx/499/cancellation retryable EAGAIN, 412
// ESTALE, 413 EFBIG.
func HTTPStatusOf(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return gofuse.Status(syscall.EAGAIN)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return gofuse.Status(syscall.EAGAIN)
	}
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusNotFound:
			return gofuse.ENOENT
		case http.StatusConflict:
			lower := strings.ToLower(se.Message)
			if strings.Contains(lower, "revision conflict") {
				return gofuse.EIO
			}
			if strings.Contains(lower, "directory not empty") {
				return gofuse.Status(syscall.ENOTEMPTY)
			}
			return gofuse.Status(syscall.EEXIST)
		case http.StatusUnauthorized, http.StatusForbidden:
			return gofuse.EACCES
		case http.StatusRequestEntityTooLarge:
			return gofuse.Status(syscall.EFBIG)
		case http.StatusInsufficientStorage:
			if status, matched := quotaErrToFuseStatus(err); matched {
				return status
			}
			return gofuse.EIO
		case http.StatusPreconditionFailed:
			return gofuse.Status(syscall.ESTALE)
		case http.StatusBadRequest:
			return gofuse.Status(syscall.EINVAL)
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, statusClientClosedRequest:
			return gofuse.Status(syscall.EAGAIN)
		default:
			return gofuse.EIO
		}
	}
	msg := err.Error()
	lowerMsg := strings.ToLower(msg)
	switch {
	case strings.Contains(lowerMsg, "not found") || strings.Contains(msg, "HTTP 404"):
		return gofuse.ENOENT
	case strings.Contains(lowerMsg, "directory not empty"):
		return gofuse.Status(syscall.ENOTEMPTY)
	case strings.Contains(lowerMsg, "already exists") || strings.Contains(msg, "HTTP 409"):
		return gofuse.Status(syscall.EEXIST)
	case strings.Contains(msg, "HTTP 401") || strings.Contains(msg, "HTTP 403"):
		return gofuse.EACCES
	case strings.Contains(msg, "HTTP 413"):
		return gofuse.Status(syscall.EFBIG)
	case strings.Contains(msg, "HTTP 412"):
		return gofuse.Status(syscall.ESTALE)
	case strings.Contains(msg, "HTTP 400"):
		return gofuse.Status(syscall.EINVAL)
	case strings.Contains(msg, "HTTP 499") ||
		strings.Contains(msg, "HTTP 500") ||
		strings.Contains(msg, "HTTP 502") ||
		strings.Contains(msg, "HTTP 503") ||
		strings.Contains(msg, "HTTP 504"):
		return gofuse.Status(syscall.EAGAIN)
	}
	return gofuse.EIO
}

// quotaStatusRules maps the server's structured quota-exceeded messages
// onto EDQUOT/EFBIG.
var quotaStatusRules = []struct {
	cause   string
	errno   syscall.Errno
	details []*regexp.Regexp
}{
	{
		cause: "tenant storage quota exceeded",
		errno: syscall.EDQUOT,
		details: []*regexp.Regexp{
			regexp.MustCompile(`^tenant storage quota exceeded: server limit=-?[0-9]+ used=-?[0-9]+ reserved=-?[0-9]+ pending=-?[0-9]+ delta=-?[0-9]+$`),
			regexp.MustCompile(`^tenant storage quota exceeded: limit=-?[0-9]+ used=-?[0-9]+ reserved=-?[0-9]+ current_path=-?[0-9]+ requested=-?[0-9]+ delta=-?[0-9]+$`),
			regexp.MustCompile(`^tenant storage quota exceeded: limit=-?[0-9]+ used=-?[0-9]+ reserved=-?[0-9]+ requested=-?[0-9]+ delta=-?[0-9]+$`),
		},
	},
	{
		cause: "tenant file count quota exceeded",
		errno: syscall.EDQUOT,
		details: []*regexp.Regexp{
			regexp.MustCompile(`^tenant file count quota exceeded: server limit=-?[0-9]+ used=-?[0-9]+ pending=-?[0-9]+ delta=-?[0-9]+$`),
		},
	},
	{
		cause: "tenant file size quota exceeded",
		errno: syscall.EFBIG,
		details: []*regexp.Regexp{
			regexp.MustCompile(`^tenant file size quota exceeded: server limit=-?[0-9]+ requested=-?[0-9]+$`),
		},
	},
}

func quotaErrToFuseStatus(err error) (gofuse.Status, bool) {
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusInsufficientStorage {
		return 0, false
	}
	for _, rule := range quotaStatusRules {
		if statusErr.Message == rule.cause {
			return gofuse.Status(rule.errno), true
		}
		for _, details := range rule.details {
			if details.MatchString(statusErr.Message) {
				return gofuse.Status(rule.errno), true
			}
		}
	}
	return 0, false
}
