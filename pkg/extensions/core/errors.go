package core

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/httperr"
)

// statusClientClosedRequest: nginx "Client Closed Request", emitted by the
// tenant auth middleware when the request context cancels early —
// transient like cancellation.
const statusClientClosedRequest = 499

// mapErr folds backend HTTP failures into fs error identities. 404 becomes
// fs.ErrNotFound (→ ENOENT, and IsNotFound for the chain); every other
// error keeps the frozen errno table exactly via httperr.HTTPStatusOf —
// cancellation and net timeouts are retryable EAGAIN, revision-conflict
// 409 is EIO, "directory not empty" 409 is ENOTEMPTY, other 409 EEXIST,
// quota 507 EDQUOT/EFBIG, 5xx/499 EAGAIN, 412 ESTALE, 413 EFBIG, and the
// string fallback applies to unwrapped errors.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var se *client.StatusError
	if errors.As(err, &se) && se.StatusCode == http.StatusNotFound {
		return fs.ErrNotFound
	}
	return &fs.StatusError{Status: httperr.HTTPStatusOf(err)}
}

// isTransientStatErr classifies stat-path errors for detached retry:
// cancellation, deadline, 5xx/499 statuses, and net
// timeouts are transient; everything else surfaces immediately.
func isTransientStatErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, statusClientClosedRequest:
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// isTransientReadErr classifies read-path errors for detached retry — same
// classification as the stat classifier, kept separate so the retry
// policies can diverge independently.
func isTransientReadErr(err error) bool { return isTransientStatErr(err) }

// isTransientMutationErr classifies namespace-mutation errors for the
// recovery wrappers.
func isTransientMutationErr(err error) bool { return isTransientStatErr(err) }

// isConflictErr reports a revision-conflict (409) reply.
func isConflictErr(err error) bool {
	if err == nil {
		return false
	}
	var se *client.StatusError
	return errors.As(err, &se) && se.StatusCode == http.StatusConflict
}
