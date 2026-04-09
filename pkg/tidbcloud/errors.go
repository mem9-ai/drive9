package tidbcloud

import "errors"

// IsAuthError reports whether err is an authentication or authorization failure
// from the account service, suitable for mapping to HTTP 401/403.
func IsAuthError(err error) (status int, ok bool) {
	if err == nil {
		return 0, false
	}
	if errors.Is(err, ErrAuthMissing) {
		return 401, true
	}
	if errors.Is(err, ErrAuthForbidden) {
		return 403, true
	}
	return 0, false
}

// IsNotFound reports whether err indicates a cluster/instance was not found.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrClusterNotFound)
}
