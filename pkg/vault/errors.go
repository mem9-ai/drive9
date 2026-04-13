package vault

import "errors"

var (
	ErrNotFound      = errors.New("vault: not found")
	ErrFieldNotFound = errors.New("vault: field not found")
	ErrOutOfScope    = errors.New("vault: secret not in token scope")
	ErrTokenExpired  = errors.New("vault: token expired")
	ErrTokenRevoked  = errors.New("vault: token revoked")
)
