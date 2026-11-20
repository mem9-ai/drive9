package viewprotocol

import (
	"errors"
	"net/http"
)

// OperationHeader identifies one durable filesystem mutation, independently
// of its work generation. Callers retain it after an unknown response.
const OperationHeader = "Idempotency-Key"

var ErrOperationID = errors.New("invalid filesystem operation identity")

func ValidateOperationID(id string) error {
	if !validID(id) {
		return ErrOperationID
	}
	return nil
}

func ParseOperationID(h http.Header) (string, error) {
	values := h.Values(OperationHeader)
	if len(values) != 1 || ValidateOperationID(values[0]) != nil {
		return "", ErrOperationID
	}
	return values[0], nil
}
