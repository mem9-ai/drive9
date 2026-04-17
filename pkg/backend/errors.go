package backend

import "errors"

// ErrNotS3Stored reports that an operation requires an S3-backed file target.
var ErrNotS3Stored = errors.New("file is not S3-stored")

// ErrS3NotConfigured reports that the backend cannot serve S3-backed flows.
var ErrS3NotConfigured = errors.New("S3 not configured")
