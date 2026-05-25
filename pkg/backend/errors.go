package backend

import "errors"

// ErrNotS3Stored reports that an operation requires an S3-backed file target.
var ErrNotS3Stored = errors.New("file is not S3-stored")

// ErrS3NotConfigured reports that the backend cannot serve S3-backed flows.
var ErrS3NotConfigured = errors.New("S3 not configured")

// ErrNotInlineStorage reports that an operation requires db9-inline file data.
var ErrNotInlineStorage = errors.New("file is not stored inline")

// ErrInvalidSymlinkTarget reports that a symbolic link target is empty or
// contains bytes that cannot be represented by the FUSE symlink contract.
var ErrInvalidSymlinkTarget = errors.New("invalid symlink target")
