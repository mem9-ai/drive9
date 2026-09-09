package datastore

import (
	"errors"
	"strings"
)

// ContentLayout describes how a file's bytes are organized. This is orthogonal
// to StorageType (where the bytes live: db9 vs s3).
type ContentLayout string

const (
	ContentLayoutSingle    ContentLayout = "single"
	ContentLayoutAppendLog ContentLayout = "append_log"
	ContentLayoutExtent    ContentLayout = "extent"
)

const (
	// ExtentChunkBits is JuiceFS-compatible 64MiB chunk partitioning.
	ExtentChunkBits = 26
	// ExtentChunkSize is 64MiB.
	ExtentChunkSize = 1 << ExtentChunkBits
	// ExtentMaxBlockSize is the maximum immutable block object size (4MiB).
	ExtentMaxBlockSize = 4 << 20
	// ExtentStorageRefPrefix marks contents.storage_ref for extent files so
	// file_gc_tasks never treat the placeholder as a blobs/ key.
	ExtentStorageRefPrefix = "extent:"
)

var (
	ErrInvalidContentLayout = errors.New("invalid content layout")
	ErrNotExtent            = errors.New("file is not content_layout=extent")
	ErrExtentCoverageHole   = errors.New("coverage_hole")
	ErrBlockNotLanded       = errors.New("block_not_landed")
	ErrPendingExpired       = errors.New("pending_expired")
	ErrGenerationConflict   = errors.New("generation conflict")
	ErrExtentUseCommit      = errors.New("extent files require commit-slices")
	ErrCompactAborted       = errors.New("compact aborted: chunk changed")
	ErrAppendLogUnsupported = errors.New("append_log content layout is not implemented")
)

// NormalizeContentLayout returns single for empty values.
func NormalizeContentLayout(layout ContentLayout) ContentLayout {
	if layout == "" {
		return ContentLayoutSingle
	}
	return layout
}

// ParseContentLayout validates a wire layout value.
func ParseContentLayout(raw string) (ContentLayout, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ContentLayoutSingle, nil
	}
	switch ContentLayout(raw) {
	case ContentLayoutSingle, ContentLayoutExtent, ContentLayoutAppendLog:
		return ContentLayout(raw), nil
	default:
		return "", ErrInvalidContentLayout
	}
}

// ExtentStorageRef returns the placeholder storage_ref for an extent inode.
func ExtentStorageRef(inodeID string) string {
	return ExtentStorageRefPrefix + inodeID
}

// IsExtentStorageRef reports whether storage_ref is an extent placeholder.
func IsExtentStorageRef(storageRef string) bool {
	return strings.HasPrefix(storageRef, ExtentStorageRefPrefix)
}

// Layout returns the file's content layout, defaulting empty to single.
func (f *File) Layout() ContentLayout {
	if f == nil {
		return ContentLayoutSingle
	}
	return NormalizeContentLayout(f.ContentLayout)
}

// IsExtent reports whether this file uses content_layout=extent.
func (f *File) IsExtent() bool {
	if f == nil {
		return false
	}
	if f.Layout() == ContentLayoutExtent {
		return true
	}
	return IsExtentStorageRef(f.StorageRef)
}

// ChunkOf returns the 64MiB chunk index for a file offset.
func ChunkOf(fileOff int64) int64 {
	if fileOff < 0 {
		return 0
	}
	return fileOff >> ExtentChunkBits
}

// ResolveContentLayout uses only an explicit create/setattr layout.
// Mount-profile [extent] globs are applied by the FUSE client, which then
// sends X-Dat9-Content-Layout; the server does not infer layout from the path.
func ResolveContentLayout(explicit ContentLayout, filePath string) (ContentLayout, error) {
	if explicit != "" {
		return ParseContentLayout(string(explicit))
	}
	return ContentLayoutSingle, nil
}
