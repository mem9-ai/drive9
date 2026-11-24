// Package viewprotocol defines the cross-repository wire contract for LayerFS
// views: server-generated Layer IDs, control-plane ref selectors, /v1/views
// route encoding, X-FS-Layer-Fence headers, and machine error codes.
package viewprotocol

import (
	"errors"
	"regexp"
)

const (
	RoutePrefix = "/v1/views/"
	FenceHeader = "X-FS-Layer-Fence"

	MaxLayerNameBytes = 255
	MaxLayerRefBytes  = 320
	MaxMountIDBytes   = 64

	CodeInvalidArgument        = "INVALID_ARGUMENT"
	CodeInvalidLayerID         = "INVALID_LAYER_ID"
	CodeInvalidLayerName       = "INVALID_LAYER_NAME"
	CodeInvalidLayerRef        = "INVALID_LAYER_REF"
	CodeENameTooLong           = "ENAMETOOLONG"
	CodeForbidden              = "FORBIDDEN"
	CodeForbiddenOutsideBase   = "FORBIDDEN_OUTSIDE_BASE_ROOT"
	CodeLayerNotFound          = "LAYER_NOT_FOUND"
	CodeENOENT                 = "ENOENT"
	CodeMethodNotAllowed       = "METHOD_NOT_ALLOWED"
	CodeLayerAlreadyExists     = "LAYER_ALREADY_EXISTS"
	CodeLayerRefAmbiguous      = "LAYER_REF_AMBIGUOUS"
	CodeLayerFenceMismatch     = "LAYER_FENCE_MISMATCH"
	CodeLayerMountConflict     = "LAYER_MOUNT_CONFLICT"
	CodeLayerMountedBusy       = "LAYER_MOUNTED_BUSY"
	CodeLayerBusyStreaming     = "LAYER_BUSY_STREAMING"
	CodeLayerHasActiveUploads  = "LAYER_HAS_ACTIVE_UPLOADS"
	CodeLayerViewChanged       = "LAYER_VIEW_CHANGED"
	CodeExtentCompactRequired  = "EXTENT_COMPACT_REQUIRED"
	CodeMergeConflict          = "MERGE_CONFLICT"
	CodeConflict               = "CONFLICT"
	CodeEOverflow              = "EOVERFLOW"
	CodeMergeTooLarge          = "MERGE_TOO_LARGE"
	CodeTenantReadOnly         = "A_READ_ONLY"
	CodeUnsupportedInLayerView  = "UNSUPPORTED_IN_LAYER_VIEW"
	CodeLayerRequiresTiDBServer = "LAYER_REQUIRES_TIDB_SERVER"
	CodeLayerCreateDisabled     = "LAYER_CREATE_DISABLED"
	CodeControlPlaneCorrupt    = "ERR_CONTROL_PLANE_CORRUPT"
	CodeCorruptUndoInode       = "ERR_CORRUPT_UNDO_INODE"
	CodeCorruptUndoExtent      = "ERR_CORRUPT_UNDO_EXTENT"

	// Local FUSE mount switch machine error codes (v36 §6.3 #2–#4).
	CodeSwitchUnsupported          = "UNSUPPORTED"
	CodeSwitchBusy                 = "EBUSY"
	CodeSwitchPrepareFailed        = "PREPARE_FAILED"
	CodeSwitchBindCleanupPending   = "BIND_CLEANUP_PENDING"
	CodeSwitchUnbindCleanupPending = "UNBIND_CLEANUP_PENDING"
	CodeSwitchTargetRevoked        = "TARGET_REVOKED"
	CodeSwitchRecovering           = "RECOVERING"
)

var (
	ErrInvalidLayerID         = errors.New("invalid layer id")
	ErrInvalidLayerName       = errors.New("invalid layer name")
	ErrInvalidLayerRef        = errors.New("invalid layer ref")
	ErrInvalidFence           = errors.New("invalid layer fence")
	ErrInvalidOperationID     = errors.New("invalid mount or operation id")
	ErrInvalidViewRoute       = errors.New("invalid layer view route")
	ErrInvalidViewRedirect    = errors.New("redirect not allowed for layer view request")
	ErrUnsupportedInLayerView = errors.New("operation unsupported in layer view")

	canonicalLayerIDRE = regexp.MustCompile(`^layer_[0-9a-hjkmnp-tv-z]{26}$`)
)
