package staging

import "time"

const (
	defaultRegularFileMode     uint32 = 0o644
	posixPermissionModeMask    uint32 = 0o7777
	remotePermissionModeMask   uint32 = 0o777
	postUploadModeAttempts            = 5
	postUploadModeInitialDelay        = 10 * time.Millisecond
)
