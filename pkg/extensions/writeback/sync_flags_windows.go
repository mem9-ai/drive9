//go:build windows

package writeback

// hasSyncOpenFlag reports O_SYNC/O_DSYNC opens; Windows has none.
func hasSyncOpenFlag(flags uint32) bool { return false }
