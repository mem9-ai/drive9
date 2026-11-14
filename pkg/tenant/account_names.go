package tenant

import "strings"

// The TiDB Cloud Native instance account scheme: every branch provisions accounts as
// <prefix>.root for the cluster root principal and <prefix>.tdc_fs_sys for the
// restricted per-tenant account. The suffixes live here rather than in one of the
// provisioning packages because building a name and parsing one back have to agree, and
// the callers now span the native provisioner, the server's legacy-grant cleanup and the
// legacy starter compatibility path. A drift compiles, silently stops matching, and
// leaves a stored DSN naming an account nothing ever provisions.
const (
	// RootUserNameSuffix marks the cluster root principal, which shares the password of
	// the restricted account with the same prefix.
	RootUserNameSuffix = ".root"
	// RestrictedSystemUserNameSuffix marks the per-tenant account the native setup
	// creates.
	RestrictedSystemUserNameSuffix = ".tdc_fs_sys"
)

// RootUsernameFor builds the cluster root principal for an instance prefix.
func RootUsernameFor(prefix string) string {
	return prefix + RootUserNameSuffix
}

// IsRestrictedSystemUsername reports whether username names the restricted per-tenant
// account rather than a cluster principal.
func IsRestrictedSystemUsername(username string) bool {
	prefix, ok := strings.CutSuffix(strings.TrimSpace(username), RestrictedSystemUserNameSuffix)
	return ok && prefix != ""
}
