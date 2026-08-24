package objectfs

// Options configure an object-store mount.
type Options struct {
	MountPoint string
	Location   Location
	CacheDir   string
	ReadOnly   bool
	Debug      bool
	AllowOther bool
	Supervised bool
	Session    SessionCredentials
}
