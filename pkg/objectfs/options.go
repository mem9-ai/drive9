package objectfs

import "time"

// Options configure an object-store mount.
type Options struct {
	MountPoint    string
	Location      Location
	CacheDir      string
	ReadOnly      bool
	Debug         bool
	AllowOther    bool
	Supervised    bool
	Session       SessionCredentials
	SessionExpiry time.Time
	Mint          MintSession
}
