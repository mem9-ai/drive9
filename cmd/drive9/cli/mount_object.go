package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/objectfs"
)

var mountObjectStore = mountObjectStoreImpl

func mountObjectStoreImpl(loc *Location, mountPoint, cacheDir string, readOnly, debug, supervised, allowOther bool) error {
	kind := "read-write"
	if readOnly {
		kind = "read-only"
	}
	fmt.Fprintf(os.Stderr, "drive9: object mount %s (prefix emulation; %s; no chmod/chown; listing is billed)\n", loc.Raw, kind)

	opts := objectfs.Options{
		MountPoint: mountPoint,
		Location:   toObjectLocation(*loc),
		CacheDir:   cacheDir,
		ReadOnly:   readOnly,
		Debug:      debug || os.Getenv("DRIVE9_OBJECTFS_DEBUG") == "1",
		AllowOther: allowOther,
		Supervised: supervised,
	}
	if !objectAuthLocal {
		c := NewFromEnv()
		minted, err := c.MintObjectCredentials(context.Background(), loc.Raw, !readOnly)
		if err != nil {
			return err
		}
		if loc.Query == nil {
			loc.Query = map[string]string{}
		}
		applyMintedQuery(loc.Query, minted)
		opts.Location = toObjectLocation(*loc)
		opts.Session = sessionFromMinted(minted)
		opts.SessionExpiry = objectfs.ParseSessionExpiry(minted.Expiration)
		opts.Mint = func(ctx context.Context) (objectfs.SessionCredentials, time.Time, error) {
			next, mintErr := c.MintObjectCredentials(ctx, loc.Raw, !readOnly)
			if mintErr != nil {
				return objectfs.SessionCredentials{}, time.Time{}, mintErr
			}
			applyMintedQuery(loc.Query, next)
			return sessionFromMinted(next), objectfs.ParseSessionExpiry(next.Expiration), nil
		}
	}
	return objectfs.Mount(opts)
}

func sessionFromMinted(m *client.ObjectCredentials) objectfs.SessionCredentials {
	if m == nil {
		return objectfs.SessionCredentials{}
	}
	return objectfs.SessionCredentials{
		AccessKeyID:     m.AccessKeyID,
		SecretAccessKey: m.SecretAccessKey,
		SessionToken:    m.SessionToken,
		SASURL:          m.SASURL,
		AccessToken:     m.AccessToken,
	}
}

func validateObjectMount(goos, mode, layerRef, checkpointRef, profile string) error {
	if goos == "windows" {
		return fmt.Errorf("drive9 mount: object-store sources are not supported on Windows")
	}
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = string(MountModeAuto)
	}
	parsed, err := ParseMountMode(mode)
	if err != nil {
		return fmt.Errorf("drive9 mount: %w", err)
	}
	if parsed == MountModeWebDAV {
		return fmt.Errorf("drive9 mount: object-store sources do not support --mode=webdav")
	}
	if goos == "darwin" && parsed == MountModeAuto {
		return fmt.Errorf("drive9 mount: object-store sources on darwin require --mode=fuse (macFUSE or FUSE-T)")
	}
	if strings.TrimSpace(layerRef) != "" || strings.TrimSpace(checkpointRef) != "" {
		return fmt.Errorf("drive9 mount: --layer/--checkpoint are drive9-only")
	}
	if profile != "" && profile != "none" {
		return fmt.Errorf("drive9 mount: object-store sources require --profile=none")
	}
	return nil
}
