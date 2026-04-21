package cli

import (
	"flag"
	"fmt"
	"os"
	"time"

	drive9fuse "github.com/mem9-ai/dat9/pkg/fuse"
)

// VaultMountCmd handles `drive9 mount vault [flags] <mountpoint>`.
//
// V2e Row A: dispatch surface. Routed to from MountCmd when args[0] == "vault".
//
// V2e Row B: credential resolver. Reuses the unified ResolveCredentials +
// resolveMountCredentials pipeline shared with the writable fs mount, so
// the priority chain (--api-key flag > DRIVE9_VAULT_TOKEN > DRIVE9_API_KEY
// > active context) and the explicit-empty-flag rejection both apply
// uniformly. The vault flavour does NOT introduce a new resolver path —
// that would create a second contract surface for the credential-confusion
// bug class called out in spec §14.2.
//
// V2e Row C-I: enforced by drive9fuse.MountVault — see vaultfs.go and
// vault_mount.go for the per-row anchors. This file owns only the CLI
// surface (flag parsing + cred resolution + handing off to MountVault).
func VaultMountCmd(args []string) error {
	fs := flag.NewFlagSet("mount vault", flag.ExitOnError)
	server := fs.String("server", "", "drive9 server URL (overrides $DRIVE9_SERVER and config)")
	apiKey := fs.String("api-key", "", "owner API key (overrides $DRIVE9_API_KEY and config)")
	dirTTL := fs.Duration("dir-ttl", 5*time.Second, "vault list / field cache TTL")
	allowOther := fs.Bool("allow-other", false, "allow other users to access mount")
	debug := fs.Bool("debug", false, "enable FUSE debug logging")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: drive9 mount vault [flags] <mountpoint>\n\nflags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	mountPoint := fs.Arg(0)

	serverGiven, apiKeyGiven := flagProvided(fs, "server"), flagProvided(fs, "api-key")
	if err := rejectEmptyFlag("server", *server, serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("api-key", *apiKey, apiKeyGiven); err != nil {
		return err
	}

	serverVal, apiKeyVal, tokenVal, err := resolveMountCredentials(ResolveCredentials(), *server, *apiKey)
	if err != nil {
		return err
	}

	opts := &drive9fuse.VaultMountOptions{
		Server:     serverVal,
		APIKey:     apiKeyVal,
		Token:      tokenVal,
		MountPoint: mountPoint,
		DirTTL:     *dirTTL,
		AllowOther: *allowOther,
		Debug:      *debug,
	}
	return drive9fuse.MountVault(opts)
}
