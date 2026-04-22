package fuse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

// VaultMountOptions configures a `drive9 mount vault <path>` mount.
//
// Like MountOptions, APIKey and Token are mutually exclusive — exactly one
// must be set, and the chosen credential is locked for the mount's lifetime
// (Invariant #3, #6). A read-only vault mount served by a delegated token
// can only see secrets the token's grant covers (Row D). Probe-time mount
// rejection is driven by the server's auth response, not by the size of the
// readable secret set: valid credentials may legitimately enumerate zero
// secrets, while malformed / expired / revoked tokens are rejected by the
// server with 401 (Row I).
type VaultMountOptions struct {
	Server     string
	APIKey     string        // owner API key (mutually exclusive with Token)
	Token      string        // delegated capability JWT (mutually exclusive with APIKey)
	MountPoint string        // local mount point
	DirTTL     time.Duration // cache TTL for the secret list and per-secret field maps
	AllowOther bool
	Debug      bool
}

func (o *VaultMountOptions) setDefaults() {
	if o.DirTTL <= 0 {
		o.DirTTL = 5 * time.Second
	}
}

// MountVault creates and serves a read-only FUSE mount that exposes vault
// secrets readable by the bound credential. It blocks until the filesystem
// is unmounted or SIGINT/SIGTERM is received.
//
// The contract row mapping is documented at the top of vaultfs.go.
func MountVault(opts *VaultMountOptions) error {
	opts.setDefaults()

	if err := os.MkdirAll(opts.MountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	if opts.APIKey != "" && opts.Token != "" {
		return fmt.Errorf("mount vault: APIKey and Token are mutually exclusive (choose one principal kind at mount time)")
	}
	if opts.APIKey == "" && opts.Token == "" {
		return fmt.Errorf("mount vault: either APIKey (owner) or Token (delegated) is required")
	}

	var c *client.Client
	if opts.Token != "" {
		c = client.NewWithToken(opts.Server, opts.Token)
	} else {
		c = client.New(opts.Server, opts.APIKey)
	}

	// Probe: verify the credential can reach the server and list secrets.
	// This surfaces connectivity / auth failures early (before the FUSE
	// mount is established) rather than as silent empty mounts.
	//
	// We intentionally do NOT reject an empty secret list here for either
	// principal kind:
	//   - Owner with zero secrets = normal new-tenant startup (quickstart
	//     flow: mount first, then create the first secret).
	//   - Delegated token with zero existing secrets = valid grant whose
	//     scope targets secrets that don't exist yet or were deleted. This
	//     is NOT the same as a revoked/malformed token (which the server
	//     surfaces as 401, caught by the error check above).
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	secrets, err := c.ListReadableVaultSecrets(probeCtx)
	probeCancel()
	if err != nil {
		return fmt.Errorf("vault probe: %w", err)
	}

	vfs := NewVaultFS(c, opts.DirTTL)

	fuseOpts := &gofuse.MountOptions{
		FsName:     "drive9-vault",
		Name:       "drive9-vault",
		Debug:      opts.Debug,
		AllowOther: opts.AllowOther,
		// Always read-only at the kernel level too (belt-and-braces with
		// the in-process EROFS rejections).
		Options: []string{"ro"},
	}

	server, err := gofuse.NewServer(vfs, opts.MountPoint, fuseOpts)
	if err != nil {
		return fmt.Errorf("fuse mount vault: %w", err)
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		return fmt.Errorf("fuse wait mount (vault): %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\ndrive9: unmounting vault %s...\n", opts.MountPoint)
		go func() {
			<-sigCh
			fmt.Fprintf(os.Stderr, "drive9: forced exit\n")
			os.Exit(1)
		}()
		const maxRetries = 5
		var unmountErr error
		for i := 0; i < maxRetries; i++ {
			if unmountErr = server.Unmount(); unmountErr == nil {
				return
			}
			fmt.Fprintf(os.Stderr, "drive9: vault unmount attempt %d/%d failed: %v\n", i+1, maxRetries, unmountErr)
			if i < maxRetries-1 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		fmt.Fprintf(os.Stderr, "drive9: vault retries exhausted, force-unmounting %s\n", opts.MountPoint)
		forceUnmountVault(opts.MountPoint)
	}()

	fmt.Fprintf(os.Stderr, "drive9: vault mounted on %s (server: %s, secrets: %d)\n", opts.MountPoint, opts.Server, len(secrets))
	server.Wait()
	return nil
}

func forceUnmountVault(mountpoint string) {
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.Command("diskutil", "unmount", "force", mountpoint)
	} else {
		if _, err := exec.LookPath("fusermount"); err == nil {
			cmd = exec.Command("fusermount", "-u", mountpoint)
		} else {
			cmd = exec.Command("umount", "-l", mountpoint)
		}
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "drive9: vault force unmount failed: %v\n", err)
	}
}
