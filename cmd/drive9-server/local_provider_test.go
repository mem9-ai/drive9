package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

// keyFilePath returns a key path inside a private (0700) directory. t.TempDir()
// is 0775 under a umask of 002 and the loader refuses group-writable ancestors,
// so the chain up to the sticky temp root is tightened to 0700.
func keyFilePath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "keys")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	tmpRoot := filepath.Clean(os.TempDir())
	for current := dir; ; current = filepath.Dir(current) {
		if current == tmpRoot || filepath.Dir(current) == current {
			break
		}
		if err := os.Chmod(current, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "keys.json")
}

func TestApplyLocalProviderEnvDefaults(t *testing.T) {
	keys := []string{
		"DRIVE9_TENANT_PROVIDER",
		"DRIVE9_META_DSN",
		"DRIVE9_LOCAL_DSN",
		"DRIVE9_MASTER_KEY",
		"DRIVE9_TOKEN_SIGNING_KEY",
		"DRIVE9_LEADER_DISABLED",
		"DRIVE9_S3_DIR",
		"DRIVE9_S3_BUCKET",
		"DRIVE9_ENCRYPT_TYPE",
		"DRIVE9_DISABLE_AUTO_EMBEDDING",
		"DRIVE9_PUBLIC_URL",
		"DRIVE9_LISTEN_ADDR",
		"DRIVE9_LOCAL_MYSQL_DSN",
	}
	keys = append(keys, EnvLocalDevKeyFile, envLocalDevKeysInUse)
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	keyFile := keyFilePath(t)
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	if err := applyLocalProviderEnvDefaults(""); err != nil {
		t.Fatalf("applyLocalProviderEnvDefaults: %v", err)
	}

	if os.Getenv("DRIVE9_LISTEN_ADDR") != "127.0.0.1:9009" {
		t.Fatalf("LISTEN_ADDR=%q", os.Getenv("DRIVE9_LISTEN_ADDR"))
	}
	if os.Getenv("DRIVE9_META_DSN") != defaultLocalAdminDSN {
		t.Fatalf("META_DSN=%q", os.Getenv("DRIVE9_META_DSN"))
	}
	master, token := os.Getenv("DRIVE9_MASTER_KEY"), os.Getenv("DRIVE9_TOKEN_SIGNING_KEY")
	if err := validateKeyHex("DRIVE9_MASTER_KEY", master); err != nil {
		t.Fatalf("MASTER_KEY: %v", err)
	}
	if err := validateKeyHex("DRIVE9_TOKEN_SIGNING_KEY", token); err != nil {
		t.Fatalf("TOKEN_SIGNING_KEY: %v", err)
	}
	if master == token {
		t.Fatal("generated master and token signing keys must differ")
	}
	if os.Getenv(envLocalDevKeysInUse) != "1" {
		t.Fatal("generated keys were not recorded as dev keys")
	}
	if runtime.GOOS != "windows" {
		// Windows reports every writable file as 0666; the mode check does not
		// apply there (see checkLocalDevKeyPermissions).
		info, err := os.Stat(keyFile)
		if err != nil {
			t.Fatalf("stat key file: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("key file mode = %04o, want 0600", perm)
		}
	}
	if os.Getenv("DRIVE9_LEADER_DISABLED") != "1" {
		t.Fatalf("LEADER_DISABLED=%q", os.Getenv("DRIVE9_LEADER_DISABLED"))
	}
	if os.Getenv("DRIVE9_TENANT_PROVIDER") != "" && os.Getenv("DRIVE9_TENANT_PROVIDER") != tenant.ProviderLocal {
		t.Fatalf("unexpected TENANT_PROVIDER=%q", os.Getenv("DRIVE9_TENANT_PROVIDER"))
	}
	if os.Getenv("DRIVE9_PUBLIC_URL") != "http://127.0.0.1:9009" {
		t.Fatalf("PUBLIC_URL=%q", os.Getenv("DRIVE9_PUBLIC_URL"))
	}
}

func TestApplyLocalProviderEnvDefaultsPublicURLFollowsListen(t *testing.T) {
	keys := []string{"DRIVE9_LISTEN_ADDR", "DRIVE9_PUBLIC_URL", "DRIVE9_MASTER_KEY", "DRIVE9_TOKEN_SIGNING_KEY", "DRIVE9_ENCRYPT_TYPE", "DRIVE9_LEADER_DISABLED", "DRIVE9_DISABLE_AUTO_EMBEDDING", "DRIVE9_S3_DIR", "DRIVE9_S3_BUCKET", "DRIVE9_LOCAL_DSN", "DRIVE9_LOCAL_MYSQL_DSN", "DRIVE9_META_DSN", EnvLocalDevKeyFile, envLocalDevKeysInUse}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })
	unsetEnv(t, keys)
	t.Setenv(EnvLocalDevKeyFile, keyFilePath(t))
	if err := applyLocalProviderEnvDefaults("127.0.0.1:19009"); err != nil {
		t.Fatalf("applyLocalProviderEnvDefaults: %v", err)
	}
	if os.Getenv("DRIVE9_LISTEN_ADDR") != "127.0.0.1:19009" {
		t.Fatalf("LISTEN_ADDR=%q", os.Getenv("DRIVE9_LISTEN_ADDR"))
	}
	if os.Getenv("DRIVE9_PUBLIC_URL") != "http://127.0.0.1:19009" {
		t.Fatalf("PUBLIC_URL=%q, want http://127.0.0.1:19009", os.Getenv("DRIVE9_PUBLIC_URL"))
	}
}

func TestResolveServerListenAddr(t *testing.T) {
	if got := resolveServerListenAddr("0.0.0.0:9009", "127.0.0.1:9009", true); got != "0.0.0.0:9009" {
		t.Fatalf("cli override = %q", got)
	}
	if got := resolveServerListenAddr("", "127.0.0.1:19009", true); got != "127.0.0.1:19009" {
		t.Fatalf("env = %q", got)
	}
	if got := resolveServerListenAddr("", "", true); got != "127.0.0.1:9009" {
		t.Fatalf("local default = %q", got)
	}
	if got := resolveServerListenAddr("", "", false); got != defaultListenAddr {
		t.Fatalf("non-local default = %q", got)
	}
}

func TestRejectLocalProviderNonLoopbackDefaults(t *testing.T) {
	keys := []string{"DRIVE9_LISTEN_ADDR", "DRIVE9_TOKEN_SIGNING_KEY", "DRIVE9_MASTER_KEY"}
	restore := snapshotEnv(t, keys)
	t.Cleanup(func() { restoreEnv(t, restore) })

	t.Setenv(envLocalDevKeysInUse, "1")
	if err := rejectLocalProviderNonLoopbackDefaults("127.0.0.1:9009"); err != nil {
		t.Fatalf("loopback: %v", err)
	}

	if err := rejectLocalProviderNonLoopbackDefaults("0.0.0.0:9009"); err == nil {
		t.Fatal("expected reject for 0.0.0.0 with default keys")
	}
	if err := rejectLocalProviderNonLoopbackDefaults(":9009"); err == nil {
		t.Fatal("expected reject for wildcard :9009 with default keys")
	}

	t.Setenv("DRIVE9_LISTEN_ADDR", "127.0.0.1:9009")
	t.Setenv(EnvLocalDevKeyFile, keyFilePath(t))
	if err := applyLocalProviderEnvDefaults("127.0.0.1:9009"); err != nil {
		t.Fatalf("applyLocalProviderEnvDefaults: %v", err)
	}
	if err := rejectLocalProviderNonLoopbackDefaults(resolveServerListenAddr("0.0.0.0:9009", os.Getenv("DRIVE9_LISTEN_ADDR"), true)); err == nil {
		t.Fatal("expected reject when positional listen overrides loopback env")
	}

	// Rotating to operator-supplied keys clears the dev-key marker, which is what
	// the guard keys off now.
	if err := os.Unsetenv(envLocalDevKeysInUse); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DRIVE9_TOKEN_SIGNING_KEY", strings.Repeat("ab", 32))
	t.Setenv("DRIVE9_MASTER_KEY", strings.Repeat("cd", 32))
	if err := rejectLocalProviderNonLoopbackDefaults("0.0.0.0:9009"); err != nil {
		t.Fatalf("overridden keys: %v", err)
	}
}

// The dev key file is the only source of local-provider keys: it must be
// generated once, reused across restarts, and never emitted again in place.
func TestLocalDevKeysAreGeneratedOnceAndReused(t *testing.T) {
	keyFile := filepath.Join(filepath.Dir(keyFilePath(t)), "nested", "keys.json")
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	first, path, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("loadOrCreateLocalDevKeys: %v", err)
	}
	if path != keyFile {
		t.Fatalf("path = %q, want %q", path, keyFile)
	}
	if err := first.validate(); err != nil {
		t.Fatalf("generated keys invalid: %v", err)
	}
	second, _, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if second != first {
		t.Fatal("a restart regenerated the dev keys, which would invalidate tokens and strand vault data")
	}

	// Deleting the file is the rotation path.
	if err := os.Remove(keyFile); err != nil {
		t.Fatal(err)
	}
	rotated, _, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.MasterKeyHex == first.MasterKeyHex || rotated.TokenSigningKeyHex == first.TokenSigningKeyHex {
		t.Fatal("deleting the key file did not rotate the keys")
	}
}

// An unreadable key file must fail closed: silently regenerating would leave
// existing encrypted tenant data behind a key nobody holds.
func TestLocalDevKeysRejectMalformedFile(t *testing.T) {
	keyFile := keyFilePath(t)
	t.Setenv(EnvLocalDevKeyFile, keyFile)
	for _, body := range []string{
		"not json",
		`{"master_key_hex":"zz","token_signing_key_hex":"zz"}`,
		`{"master_key_hex":"","token_signing_key_hex":""}`,
		`{"master_key_hex":"` + strings.Repeat("ab", 32) + `","token_signing_key_hex":"` + strings.Repeat("ab", 32) + `"}`,
	} {
		if err := os.WriteFile(keyFile, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadOrCreateLocalDevKeys(); err == nil {
			t.Fatalf("accepted a malformed key file: %s", body)
		}
	}
}

// Operator-supplied keys win and are not overwritten by the generated pair.
func TestApplyLocalProviderDevKeysKeepsEnvironmentKeys(t *testing.T) {
	keyFile := keyFilePath(t)
	t.Setenv(EnvLocalDevKeyFile, keyFile)
	t.Setenv("DRIVE9_MASTER_KEY", strings.Repeat("11", 32))
	t.Setenv("DRIVE9_TOKEN_SIGNING_KEY", strings.Repeat("22", 32))
	t.Setenv(envLocalDevKeysInUse, "")

	if err := applyLocalProviderDevKeys(); err != nil {
		t.Fatalf("applyLocalProviderDevKeys: %v", err)
	}
	if got := os.Getenv("DRIVE9_MASTER_KEY"); got != strings.Repeat("11", 32) {
		t.Fatalf("master key was overwritten: %q", got)
	}
	if _, err := os.Stat(keyFile); err == nil {
		t.Fatal("a key file was written even though the environment supplied both keys")
	}
	if os.Getenv(envLocalDevKeysInUse) == "1" {
		t.Fatal("environment keys were marked as generated dev keys")
	}
}

// A key file that other users can read is not trusted: on the shared temp
// fallback another user could pre-create it with keys of their choosing.
func TestLocalDevKeysRejectBroadPermissionsAndSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are not meaningful on Windows")
	}
	keyFile := keyFilePath(t)
	dir := filepath.Dir(keyFile)
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	if _, _, err := loadOrCreateLocalDevKeys(); err != nil {
		t.Fatalf("generate: %v", err)
	}
	if _, _, err := loadOrCreateLocalDevKeys(); err != nil {
		t.Fatalf("reload: %v", err)
	}

	for _, perm := range []os.FileMode{0o644, 0o640, 0o604} {
		if err := os.Chmod(keyFile, perm); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadOrCreateLocalDevKeys(); err == nil {
			t.Fatalf("accepted a key file with mode %04o", perm)
		}
	}
	if err := os.Chmod(keyFile, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadOrCreateLocalDevKeys(); err != nil {
		t.Fatalf("rejected a 0600 key file: %v", err)
	}

	// A symlink at the key path would let a pre-creator redirect the read.
	linkTarget := filepath.Join(dir, "real.json")
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(linkTarget, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(keyFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linkTarget, keyFile); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := loadOrCreateLocalDevKeys(); err == nil {
		t.Fatal("accepted a symlinked key file")
	}
}

// Two processes starting together must end up on the same pair: the second must
// adopt the first's file instead of replacing it.
func TestLocalDevKeysAdoptAnExistingFileInsteadOfReplacing(t *testing.T) {
	keyFile := keyFilePath(t)
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	winner := localDevKeys{
		MasterKeyHex:       strings.Repeat("3a", 32),
		TokenSigningKeyHex: strings.Repeat("7b", 32),
	}
	raw, err := json.Marshal(winner)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	got, _, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != winner {
		t.Fatal("an existing key file was replaced instead of adopted")
	}
	// installLocalDevKeys reports false when the destination already exists.
	installed, err := installLocalDevKeys(keyFile, localDevKeys{MasterKeyHex: strings.Repeat("cd", 32), TokenSigningKeyHex: strings.Repeat("ef", 32)})
	if err != nil {
		t.Fatalf("install over existing: %v", err)
	}
	if installed {
		t.Fatal("installLocalDevKeys replaced an existing key file")
	}
	after, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(raw) {
		t.Fatal("the installed key file changed under a concurrent start")
	}
}

// An existing directory may be shared with other tooling (a 0755 config dir, or
// 0775 under a umask of 002), so the loader must neither re-permission it nor
// refuse it. The key file's own mode and ownership are the boundary.
func TestLocalDevKeysLeaveExistingDirectoryPermissionsAlone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are not meaningful on Windows")
	}
	base := t.TempDir()
	// Keep the ancestors private: the loader refuses group-writable ones.
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(base, "config")
	if err := os.MkdirAll(shared, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvLocalDevKeyFile, filepath.Join(shared, "keys.json"))

	if _, _, err := loadOrCreateLocalDevKeys(); err != nil {
		t.Fatalf("a 0755 parent directory must be usable: %v", err)
	}
	info, err := os.Stat(shared)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o755 {
		t.Fatalf("parent directory mode = %04o, want 0755 unchanged", perm)
	}

	// A parent other users can write to is refused, not repaired: they could
	// delete the key file, and the next start would rotate and strand the data.
	if err := os.Chmod(shared, 0o775); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(shared, 0o755) })
	_, _, err = loadOrCreateLocalDevKeys()
	if err == nil {
		t.Fatal("accepted a group-writable key directory")
	}
	if !strings.Contains(err.Error(), "can delete the key file") {
		t.Fatalf("error = %v, want the writable-parent rejection", err)
	}
	info, err = os.Stat(shared)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o775 {
		t.Fatalf("parent directory mode = %04o, want 0775 unchanged (no chmod)", perm)
	}
}

// The temp-dir fallback path is guessable, so a local attacker can pre-create the
// key directory as a symlink to an unrelated directory. The loader must refuse it
// instead of following the link, and must not chmod what the link points at.
func TestLocalDevKeysRejectSymlinkedDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	base := t.TempDir()
	victim := filepath.Join(base, "victim")
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	keyDir := filepath.Join(base, "keys")
	if err := os.Symlink(victim, keyDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	keyFile := filepath.Join(keyDir, "keys.json")
	t.Setenv(EnvLocalDevKeyFile, keyFile)

	if _, _, err := loadOrCreateLocalDevKeys(); err == nil {
		t.Fatal("accepted a symlinked key directory")
	}
	// The pointed-at directory must be untouched: a chmod through the link would
	// have rewritten its mode.
	info, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o755 {
		t.Fatalf("victim directory mode = %04o, want 0755 (chmod followed the symlink)", perm)
	}
	if entries, err := os.ReadDir(victim); err == nil && len(entries) != 0 {
		t.Fatalf("a key file was written through the symlinked directory: %v", entries)
	}
}

// A writable ancestor defeats the final-directory check: renaming keys/ aside
// needs write access only on its parent, so a 0775 ancestor above a 0700 keys/
// directory still lets another user substitute a directory holding a stale key
// file, or delete it to force a rotation.
func TestLocalDevKeysRejectWritableAncestor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode bits are not meaningful on Windows")
	}
	base := t.TempDir()
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(base, "shared")
	keyDir := filepath.Join(shared, "keys")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, 0o775); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(shared, 0o700) })
	t.Setenv(EnvLocalDevKeyFile, filepath.Join(keyDir, "keys.json"))

	_, _, err := loadOrCreateLocalDevKeys()
	if err == nil {
		t.Fatal("accepted a group-writable ancestor directory")
	}
	if !strings.Contains(err.Error(), "other users can replace the key directory") {
		t.Fatalf("error = %v, want the writable-ancestor rejection", err)
	}
	// A sticky world-writable ancestor (/tmp) is allowed: it refuses renames and
	// deletions of other users' entries.
	if err := os.Chmod(shared, os.ModeSticky|0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(shared, 0o700) })
	if _, _, err := loadOrCreateLocalDevKeys(); err != nil {
		t.Fatalf("sticky ancestor must be allowed: %v", err)
	}

	// A symlinked ancestor is not a threat: the kernel resolves it before the key
	// file is opened, and the final component keeps its O_NOFOLLOW validation.
	// macOS needs this (/var -> /private/var), and every $TMPDIR path traverses it.
	if err := os.Chmod(shared, 0o700); err != nil {
		t.Fatal(err)
	}
	realDir := filepath.Join(base, "real")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(base, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	linkedKeyDir := filepath.Join(linkDir, "keys")
	if err := os.MkdirAll(linkedKeyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(linkedKeyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvLocalDevKeyFile, filepath.Join(linkedKeyDir, "keys.json"))
	if _, _, err := loadOrCreateLocalDevKeys(); err != nil {
		t.Fatalf("a symlinked ancestor must be allowed (macOS /var -> /private/var): %v", err)
	}
}

// The trusted-owner set must not be a numeric range: macOS allocates interactive
// users from 501 and legacy Linux sites still hand out 500-999 ids, so a "below
// 1000 is a system account" ceiling would trust exactly the users this check
// exists to distrust.
func TestLocalDevKeyTrustedOwnerIsNotARange(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the owner checks are Unix-only")
	}
	uid := os.Getuid()
	if !localDevKeyTrustedOwner(0) {
		t.Fatal("root must be trusted")
	}
	if !localDevKeyTrustedOwner(uid) {
		t.Fatalf("the current user (%d) must be trusted", uid)
	}
	if !localDevKeyTrustedOwner(localDevKeyNobodyUID) {
		t.Fatal("nobody must be trusted so rootless containers and sandboxes work")
	}
	for _, other := range []int{501, 502, 999, uid + 1} {
		if other == 0 || other == uid || other == localDevKeyNobodyUID {
			continue
		}
		if localDevKeyTrustedOwner(other) {
			t.Fatalf("uid %d was trusted; only root, the current user and nobody may be", other)
		}
	}
}

// A hand-edited key file may carry stray whitespace. Validation normalises the
// values, so the trimmed copy that gets checked is the one that gets used.
func TestLocalDevKeysNormalizeWhitespaceBeforeValidating(t *testing.T) {
	keyFile := keyFilePath(t)
	t.Setenv(EnvLocalDevKeyFile, keyFile)
	master := strings.Repeat("1a", 32)
	token := strings.Repeat("2b", 32)
	body := `{"master_key_hex":"  ` + master + `  ","token_signing_key_hex":"\t` + token + `\n"}`
	if err := os.WriteFile(keyFile, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	keys, _, err := loadOrCreateLocalDevKeys()
	if err != nil {
		t.Fatalf("padded key file rejected: %v", err)
	}
	if keys.MasterKeyHex != master || keys.TokenSigningKeyHex != token {
		t.Fatalf("keys were not normalised: %q / %q", keys.MasterKeyHex, keys.TokenSigningKeyHex)
	}
}
