package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/term"
)

// V2d: `drive9 vault put /n/vault/<secret> --from <dir>` — wholesale,
// atomic replace of all visible keys for a single secret path.
//
// This is the minimal MVP sanctioned by the sealed 8-row pre-PR contract
// (see issue #307). The contract axes are mapped 1:1 below, and each row
// has a matching test witness in secret_put_test.go.
//
//   Row A — Argv/path shape (single `/n/vault/<secret>` + required `--from <dir>`)
//   Row B — `--from <dir>` dir semantics (wholesale replace; B1..B9 rejected)
//   Row C — Single HTTP request (atomic, Invariant #1)
//   Row D — No-partial witness (server never sees a mixed/short map)
//   Row E — Principal = owner DRIVE9_API_KEY (cap-token rejected by class)
//   Row F — Input source contract: stdin rejected with EINVAL-shaped error
//   Row G — Cross-ref to B4..B9 (no new tests; preserves 8-axis traceability)
//   Row H — Observability anchors: `aborted locally` / `server refused` /
//            `status unknown`; no-auto-retry transport (DisableKeepAlives,
//            Request.GetBody=nil).
//
// Deliberately NOT supported in MVP:
//   --merge / --upsert / --prune / --patch / stdin streaming / multiple dirs.
// Adding any of these would create a second contract surface outside §2's
// wholesale-replace model. Fail-loud > silent-drop: extra flags and stdin
// input both hit explicit rejections rather than being ignored.

// errorKind is the anchor class surfaced in error strings for Row H
// observability. Each value's String() is a user-visible anchor substring
// that operators can grep on and that tests assert verbatim.
type errorKind int

const (
	// errAbortedLocally is reserved strictly for the pre-send path where
	// the CLI can prove no byte reached the server (len(requests)==0 in
	// tests). Do NOT use this for network errors during or after the PUT —
	// once bytes are on the wire, local code cannot prove zero-byte peer
	// delivery from socket errno alone.
	errAbortedLocally errorKind = iota
	// errServerRefused: 4xx response received. Server actively rejected;
	// no ambiguity about whether the state changed (server says no).
	errServerRefused
	// errStatusUnknown: 5xx, transport-level failure mid-request, or any
	// ack-lost condition. The CLI CANNOT prove from the local side whether
	// the server applied the write or not. Operators must reconcile.
	errStatusUnknown
)

func (k errorKind) anchor() string {
	switch k {
	case errAbortedLocally:
		return "aborted locally"
	case errServerRefused:
		return "server refused"
	case errStatusUnknown:
		return "status unknown"
	}
	return ""
}

// stdinIsTTY is indirected through a var so tests can force-override it.
// Production callers go through the real isatty check.
var stdinIsTTY = func() bool {
	return term.IsTerminal(int(os.Stdin.Fd()))
}

// SecretPut implements `drive9 vault put /n/vault/<secret> --from <dir>`.
//
// Argv order is pinned: path first, `--from <dir>` is the only legal
// trailing form. Flag/argv parse errors fail loud; see the Row-labeled
// blocks below for the exact error-string anchors each branch must hit.
//
// Priority order for mixed errors (matters because Row F witness (d)
// asserts double-sided "stdin error wins over --from is required"):
//
//  1. ROW F stdin-present check runs BEFORE argv parsing. Non-TTY stdin
//     is rejected with the stdin anchor regardless of which flags are
//     passed or omitted. This is the whole point of Row F witness (d).
//  2. ROW A path-shape check (reuses parseVaultPath from V2c).
//  3. ROW E owner-credential check.
//  4. ROW B dir semantics (includes `--from` missing/empty/nonexistent/etc).
func SecretPut(args []string) error {
	// Row F: stdin input contract. The wholesale-replace payload is
	// multi-key; stdin would require adopting a new framing (tar/dotenv/
	// envdir-stream) that is out of MVP scope. We reject early — BEFORE
	// argv parse — so the error names the rejected input class rather
	// than being shadowed by a "--from is required" from argv parse.
	// Test witness (d) in Row F pins this double-sided.
	if !stdinIsTTY() {
		return fmt.Errorf("stdin input not supported for put; use --from <dir> (EINVAL)")
	}

	// Row A: argv shape. Exactly one positional (the `/n/vault/<secret>`
	// path) and exactly the `--from <dir>` flag pair. No other flags.
	if len(args) == 0 {
		return fmt.Errorf("usage drive9 vault put /n/vault/<secret> --from <dir>")
	}
	pathArg := args[0]
	name, err := parseVaultPath(pathArg)
	if err != nil {
		return err
	}

	// Parse the remainder. We accept only `--from <dir>`. Any other flag
	// (including `--merge`, `--upsert`, etc. that the spec explicitly
	// outlaws in §2) is rejected loudly.
	var fromDir string
	fromSet := false
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		arg := rest[i]
		switch arg {
		case "--from":
			if i+1 >= len(rest) {
				return fmt.Errorf("--from requires a directory argument")
			}
			i++
			fromDir = rest[i]
			fromSet = true
		default:
			if strings.HasPrefix(arg, "--") {
				return fmt.Errorf("unknown flag %q", arg)
			}
			return fmt.Errorf("unexpected argument %q", arg)
		}
	}
	if !fromSet {
		return fmt.Errorf("--from is required (wholesale replace has no other input source for put in MVP)")
	}
	if fromDir == "" {
		return fmt.Errorf("--from was given an empty value; pass a directory path")
	}

	// Row E: principal MUST be owner API key. A cap-token (delegated
	// DRIVE9_VAULT_TOKEN) cannot reach the management plane and would be
	// rejected server-side with EACCES anyway — but we stop at the CLI
	// boundary with a principal-class-naming error so the operator gets
	// a clear "wrong principal" signal instead of a generic HTTP 401/403.
	creds := ResolveCredentials()
	switch creds.Kind {
	case CredentialOwner:
		// ok
	case CredentialDelegated:
		return fmt.Errorf("drive9 vault put requires the owner API key (%s); a capability token (%s) cannot write secrets", EnvAPIKey, EnvVaultToken)
	default:
		return fmt.Errorf("missing tenant API key; set %s or run drive9 create", EnvAPIKey)
	}

	// Row B: load `--from <dir>` and validate every key/value pair via
	// the V2c strict charset. B1..B9 rejected-classes are:
	//
	//   B1 path does not exist / is not a directory
	//   B2 directory is empty
	//   B3 contains non-regular entry (symlink / subdir / device / socket)
	//   B4 key violates [A-Z_][A-Z0-9_]* charset
	//   B5 value contains forbidden control byte (`\x00`..`\x1f` except `\t`)
	//   B6 (reserved — handled by filesystem case)
	//   B7 filename duplicate (case-preserving; same name after read)
	//   B8 unreadable file (permission / IO)
	//   B9 filename with path separator or dotfile
	fields, err := loadSecretDir(fromDir)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return fmt.Errorf("--from directory %q contains no secret files (B2)", fromDir)
	}
	// Reuse the V2c @env validator verbatim so there is a single source
	// of truth for key charset and value control-byte rules.
	validated, err := validateVaultEnvFields(fields)
	if err != nil {
		return err
	}

	// Row C+D: single atomic HTTP request with the whole map. A partial
	// server view is impossible by construction because we serialize the
	// full map once and do not re-issue on failure.
	//
	// Row H no-auto-retry: Go's net/http will otherwise auto-retry
	// idempotent PUTs on connection-reset / EOF-before-response. That
	// would turn a lost ack into a silent double-apply and falsify the
	// "single HTTP request" contract. We disable it two ways:
	//   (1) Transport.DisableKeepAlives = true (no reuse, no silent retry)
	//   (2) Request.GetBody = nil (net/http cannot rewind even if it wanted to)
	resp, sendErr := putSecretAtomic(context.Background(), creds.Server, creds.APIKey, name, validated)
	if sendErr != nil {
		// Pre-send errors (URL build, request construct) are the only
		// path that can legitimately claim `aborted locally` — everything
		// past http.Client.Do is status-unknown territory.
		if errors.Is(sendErr, errPreSend) {
			return fmt.Errorf("drive9 vault put %s: aborted locally: %w", pathArg, sendErr)
		}
		// Transport-layer failure during or after send — we have no proof
		// of non-delivery. Bucket as status-unknown.
		return fmt.Errorf("drive9 vault put %s: status unknown: %w", pathArg, sendErr)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 500 {
		// 5xx: server acknowledged receipt but its own state is ambiguous.
		// We surface status-unknown so operators reconcile.
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("drive9 vault put %s: status unknown: HTTP %d", pathArg, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drive9 vault put %s: server refused: HTTP %d: %s", pathArg, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// errPreSend marks errors that the CLI constructed locally before any byte
// could hit the wire. Only those can be reported as `aborted locally`.
var errPreSend = errors.New("pre-send")

// putSecretAtomic builds a fresh, no-retry http.Client, sends a single PUT
// with the full map as JSON, and returns the response. The caller closes
// the body. Any error wrapping errPreSend is known to be before-send;
// any other non-nil error is transport-level during/after send.
func putSecretAtomic(ctx context.Context, server, apiKey, name string, fields map[string]string) (*http.Response, error) {
	if server == "" {
		return nil, fmt.Errorf("%w: no server URL resolved", errPreSend)
	}
	body, err := json.Marshal(map[string]any{
		"fields":     fields,
		"updated_by": "drive9-cli",
	})
	if err != nil {
		return nil, fmt.Errorf("%w: marshal body: %v", errPreSend, err)
	}
	target := strings.TrimRight(server, "/") + "/v1/vault/secrets/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %v", errPreSend, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	// Row H: kill Go net/http idempotent auto-retry. Without these two
	// knobs, a server that closes the keepalive connection after 200 OK
	// will cause the stdlib to silently re-issue the PUT on the next call
	// — but crucially also during a single Do() if net/http thinks the
	// conn was bad. We cannot let that happen: V2d's contract is exactly
	// one state transition per call.
	req.GetBody = nil
	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}
	return client.Do(req)
}

// loadSecretDir reads every regular file directly under dir and returns
// {filename -> file contents}. Filenames become env-key candidates; they
// are validated by the caller via validateVaultEnvFields against the
// strict `[A-Z_][A-Z0-9_]*` charset. Subdirectories, symlinks, and
// dotfiles are all rejected — the dir is flat-only by design.
//
// Rejected classes map to the 8-row Row B test matrix:
//
//	B1  dir missing / not-a-directory
//	B3  non-regular entry (symlink, subdir, device, socket)
//	B7  post-read same-filename collision (can't happen on a real FS,
//	    but we assert the invariant in case of case-insensitive FS quirks)
//	B8  unreadable file
//	B9  path-separator or dotfile in name
func loadSecretDir(dir string) (map[string]string, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("--from directory %q does not exist (B1)", dir)
		}
		return nil, fmt.Errorf("--from directory %q: %w (B1)", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("--from %q is not a directory (B1)", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("--from directory %q: read: %w (B8)", dir, err)
	}
	// Sort so error ordering is deterministic: whichever B-class a test
	// stages first is the one asserted.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	fields := make(map[string]string, len(entries))
	for _, ent := range entries {
		name := ent.Name()
		// B9: reject dotfiles and any name that tries to smuggle a path
		// separator. The OS already filters `/` at Readdir time on unix,
		// but we pin the invariant for belt-and-suspenders.
		if strings.HasPrefix(name, ".") {
			return nil, fmt.Errorf("--from entry %q: dotfiles are not allowed (B9)", name)
		}
		if strings.ContainsAny(name, "/\\") {
			return nil, fmt.Errorf("--from entry %q: path separators are not allowed in filename (B9)", name)
		}
		// B3: only regular files. Symlinks, subdirs, devices, sockets all
		// reject here with the filename named so operators can fix it.
		fi, err := ent.Info()
		if err != nil {
			return nil, fmt.Errorf("--from entry %q: stat: %w (B8)", name, err)
		}
		if !fi.Mode().IsRegular() {
			return nil, fmt.Errorf("--from entry %q: not a regular file (B3)", name)
		}
		// B8: read. io.ReadAll fails the whole operation — partial
		// results are never returned.
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("--from entry %q: read: %w (B8)", name, err)
		}
		// B7: same-key dupe. Can happen on case-insensitive filesystems
		// (macOS default HFS+) when two names differ only in case but
		// map to the same key. We reject rather than silently letting
		// one win.
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("--from entry %q: duplicate filename after read (B7)", name)
		}
		fields[name] = string(data)
	}
	return fields, nil
}
