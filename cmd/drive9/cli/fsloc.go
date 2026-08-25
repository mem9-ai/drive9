package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/mem9-ai/drive9/pkg/objectfs"
)

// Kind classifies a parsed location.
type Kind int

const (
	KindLocal Kind = iota
	KindStdin
	KindDrive9
	KindObject
)

func (k Kind) String() string {
	switch k {
	case KindLocal:
		return "local"
	case KindStdin:
		return "stdin"
	case KindDrive9:
		return "drive9"
	case KindObject:
		return "object"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

// Scheme is a URI scheme the CLI recognizes.
type Scheme string

const (
	SchemeDrive9 Scheme = "drive9"
	SchemeFile   Scheme = "file"
	SchemeS3     Scheme = "s3"
	SchemeCOS    Scheme = "cos"
	SchemeTOS    Scheme = "tos"
	SchemeOSS    Scheme = "oss"
	SchemeGS     Scheme = "gs"
	SchemeGCS    Scheme = "gcs" // alias of gs
	SchemeAZ     Scheme = "az"
	SchemeAzure  Scheme = "azure" // alias of az
)

// Location is a parsed CLI path. Parse never returns a registered
// scheme:// URI as KindLocal.
type Location struct {
	Raw     string
	Kind    Kind
	Scheme  Scheme
	Context string // KindDrive9 named context only
	Bucket  string
	Path    string // drive9: leading /; object: decoded key
	DirHint bool
	Query   map[string]string
	Local   string // KindLocal filesystem path
}

// Object query keys allowed on object-store URIs.
const (
	QueryRegion         = "region"
	QueryEndpoint       = "endpoint"
	QueryProvider       = "provider"
	QueryForcePathStyle = "forcePathStyle"
	QueryVersionID      = "versionId"
	QueryAccount        = "account"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrNotSupported        = errors.New("operation not supported by backend")
	ErrUnsupportedScheme   = errors.New("unsupported remote scheme")
	ErrNotDir              = errors.New("not a directory")
	ErrIsDir               = errors.New("is a directory")
	ErrCrossBackend        = errors.New("operation requires the same backend identity")
	ErrCollisionFilePrefix = errors.New("object key exists both as file and as prefix")
	ErrInvalidLocation     = errors.New("invalid location")
)

// Capability is a bitset of backend operations.
type Capability uint64

const (
	CapStat Capability = 1 << iota
	CapList
	CapRead
	CapRangeRead
	CapWrite
	CapDelete
	CapMkdir
	CapRenameSame
	CapCopySame
	CapAppend
	CapChmod
	CapSymlink
	CapHardlink
	CapTags
	CapDescription
	CapSearch
	CapLayer
	CapMultipart
)

// Has reports whether c includes want.
func (c Capability) Has(want Capability) bool {
	return c&want == want
}

// FileInfo is a backend-neutral directory or stat entry.
type FileInfo struct {
	Name        string
	Path        string
	Size        int64
	IsDir       bool
	Mtime       time.Time
	ETag        string
	Mode        uint32
	HasMode     bool
	ContentType string
}

// ListOpts controls one page of listing.
type ListOpts struct {
	Recursive bool
	Cursor    string
	Limit     int
}

// ListPage is one page of listing results.
type ListPage struct {
	Entries    []FileInfo
	NextCursor string
}

// WriteOpts configures OpenWrite.
type WriteOpts struct {
	Size        int64 // -1 = unknown
	Overwrite   bool
	ContentType string
	Tags        map[string]string
	Description string
}

// WriteHandle is a streaming write that can complete or abort.
type WriteHandle interface {
	io.Writer
	Close() error
	Abort(ctx context.Context) error
}

// Backend is a remote or local filesystem.
type Backend interface {
	Scheme() Scheme
	Caps() Capability
	Close() error

	Stat(ctx context.Context, loc Location) (*FileInfo, error)
	List(ctx context.Context, loc Location, opts ListOpts) (ListPage, error)
	OpenRead(ctx context.Context, loc Location) (io.ReadCloser, error)
	OpenReadRange(ctx context.Context, loc Location, offset, length int64) (io.ReadCloser, error)
	OpenWrite(ctx context.Context, loc Location, opts WriteOpts) (WriteHandle, error)
	Remove(ctx context.Context, loc Location, recursive bool) error
	Mkdir(ctx context.Context, loc Location) error
}

// Renamer is an optional same-backend rename.
type Renamer interface {
	Rename(ctx context.Context, old, new Location) error
}

// SameCopier is an optional same-backend server-side copy.
type SameCopier interface {
	Copy(ctx context.Context, src, dst Location) error
}

// Identifier reports a stable backend identity for same-backend routing.
type Identifier interface {
	Identity() string
}

// IsRegisteredScheme reports whether s is a URI scheme the CLI classifies.
func IsRegisteredScheme(s string) bool {
	switch Scheme(strings.ToLower(s)) {
	case SchemeDrive9, SchemeFile,
		SchemeS3, SchemeCOS, SchemeTOS, SchemeOSS,
		SchemeGS, SchemeGCS, SchemeAZ, SchemeAzure:
		return true
	default:
		return false
	}
}

// IsReservedContextName reports whether name collides with a registered scheme.
func IsReservedContextName(name string) bool {
	return IsRegisteredScheme(name)
}

// SameIdentity reports whether two backends are the same routing identity.
func SameIdentity(a, b Backend) bool {
	ia, oka := a.(Identifier)
	ib, okb := b.(Identifier)
	if !oka || !okb {
		return false
	}
	return ia.Identity() != "" && ia.Identity() == ib.Identity()
}

// RequireCap returns a CLI-facing error when b lacks cap.
func RequireCap(b Backend, cap Capability, verb string) error {
	if b == nil || b.Caps().Has(cap) {
		return nil
	}
	scheme := ""
	if b != nil {
		scheme = string(b.Scheme())
	}
	return fmt.Errorf("drive9 fs %s: backend %q does not support %s", verb, scheme, capName(cap))
}

func capName(c Capability) string {
	switch c {
	case CapChmod:
		return "chmod"
	case CapSearch:
		return "find/grep"
	case CapLayer:
		return "layer"
	case CapSymlink:
		return "symlink"
	case CapHardlink:
		return "hardlink"
	case CapAppend:
		return "append"
	case CapTags:
		return "tags"
	case CapDescription:
		return "description"
	case CapRenameSame:
		return "mv"
	default:
		return "this operation"
	}
}

// Parse classifies a CLI path argument. Registered scheme:// URIs are never
// returned as KindLocal.
func Parse(s string) (Location, error) {
	loc := Location{Raw: s, Query: map[string]string{}}
	if s == "-" {
		loc.Kind = KindStdin
		return loc, nil
	}

	scheme, rest, ok := splitRegisteredScheme(s)
	if ok {
		return parseRegisteredURI(s, scheme, rest)
	}

	// Existing :/ grammar. Unregistered https:// stays here on purpose.
	idx := strings.Index(s, ":/")
	if idx < 0 {
		if strings.HasPrefix(s, ":") {
			loc.Kind = KindDrive9
			loc.Scheme = SchemeDrive9
			loc.Path = normalizeDrive9Path(s[1:])
			return loc, nil
		}
		loc.Kind = KindLocal
		loc.Local = s
		return loc, nil
	}
	if idx == 1 {
		loc.Kind = KindLocal
		loc.Local = s
		return loc, nil
	}
	loc.Kind = KindDrive9
	loc.Scheme = SchemeDrive9
	loc.Context = s[:idx]
	loc.Path = s[idx+1:]
	if loc.Path == "" {
		loc.Path = "/"
	}
	return loc, nil
}

func splitRegisteredScheme(s string) (scheme string, rest string, ok bool) {
	// ^[A-Za-z][A-Za-z0-9+.-]*://
	if len(s) < 4 {
		return "", "", false
	}
	colon := strings.Index(s, "://")
	if colon < 1 {
		return "", "", false
	}
	raw := s[:colon]
	if raw[0] > unicode.MaxASCII || !isAlpha(raw[0]) {
		return "", "", false
	}
	for i := 1; i < len(raw); i++ {
		c := raw[i]
		if !isAlphaNum(c) && c != '+' && c != '.' && c != '-' {
			return "", "", false
		}
	}
	lower := strings.ToLower(raw)
	if !IsRegisteredScheme(lower) {
		return "", "", false
	}
	return lower, s[colon+3:], true
}

func isAlpha(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func isAlphaNum(c byte) bool {
	return isAlpha(c) || (c >= '0' && c <= '9')
}

func parseRegisteredURI(raw, scheme, _ string) (Location, error) {
	if objectfs.IsScheme(scheme) {
		oloc, err := objectfs.Parse(raw)
		if err != nil {
			return Location{}, mapObjectErr(err)
		}
		return Location{
			Raw:     oloc.Raw,
			Kind:    KindObject,
			Scheme:  Scheme(oloc.Scheme),
			Bucket:  oloc.Bucket,
			Path:    oloc.Path,
			DirHint: oloc.DirHint,
			Query:   oloc.Query,
		}, nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return Location{}, fmt.Errorf("%w: %s: %v", ErrInvalidLocation, raw, err)
	}
	if u.User != nil {
		return Location{}, fmt.Errorf("%w: userinfo is not allowed in %s", ErrInvalidLocation, raw)
	}
	if err := rejectQuery(u, scheme); err != nil {
		return Location{}, err
	}

	loc := Location{
		Raw:    raw,
		Scheme: Scheme(scheme),
		Query:  map[string]string{},
	}
	switch Scheme(scheme) {
	case SchemeDrive9:
		loc.Kind = KindDrive9
		loc.Path = opaqueDrive9Path(u)
		return loc, nil
	case SchemeFile:
		p, err := filePathFromURL(u)
		if err != nil {
			return Location{}, err
		}
		loc.Kind = KindLocal
		loc.Local = p
		loc.Path = p
		return loc, nil
	default:
		return Location{}, fmt.Errorf("%w: %q", ErrUnsupportedScheme, scheme)
	}
}

func rejectQuery(u *url.URL, scheme string) error {
	if u.RawQuery == "" {
		return nil
	}
	return fmt.Errorf("%w: query is not allowed on %s://", ErrInvalidLocation, scheme)
}

func opaqueDrive9Path(u *url.URL) string {
	var b strings.Builder
	if u.Host != "" {
		b.WriteString(u.Host)
	}
	if u.Path != "" {
		b.WriteString(u.Path)
	}
	return normalizeDrive9Path(b.String())
}

func normalizeDrive9Path(p string) string {
	p = strings.TrimLeft(p, "/")
	if p == "" {
		return "/"
	}
	return "/" + p
}

func filePathFromURL(u *url.URL) (string, error) {
	host := strings.ToLower(u.Host)
	if host != "" && host != "localhost" {
		return "", fmt.Errorf("%w: file:// host %q is not allowed", ErrInvalidLocation, u.Host)
	}
	if u.Path == "" || u.Path == "/" && host == "" && !strings.HasPrefix(u.String(), "file:///") {
		return "", fmt.Errorf("%w: file:// requires an absolute path", ErrInvalidLocation)
	}
	if !strings.HasPrefix(u.Path, "/") {
		return "", fmt.Errorf("%w: file:// requires an absolute path", ErrInvalidLocation)
	}
	return u.Path, nil
}

func mapObjectErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, objectfs.ErrNotFound):
		return fmt.Errorf("%v: %w", err, ErrNotFound)
	case errors.Is(err, objectfs.ErrInvalidLocation):
		return fmt.Errorf("%v: %w", err, ErrInvalidLocation)
	case errors.Is(err, objectfs.ErrUnsupportedScheme):
		return fmt.Errorf("%v: %w", err, ErrUnsupportedScheme)
	default:
		return err
	}
}

type RemotePath struct {
	Context string
	Path    string
}

// ParseRemote is the legacy drive9-only classifier. New code must use
// Parse. Registered scheme:// URIs are NOT classified here for
// command dispatch — callers that still use this for "is remote?" will
// mis-handle s3://; those sites must call Parse.
func ParseRemote(s string) (RemotePath, bool) {
	if s == "-" {
		return RemotePath{}, false
	}
	idx := strings.Index(s, ":/")
	if idx < 0 {
		if strings.HasPrefix(s, ":") {
			return RemotePath{Path: s[1:]}, true
		}
		return RemotePath{}, false
	}
	if idx == 1 {
		return RemotePath{}, false
	}
	return RemotePath{
		Context: s[:idx],
		Path:    s[idx+1:],
	}, true
}

// promoteBareFSArg lifts a bare absolute POSIX path to the current
// drive9 context. Relative paths and C:/ stay KindLocal (callers still
// send the raw string to the current Client except for cp).
func promoteBareFSArg(loc Location) Location {
	if loc.Kind != KindLocal {
		return loc
	}
	if loc.Scheme == SchemeFile {
		return loc
	}
	raw := loc.Raw
	if raw == "" {
		raw = loc.Local
	}
	if isAbsPOSIX(raw) {
		loc.Kind = KindDrive9
		loc.Scheme = SchemeDrive9
		loc.Path = raw
		loc.Local = ""
		return loc
	}
	return loc
}

func isAbsPOSIX(s string) bool {
	return len(s) > 0 && s[0] == '/'
}

func locationAsRemotePath(loc Location) (RemotePath, bool) {
	if loc.Kind != KindDrive9 {
		return RemotePath{}, false
	}
	return RemotePath{Context: loc.Context, Path: loc.Path}, true
}

func objectSchemeError(loc Location) error {
	return fmt.Errorf("unsupported remote scheme %q", loc.Scheme)
}
