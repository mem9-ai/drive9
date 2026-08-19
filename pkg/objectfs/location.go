package objectfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode"
)

// Scheme is an object-store URI scheme.
type Scheme string

const (
	SchemeS3    Scheme = "s3"
	SchemeCOS   Scheme = "cos"
	SchemeTOS   Scheme = "tos"
	SchemeOSS   Scheme = "oss"
	SchemeGS    Scheme = "gs"
	SchemeGCS   Scheme = "gcs" // alias of gs
	SchemeAZ    Scheme = "az"
	SchemeAzure Scheme = "azure" // alias of az
)

// Location is a parsed object-store URI.
type Location struct {
	Raw     string
	Scheme  Scheme
	Bucket  string
	Path    string
	DirHint bool
	Query   map[string]string
}

const (
	QueryRegion         = "region"
	QueryEndpoint       = "endpoint"
	QueryProvider       = "provider"
	QueryForcePathStyle = "forcePathStyle"
	QueryVersionID      = "versionId"
	QueryAccount        = "account"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrInvalidLocation   = errors.New("invalid location")
	ErrUnsupportedScheme = errors.New("unsupported remote scheme")
)

var s3QueryKeys = map[string]bool{
	QueryRegion:         true,
	QueryEndpoint:       true,
	QueryProvider:       true,
	QueryForcePathStyle: true,
}

var gsQueryKeys = map[string]bool{
	QueryEndpoint: true,
}

var azQueryKeys = map[string]bool{
	QueryAccount:  true,
	QueryEndpoint: true,
}

// FileInfo is a directory or object stat entry.
type FileInfo struct {
	Name  string
	Path  string
	Size  int64
	IsDir bool
	Mtime time.Time
}

// ListOpts controls one listing page.
type ListOpts struct {
	Recursive bool
	Cursor    string
	Limit     int
}

// ListPage is one listing page.
type ListPage struct {
	Entries    []FileInfo
	NextCursor string
}

// WriteOpts configures OpenWrite.
type WriteOpts struct {
	Size      int64
	Overwrite bool
}

// WriteHandle is a streaming write that can complete or abort.
type WriteHandle interface {
	io.Writer
	Close() error
	Abort(ctx context.Context) error
}

// CanonicalScheme maps aliases onto the scheme stored on Location.
func CanonicalScheme(s Scheme) Scheme {
	switch Scheme(strings.ToLower(string(s))) {
	case SchemeGCS, SchemeGS:
		return SchemeGS
	case SchemeAzure, SchemeAZ:
		return SchemeAZ
	default:
		return Scheme(strings.ToLower(string(s)))
	}
}

// IsScheme reports whether s is an object-store scheme or alias.
func IsScheme(s string) bool {
	switch CanonicalScheme(Scheme(s)) {
	case SchemeS3, SchemeCOS, SchemeTOS, SchemeOSS, SchemeGS, SchemeAZ:
		return true
	default:
		return false
	}
}

// Schemes returns reserved object-store scheme names, including aliases.
func Schemes() []string {
	return []string{
		string(SchemeS3), string(SchemeCOS), string(SchemeTOS), string(SchemeOSS),
		string(SchemeGS), string(SchemeGCS), string(SchemeAZ), string(SchemeAzure),
	}
}

// Parse parses an object-store URI. gcs:// and azure:// are stored as gs/az.
func Parse(s string) (Location, error) {
	scheme, _, ok := splitObjectScheme(s)
	if !ok {
		return Location{}, fmt.Errorf("%w: %q", ErrUnsupportedScheme, s)
	}
	u, err := url.Parse(s)
	if err != nil {
		return Location{}, fmt.Errorf("%w: %s: %v", ErrInvalidLocation, s, err)
	}
	if u.User != nil {
		return Location{}, fmt.Errorf("%w: userinfo is not allowed in %s", ErrInvalidLocation, s)
	}
	canon := CanonicalScheme(Scheme(scheme))
	q, err := parseObjectQuery(u, canon)
	if err != nil {
		return Location{}, err
	}
	if u.Host == "" {
		return Location{}, fmt.Errorf("%w: empty bucket in %s", ErrInvalidLocation, s)
	}
	key := strings.TrimPrefix(u.Path, "/")
	dirHint := strings.HasSuffix(u.Path, "/") || (u.Path == "" && strings.HasSuffix(s, "/"))
	if key == "" {
		dirHint = true
	}
	return Location{
		Raw:     s,
		Scheme:  canon,
		Bucket:  u.Host,
		Path:    key,
		DirHint: dirHint,
		Query:   q,
	}, nil
}

func splitObjectScheme(s string) (scheme string, rest string, ok bool) {
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
	if !IsScheme(lower) {
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

func queryKeysFor(scheme Scheme) map[string]bool {
	switch CanonicalScheme(scheme) {
	case SchemeGS:
		return gsQueryKeys
	case SchemeAZ:
		return azQueryKeys
	default:
		return s3QueryKeys
	}
}

func parseObjectQuery(u *url.URL, scheme Scheme) (map[string]string, error) {
	if u.RawQuery == "" {
		return map[string]string{}, nil
	}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("%w: query: %v", ErrInvalidLocation, err)
	}
	allowed := queryKeysFor(scheme)
	out := make(map[string]string, len(values))
	for rawKey, vs := range values {
		key := canonicalQueryKey(rawKey)
		if len(vs) != 1 {
			return nil, fmt.Errorf("%w: duplicate query key %q", ErrInvalidLocation, rawKey)
		}
		if !allowed[key] {
			return nil, fmt.Errorf("%w: unknown query key %q", ErrInvalidLocation, rawKey)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("%w: duplicate query key %q", ErrInvalidLocation, rawKey)
		}
		out[key] = vs[0]
	}
	if v, ok := out[QueryForcePathStyle]; ok && v != "true" && v != "false" {
		return nil, fmt.Errorf("%w: forcePathStyle must be true or false", ErrInvalidLocation)
	}
	if v, ok := out[QueryProvider]; ok {
		switch v {
		case "aws", "tencent", "volcengine", "aliyun":
		default:
			return nil, fmt.Errorf("%w: unknown provider %q", ErrInvalidLocation, v)
		}
	}
	if v, ok := out[QueryAccount]; ok && strings.TrimSpace(v) == "" {
		return nil, fmt.Errorf("%w: account must not be empty", ErrInvalidLocation)
	}
	if v, ok := out[QueryEndpoint]; ok {
		if err := validateEndpoint(v); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func canonicalQueryKey(k string) string {
	switch strings.ToLower(k) {
	case "region":
		return QueryRegion
	case "endpoint":
		return QueryEndpoint
	case "provider":
		return QueryProvider
	case "forcepathstyle":
		return QueryForcePathStyle
	case "versionid":
		return QueryVersionID
	case "account":
		return QueryAccount
	default:
		return k
	}
}

func validateEndpoint(ep string) error {
	u, err := url.Parse(ep)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%w: endpoint must be an absolute URL", ErrInvalidLocation)
	}
	if u.User != nil {
		return fmt.Errorf("%w: userinfo is not allowed in endpoint", ErrInvalidLocation)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if host == "127.0.0.1" || host == "localhost" || host == "::1" {
			return nil
		}
		return fmt.Errorf("%w: endpoint must use https:// (http:// is only allowed for loopback)", ErrInvalidLocation)
	default:
		return fmt.Errorf("%w: endpoint scheme %q not allowed", ErrInvalidLocation, u.Scheme)
	}
}
