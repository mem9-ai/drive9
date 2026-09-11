package extent

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

const (
	SchemeFile = "file"
	SchemeS3   = "s3"
)

// Credential is the tenant-prefix object-store session from POST /v1/data-credential.
type Credential struct {
	Scheme          string `json:"scheme"`
	Endpoint        string `json:"endpoint"`
	Bucket          string `json:"bucket,omitempty"`
	Prefix          string `json:"prefix"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

// CredentialSource refreshes STS sessions. *client.Client implements this.
type CredentialSource interface {
	GetDataCredential(ctx context.Context) (*Credential, error)
}

// OpenStorage builds a JuiceFS ObjectStorage from a data-credential response.
// file is the in-process mock used by unit tests. s3 is the production and
// local-MinIO data plane (STS or static keys). Blocks are never proxied
// through drive9-server.
func OpenStorage(cred *Credential, src CredentialSource) (object.ObjectStorage, error) {
	if cred == nil {
		return nil, fmt.Errorf("nil data credential")
	}
	inner, err := openInner(cred)
	if err != nil {
		return nil, err
	}
	prefix := cred.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix != "" {
		inner = object.WithPrefix(inner, prefix)
	}
	return &refreshingStore{
		src:    src,
		inner:  inner,
		cred:   *cred,
		scheme: cred.Scheme,
	}, nil
}

func openInner(cred *Credential) (object.ObjectStorage, error) {
	switch strings.ToLower(strings.TrimSpace(cred.Scheme)) {
	case SchemeFile, "":
		root := cred.Endpoint
		if root == "" {
			return nil, fmt.Errorf("file storage endpoint is required")
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(root, "/") {
			root += "/"
		}
		st, err := object.CreateStorage("file", root, "", "", "")
		if err != nil {
			return nil, err
		}
		return st, st.Create(context.Background())
	case SchemeS3:
		ep := cred.Endpoint
		if cred.Bucket != "" && !strings.Contains(ep, cred.Bucket) {
			ep = strings.TrimRight(ep, "/") + "/" + cred.Bucket
		}
		st, err := object.CreateStorage("s3", ep, cred.AccessKeyID, cred.SecretAccessKey, cred.SessionToken)
		if err != nil {
			return nil, err
		}
		return st, nil
	case "drive9":
		return nil, fmt.Errorf("extent data plane does not proxy blocks through drive9-server")
	default:
		return nil, fmt.Errorf("unsupported data-credential scheme %q", cred.Scheme)
	}
}

type refreshingStore struct {
	object.DefaultObjectStorage
	src    CredentialSource
	mu     sync.Mutex
	inner  object.ObjectStorage
	cred   Credential
	scheme string
	puts   atomic.Int64
	gets   atomic.Int64
}

func (s *refreshingStore) String() string {
	return "drive9-" + s.scheme
}

func (s *refreshingStore) Scheme() string { return s.scheme }

func (s *refreshingStore) Prefix() string { return s.cred.Prefix }

func (s *refreshingStore) PutCount() int64 { return s.puts.Load() }

func (s *refreshingStore) GetCount() int64 { return s.gets.Load() }

func (s *refreshingStore) innerStore(ctx context.Context) (object.ObjectStorage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.src == nil || s.cred.ExpiresAt == "" {
		return s.inner, nil
	}
	exp, err := time.Parse(time.RFC3339, s.cred.ExpiresAt)
	if err != nil {
		return s.inner, nil
	}
	if time.Until(exp) > 2*time.Minute {
		return s.inner, nil
	}
	next, err := s.src.GetDataCredential(ctx)
	if err != nil {
		return s.inner, nil
	}
	inner, err := OpenStorage(next, s.src)
	if err != nil {
		return nil, err
	}
	if rs, ok := inner.(*refreshingStore); ok {
		s.inner = rs.inner
		s.cred = rs.cred
		s.scheme = rs.scheme
	} else {
		s.inner = inner
		s.cred = *next
		s.scheme = next.Scheme
	}
	return s.inner, nil
}

func (s *refreshingStore) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	inner, err := s.innerStore(ctx)
	if err != nil {
		return nil, err
	}
	s.gets.Add(1)
	return inner.Get(ctx, key, off, limit, getters...)
}

func (s *refreshingStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	inner, err := s.innerStore(ctx)
	if err != nil {
		return err
	}
	s.puts.Add(1)
	return inner.Put(ctx, key, in, getters...)
}

func (s *refreshingStore) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	inner, err := s.innerStore(ctx)
	if err != nil {
		return err
	}
	return inner.Delete(ctx, key, getters...)
}

func (s *refreshingStore) Head(ctx context.Context, key string) (object.Object, error) {
	inner, err := s.innerStore(ctx)
	if err != nil {
		return nil, err
	}
	return inner.Head(ctx, key)
}
