package objectfs

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/walk"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// MintSession mints a replacement session for a long-lived object mount.
type MintSession func(ctx context.Context) (SessionCredentials, time.Time, error)

const (
	sessionRefreshMinWait  = 5 * time.Second
	sessionRefreshMinLead  = 30 * time.Second
	sessionRefreshMaxLead  = 15 * time.Minute
	sessionRefreshMintWait = 30 * time.Second
	sessionRefreshFailWait = 30 * time.Second
	sessionRefreshFailCap  = 5 * time.Minute
	sessionDefaultTTL      = 50 * time.Minute
)

type sessionFs struct {
	mu    sync.RWMutex
	inner fs.Fs
	name  string
	root  string
}

func newSessionFs(inner fs.Fs, name, root string) *sessionFs {
	return &sessionFs{inner: inner, name: name, root: root}
}

func (s *sessionFs) current() fs.Fs {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inner
}

func (s *sessionFs) replace(inner fs.Fs) {
	s.mu.Lock()
	s.inner = inner
	s.mu.Unlock()
}

func (s *sessionFs) Name() string {
	return s.name
}

func (s *sessionFs) Root() string {
	return s.root
}

func (s *sessionFs) String() string {
	return s.name + ":" + s.root
}

func (s *sessionFs) Precision() time.Duration {
	return s.current().Precision()
}

func (s *sessionFs) Hashes() hash.Set {
	return s.current().Hashes()
}

func (s *sessionFs) Features() *fs.Features {
	inner := s.current().Features()
	ft := &fs.Features{
		BucketBased:             inner.BucketBased,
		BucketBasedRootOK:       inner.BucketBasedRootOK,
		CanHaveEmptyDirectories: inner.CanHaveEmptyDirectories,
		ReadMimeType:            inner.ReadMimeType,
		WriteMimeType:           inner.WriteMimeType,
		SlowHash:                inner.SlowHash,
		SlowModTime:             inner.SlowModTime,
	}
	return ft.Fill(context.Background(), s)
}

func (s *sessionFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	return s.current().List(ctx, dir)
}

func (s *sessionFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return s.current().NewObject(ctx, remote)
}

func (s *sessionFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return s.current().Put(ctx, in, src, options...)
}

func (s *sessionFs) Mkdir(ctx context.Context, dir string) error {
	return s.current().Mkdir(ctx, dir)
}

func (s *sessionFs) Rmdir(ctx context.Context, dir string) error {
	return s.current().Rmdir(ctx, dir)
}

func (s *sessionFs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if c, ok := s.current().(fs.Copier); ok {
		return c.Copy(ctx, src, remote)
	}
	return nil, fs.ErrorCantCopy
}

func (s *sessionFs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if m, ok := s.current().(fs.Mover); ok {
		return m.Move(ctx, src, remote)
	}
	return nil, fs.ErrorCantMove
}

func (s *sessionFs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if p, ok := s.current().(fs.PutStreamer); ok {
		return p.PutStream(ctx, in, src, options...)
	}
	return s.Put(ctx, in, src, options...)
}

func (s *sessionFs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	if lr, ok := s.current().(fs.ListRer); ok {
		return lr.ListR(ctx, dir, callback)
	}
	return walk.ErrorCantListR
}

func (s *sessionFs) Purge(ctx context.Context, dir string) error {
	if p, ok := s.current().(fs.Purger); ok {
		return p.Purge(ctx, dir)
	}
	return fs.ErrorCantPurge
}

var (
	_ fs.Fs          = (*sessionFs)(nil)
	_ fs.Copier      = (*sessionFs)(nil)
	_ fs.Mover       = (*sessionFs)(nil)
	_ fs.PutStreamer = (*sessionFs)(nil)
	_ fs.ListRer     = (*sessionFs)(nil)
	_ fs.Purger      = (*sessionFs)(nil)
)

func startSessionRefresh(parent context.Context, s *sessionFs, loc Location, mint MintSession, expiry time.Time) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	go runSessionRefresh(ctx, s, loc, mint, expiry)
	return cancel
}

func runSessionRefresh(ctx context.Context, s *sessionFs, loc Location, mint MintSession, expiry time.Time) {
	failWait := sessionRefreshFailWait
	for {
		wait := sessionRefreshWait(effectiveSessionExpiry(expiry, time.Now()), time.Now())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		mintCtx, mintCancel := context.WithTimeout(ctx, sessionRefreshMintWait)
		sess, nextExp, err := mint(mintCtx)
		mintCancel()
		if err != nil {
			logger.Warn(ctx, "object mount session refresh failed; will retry", zap.Error(err), zap.Duration("retry_in", failWait))
			expiry = time.Now().Add(failWait)
			if failWait < sessionRefreshFailCap {
				failWait *= 2
				if failWait > sessionRefreshFailCap {
					failWait = sessionRefreshFailCap
				}
			}
			continue
		}
		failWait = sessionRefreshFailWait
		openCtx, openCancel := context.WithTimeout(ctx, sessionRefreshMintWait)
		next, err := OpenFsBucketWithSession(openCtx, loc, sess)
		openCancel()
		if err != nil {
			logger.Warn(ctx, "object mount failed to open refreshed session; will retry", zap.Error(err))
			expiry = time.Now().Add(failWait)
			continue
		}
		s.replace(next)
		expiry = nextExp
		logger.Info(ctx, "object mount session refreshed", zap.Time("expiration", effectiveSessionExpiry(expiry, time.Now())))
	}
}

func effectiveSessionExpiry(exp, now time.Time) time.Time {
	if exp.IsZero() {
		return now.Add(sessionDefaultTTL)
	}
	return exp
}

func sessionRefreshWait(exp, now time.Time) time.Duration {
	until := exp.Sub(now)
	if until <= 0 {
		return sessionRefreshMinWait
	}
	if until <= 30*time.Second {
		half := until / 2
		if half < sessionRefreshMinWait {
			return sessionRefreshMinWait
		}
		return half
	}
	lead := until / 4
	if lead > sessionRefreshMaxLead {
		lead = sessionRefreshMaxLead
	}
	if lead < sessionRefreshMinLead {
		lead = sessionRefreshMinLead
	}
	wait := until - lead
	if wait < sessionRefreshMinWait {
		return sessionRefreshMinWait
	}
	return wait
}

// ParseSessionExpiry parses a mint expiration timestamp (RFC3339).
func ParseSessionExpiry(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC()
	}
	return time.Time{}
}
