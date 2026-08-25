package objectfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
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

func startSessionRefresh(parent context.Context, f fs.Fs, mint MintSession, expiry time.Time) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	go runSessionRefresh(ctx, f, mint, expiry)
	return cancel
}

func runSessionRefresh(ctx context.Context, f fs.Fs, mint MintSession, expiry time.Time) {
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
		applyCtx, applyCancel := context.WithTimeout(ctx, sessionRefreshMintWait)
		err = applySession(applyCtx, f, sess)
		applyCancel()
		if err != nil {
			logger.Warn(ctx, "object mount failed to apply refreshed session; will retry", zap.Error(err), zap.Duration("retry_in", failWait))
			expiry = time.Now().Add(failWait)
			continue
		}
		expiry = nextExp
		logger.Info(ctx, "object mount session refreshed", zap.Time("expiration", effectiveSessionExpiry(expiry, time.Now())))
	}
}

// applySession updates credentials on the existing rclone Fs. It never
// constructs a replacement Fs: rclone S3 Copy/Move only run server-side
// when src.Fs().Name() == dst.Name(), and on-the-fly remotes put a hash of
// the keys in Name().
func applySession(ctx context.Context, f fs.Fs, sess SessionCredentials) error {
	if sess.AccessKeyID == "" && sess.AccessToken == "" && sess.SASURL == "" {
		return fmt.Errorf("objectfs: refreshed session has no credentials")
	}
	if sess.AccessKeyID != "" {
		if err := applyS3Session(ctx, f, sess); err != nil {
			return err
		}
	}
	if sess.AccessToken != "" {
		if err := setGCSAccessTokenOn(f, sess.AccessToken); err != nil {
			return err
		}
	}
	if sess.SASURL != "" {
		installAzureSASFilter(sess.SASURL)
	}
	return nil
}

func applyS3Session(ctx context.Context, f fs.Fs, sess SessionCredentials) error {
	cmd, ok := f.(fs.Commander)
	if !ok {
		return fmt.Errorf("objectfs: rclone %s does not support in-place credential refresh", f.Name())
	}
	_, err := cmd.Command(ctx, "set", nil, map[string]string{
		"env_auth":          "false",
		"access_key_id":     sess.AccessKeyID,
		"secret_access_key": sess.SecretAccessKey,
		"session_token":     sess.SessionToken,
	})
	if err != nil {
		if errors.Is(err, fs.ErrorCommandNotFound) {
			return fmt.Errorf("objectfs: rclone %s does not support in-place credential refresh", f.Name())
		}
		return fmt.Errorf("rclone backend set: %w", err)
	}
	return nil
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
