package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/gitwsindex"
)

// Bounds for the user-writable remote existence index. Automatic mount/DELETE
// paths must not io.ReadAll an unbounded document (default file cap is 10 GiB).
const (
	MaxGitWorkspaceIndexBytes   = gitwsindex.MaxBytes
	MaxGitWorkspaceIndexEntries = gitwsindex.MaxEntries
)

// ErrGitWorkspaceIndexTooLarge is returned when the remote index document
// exceeds MaxGitWorkspaceIndexBytes or MaxGitWorkspaceIndexEntries.
var ErrGitWorkspaceIndexTooLarge = errors.New("git workspace index exceeds size or entry limit")

// Tenant-absolute path for the cross-sandbox git workspace existence index.
// Aligns with pack metadata under /.drive9/packs.
const GitWorkspaceIndexPath = gitwsindex.Path

// GitWorkspaceIndex is the remote FS document that signals whether any
// drive9 git --fast workspaces exist. FUSE uses it only as an arming signal;
// runtime content always comes from ListGitWorkspaces.
type GitWorkspaceIndex = gitwsindex.Index

// GitWorkspaceIndexEntry is a minimal existence record (tenant-absolute root_path).
type GitWorkspaceIndexEntry = gitwsindex.Entry

// GitWorkspaceIndexHasEntries reports whether the index lists any workspaces.
func GitWorkspaceIndexHasEntries(idx *GitWorkspaceIndex) bool {
	return idx != nil && len(idx.Workspaces) > 0
}

// ReadGitWorkspaceIndex loads the remote index. Returns (nil, nil) when the
// file does not exist (404). Other errors are returned as-is.
func (c *Client) ReadGitWorkspaceIndex(ctx context.Context) (*GitWorkspaceIndex, int64, error) {
	if c == nil {
		return nil, 0, fmt.Errorf("client is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stat, err := c.StatCtx(ctx, GitWorkspaceIndexPath)
	if IsNotFound(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	rev := int64(0)
	if stat != nil {
		rev = stat.Revision
		if stat.Size > MaxGitWorkspaceIndexBytes {
			return nil, rev, fmt.Errorf("%w: stat size %d", ErrGitWorkspaceIndexTooLarge, stat.Size)
		}
	}
	rc, err := c.ReadStream(ctx, GitWorkspaceIndexPath)
	if IsNotFound(err) {
		return nil, rev, nil
	}
	if err != nil {
		return nil, rev, err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, MaxGitWorkspaceIndexBytes+1))
	if err != nil {
		return nil, rev, fmt.Errorf("read git workspace index: %w", err)
	}
	idx, err := parseGitWorkspaceIndex(data)
	if err != nil {
		return nil, rev, err
	}
	return idx, rev, nil
}

func parseGitWorkspaceIndex(data []byte) (*GitWorkspaceIndex, error) {
	idx, err := gitwsindex.Parse(data)
	if err != nil {
		if errors.Is(err, gitwsindex.ErrUnreadable) {
			return nil, fmt.Errorf("%w: %v", ErrGitWorkspaceIndexTooLarge, err)
		}
		return nil, err
	}
	return idx, nil
}

// IsUnreadableGitWorkspaceIndex reports a malformed or oversized index document.
func IsUnreadableGitWorkspaceIndex(err error) bool {
	return errors.Is(err, ErrGitWorkspaceIndexTooLarge) || errors.Is(err, gitwsindex.ErrUnreadable)
}

// StatGitWorkspaceIndex reports whether the index exists and its revision/mtime.
// notFound is true on 404. Network/other errors return err with notFound=false.
func (c *Client) StatGitWorkspaceIndex(ctx context.Context) (exists bool, revision int64, mtime time.Time, err error) {
	if c == nil {
		return false, 0, time.Time{}, fmt.Errorf("client is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stat, err := c.StatCtx(ctx, GitWorkspaceIndexPath)
	if IsNotFound(err) {
		return false, 0, time.Time{}, nil
	}
	if err != nil {
		return false, 0, time.Time{}, err
	}
	if stat == nil {
		return false, 0, time.Time{}, nil
	}
	return true, stat.Revision, stat.Mtime, nil
}

// UpsertGitWorkspaceIndexEntry adds or replaces an entry by workspace_id (and
// by root_path collision). Uses revision CAS with bounded retries.
func (c *Client) UpsertGitWorkspaceIndexEntry(ctx context.Context, entry GitWorkspaceIndexEntry) error {
	entry.WorkspaceID = strings.TrimSpace(entry.WorkspaceID)
	entry.RootPath = strings.TrimSpace(entry.RootPath)
	if entry.WorkspaceID == "" || entry.RootPath == "" {
		return fmt.Errorf("git workspace index entry requires workspace_id and root_path")
	}
	return c.mutateGitWorkspaceIndex(ctx, func(idx *GitWorkspaceIndex) {
		out := make([]GitWorkspaceIndexEntry, 0, len(idx.Workspaces)+1)
		for _, e := range idx.Workspaces {
			if e.WorkspaceID == entry.WorkspaceID || e.RootPath == entry.RootPath {
				continue
			}
			out = append(out, e)
		}
		out = append(out, entry)
		idx.Workspaces = out
	})
}

// RemoveGitWorkspaceIndexEntry removes an entry by workspace_id. If the index
// becomes empty, the remote file is deleted.
func (c *Client) RemoveGitWorkspaceIndexEntry(ctx context.Context, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil
	}
	return c.mutateGitWorkspaceIndex(ctx, func(idx *GitWorkspaceIndex) {
		out := make([]GitWorkspaceIndexEntry, 0, len(idx.Workspaces))
		for _, e := range idx.Workspaces {
			if e.WorkspaceID == workspaceID {
				continue
			}
			out = append(out, e)
		}
		idx.Workspaces = out
	})
}

func (c *Client) mutateGitWorkspaceIndex(ctx context.Context, mut func(*GitWorkspaceIndex)) error {
	if c == nil {
		return fmt.Errorf("client is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	const maxAttempts = 4
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		idx, rev, err := c.ReadGitWorkspaceIndex(ctx)
		if err != nil {
			return err
		}
		if idx == nil {
			idx = &GitWorkspaceIndex{Version: 1}
			rev = 0 // create-only if missing
		}
		if idx.Version == 0 {
			idx.Version = 1
		}
		mut(idx)
		idx.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)

		if len(idx.Workspaces) == 0 {
			// Empty index: CAS-write empty document (safer than unconditional delete
			// which can race a concurrent upsert and drop a new entry).
			if rev == 0 {
				return nil // never existed
			}
			body, encErr := json.MarshalIndent(idx, "", "  ")
			if encErr != nil {
				return fmt.Errorf("encode empty git workspace index: %w", encErr)
			}
			if _, err := c.WriteCtxConditionalWithRevision(ctx, GitWorkspaceIndexPath, body, rev); err != nil {
				lastErr = err
				if attempt+1 < maxAttempts {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Duration(50*(attempt+1)) * time.Millisecond):
					}
					continue
				}
				return fmt.Errorf("write empty git workspace index: %w", err)
			}
			return nil
		}

		body, err := json.MarshalIndent(idx, "", "  ")
		if err != nil {
			return fmt.Errorf("encode git workspace index: %w", err)
		}
		// expectedRevision: 0 = must not exist (create); positive = exact CAS.
		// ReadGitWorkspaceIndex returns (nil, 0) on 404 → create with expected 0.
		// rev == 0 is create-only; if a concurrent create won, the write conflicts → retry.
		// Do not re-Stat here: that opens a lost-update window after concurrent create.
		expected := rev
		_, err = c.WriteCtxConditionalWithRevision(ctx, GitWorkspaceIndexPath, body, expected)
		if err == nil {
			return nil
		}
		lastErr = err
		// Retry on conflict / race.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(50*(attempt+1)) * time.Millisecond):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("upsert git workspace index: %w", lastErr)
	}
	return fmt.Errorf("upsert git workspace index: exhausted retries")
}
