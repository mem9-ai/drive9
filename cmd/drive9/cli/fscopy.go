package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// CopyOpts configures StreamCopy.
type CopyOpts struct {
	Overwrite   bool
	Progress    func(n int64)
	Tags        map[string]string
	Description string
}

// StreamCopy pipes src → dst without creating a whole-file tempfile.
// On any error or cancel it Aborts the write handle.
func StreamCopy(ctx context.Context, srcB, dstB Backend, src, dst Location, srcInfo *FileInfo, opts CopyOpts) (err error) {
	size := int64(-1)
	if srcInfo != nil {
		size = srcInfo.Size
	}
	rc, err := srcB.OpenRead(ctx, src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src.Raw, err)
	}
	defer func() { _ = rc.Close() }()

	wh, err := dstB.OpenWrite(ctx, dst, WriteOpts{
		Size:        size,
		Overwrite:   opts.Overwrite,
		Tags:        opts.Tags,
		Description: opts.Description,
	})
	if err != nil {
		return fmt.Errorf("create %s: %w", dst.Raw, err)
	}
	aborted := false
	defer func() {
		if err != nil && !aborted {
			_ = wh.Abort(context.WithoutCancel(ctx))
		}
	}()

	buf := make([]byte, 32<<10)
	var copied int64
	for {
		if err = ctx.Err(); err != nil {
			aborted = true
			_ = wh.Abort(context.WithoutCancel(ctx))
			return err
		}
		n, readErr := rc.Read(buf)
		if n > 0 {
			wn, werr := wh.Write(buf[:n])
			if werr == nil && wn < n {
				werr = io.ErrShortWrite
			}
			if werr != nil {
				err = fmt.Errorf("write %s: %w", dst.Raw, werr)
				return err
			}
			copied += int64(wn)
			if opts.Progress != nil {
				opts.Progress(copied)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			err = fmt.Errorf("read %s: %w", src.Raw, readErr)
			return err
		}
	}
	if size >= 0 && copied != size {
		err = fmt.Errorf("read %s: short copy: got %d bytes, want %d", src.Raw, copied, size)
		return err
	}
	if err = wh.Close(); err != nil {
		return fmt.Errorf("close %s: %w", dst.Raw, err)
	}
	return nil
}

// Copy routes same-backend CopyObject/Client.Copy when possible, otherwise
// StreamCopy.
func Copy(ctx context.Context, srcB, dstB Backend, src, dst Location, srcInfo *FileInfo, opts CopyOpts) error {
	if len(opts.Tags) == 0 && opts.Description == "" &&
		SameIdentity(srcB, dstB) && src.Bucket == dst.Bucket {
		if sc, ok := dstB.(SameCopier); ok {
			if !opts.Overwrite {
				st, statErr := dstB.Stat(ctx, dst)
				if statErr == nil && st != nil && !st.IsDir {
					return fmt.Errorf("object exists (use --force to overwrite): %s", dst.Raw)
				}
				if statErr != nil && !errors.Is(statErr, ErrNotFound) {
					return fmt.Errorf("stat %s: %w", dst.Raw, statErr)
				}
			}
			return sc.Copy(ctx, src, dst)
		}
	}
	return StreamCopy(ctx, srcB, dstB, src, dst, srcInfo, opts)
}

// Walk pages List until exhausted and calls fn for every entry.
func Walk(ctx context.Context, b Backend, loc Location, fn func(FileInfo) error) error {
	opts := ListOpts{Recursive: true}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := b.List(ctx, loc, opts)
		if err != nil {
			return err
		}
		for _, e := range page.Entries {
			if err := fn(e); err != nil {
				return err
			}
		}
		if page.NextCursor == "" {
			return nil
		}
		opts.Cursor = page.NextCursor
	}
}
