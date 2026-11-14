package core

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/client"
)

// createEmptyRemote keeps a nondefault initial mode in the same commit as
// creation. A separate chmod advances the inode revision and invalidates the
// create receipt before the first buffered write can use it.
func (c *Core) createEmptyRemote(ctx context.Context, path string, mode uint32) (int64, bool, error) {
	if mode == 0o644 || c.cl.ViewBinding().LayerID != "" || !c.cl.CachedBatchWriteModeSupported() {
		rev, err := c.cl.CreateFileCtx(ctx, path)
		return rev, false, err
	}
	results, err := c.cl.BatchWriteCtx(ctx, []client.BatchWriteItem{{
		Path: path, ExpectedRevision: 0, Data: []byte{}, Mode: mode & 0o777, HasMode: true,
	}})
	if err != nil {
		return 0, false, err
	}
	result := results[0] // BatchWriteCtx checks the result count and path.
	if !result.OK() {
		return 0, false, &client.StatusError{StatusCode: result.Status, Message: result.Error}
	}
	if result.Revision <= 0 {
		return 0, false, fmt.Errorf("create with mode omitted committed revision")
	}
	return result.Revision, true, nil
}
