package fuse

import (
	"bytes"
	"context"

	"github.com/mem9-ai/dat9/pkg/client"
)

// uploadBufferedRemoteFile keeps FUSE background uploads aligned with the
// foreground flush path: small files can still use direct PUT, while larger
// files must go through the multipart client flow so the server receives the
// required part metadata/checksum semantics.
func uploadBufferedRemoteFile(ctx context.Context, c *client.Client, remotePath string, data []byte, expectedRevision int64) error {
	return c.WriteStreamConditional(ctx, remotePath, bytes.NewReader(data), int64(len(data)), nil, expectedRevision)
}
