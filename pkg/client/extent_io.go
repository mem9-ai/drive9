package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/mem9-ai/drive9/pkg/extent"
)

func (c *Client) ensureExtentRuntime() (*extent.Runtime, error) {
	c.extentMu.Lock()
	defer c.extentMu.Unlock()
	if c.extentRT != nil {
		return c.extentRT, nil
	}
	cred, err := c.GetDataCredential(context.Background())
	if err != nil {
		return nil, fmt.Errorf("data credential: %w", err)
	}
	store, err := extent.OpenStorage(cred, c)
	if err != nil {
		return nil, fmt.Errorf("extent storage: %w", err)
	}
	rt, err := extent.NewRuntime(extent.RuntimeConfig{
		Transport: extent.NewHTTPTransport(c),
		Storage:   store,
	})
	if err != nil {
		return nil, err
	}
	c.extentRT = rt
	return rt, nil
}

func extentHintFromResponse(resp *http.Response) (ino, length uint64, ok bool) {
	if resp == nil || resp.StatusCode != http.StatusUnprocessableEntity {
		return 0, 0, false
	}
	if ContentLayout(resp.Header.Get("X-Dat9-Content-Layout")) != ContentLayoutExtent {
		return 0, 0, false
	}
	ino, _ = strconv.ParseUint(resp.Header.Get("X-Dat9-Extent-Ino"), 10, 64)
	if ino == 0 {
		return 0, 0, false
	}
	length, _ = strconv.ParseUint(resp.Header.Get("X-Dat9-Extent-Length"), 10, 64)
	return ino, length, true
}

func (c *Client) openExtentStream(ino, length uint64, offset, limit int64) (io.ReadCloser, error) {
	rt, err := c.ensureExtentRuntime()
	if err != nil {
		return nil, err
	}
	off := uint64(0)
	if offset > 0 {
		off = uint64(offset)
	}
	lim := uint64(0)
	if limit > 0 {
		lim = uint64(limit)
	}
	return rt.OpenRead(ino, length, off, lim)
}
