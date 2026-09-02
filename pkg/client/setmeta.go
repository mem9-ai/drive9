package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// SetMetadataOptions selects which file metadata fields to update. A nil
// Tags map leaves the tag set unchanged; a non-nil map (including an empty
// one) replaces the user-owned tag set. A nil Description leaves the
// description unchanged; a non-nil pointer sets it (empty string clears).
type SetMetadataOptions struct {
	Tags        map[string]string
	Description *string
}

// setMetadataRequest is the wire body for POST /v1/fs/{path}?setmeta=1.
// Pointer fields distinguish "absent" (leave unchanged) from "present"
// (replace/clear).
type setMetadataRequest struct {
	Tags        *map[string]string `json:"tags,omitempty"`
	Description *string            `json:"description,omitempty"`
}

// SetMetadata updates tags and/or description of an existing file without
// rewriting its content.
func (c *Client) SetMetadata(path string, opts SetMetadataOptions) error {
	return c.SetMetadataCtx(context.Background(), path, opts)
}

// SetMetadataCtx updates tags and/or description of an existing file with
// context support.
func (c *Client) SetMetadataCtx(ctx context.Context, path string, opts SetMetadataOptions) error {
	if opts.Tags == nil && opts.Description == nil {
		return fmt.Errorf("setmeta: nothing to update")
	}
	if err := validateTags(opts.Tags); err != nil {
		return err
	}
	reqBody := setMetadataRequest{Description: opts.Description}
	if opts.Tags != nil {
		reqBody.Tags = &opts.Tags
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path)+"?setmeta=1", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}
