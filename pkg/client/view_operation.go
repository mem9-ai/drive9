package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

type viewOperationKey struct{}

// WithViewOperationID supplies the stable identity for a single fixed-view
// filesystem mutation. Reuse it only with the identical request to reconcile
// an unknown outcome. Do not reuse this context across different mutations.
// For seekable extent uploads this is the completion ID; initiation uses a
// separate derived ID. After an unknown completion, use CompleteFixedExtentUpload
// with the saved upload ID and parts, or query GetFixedViewOperation. Restarting
// the whole transfer is not a completion retry.
func WithViewOperationID(ctx context.Context, id string) (context.Context, error) {
	if err := viewprotocol.ValidateOperationID(id); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, viewOperationKey{}, id), nil
}

// ViewOperationError retains the identity needed to query GetFixedViewOperation.
// An error, including a timeout or 5xx, does not prove the mutation rolled back.
type ViewOperationError struct {
	LayerID, OperationID string
	Err                  error
}

func (e *ViewOperationError) Error() string {
	return fmt.Sprintf("view %s operation %s: %v", e.LayerID, e.OperationID, e.Err)
}
func (e *ViewOperationError) Unwrap() error { return e.Err }

// The request is owned by newFSRequest. Main and read-only APIs keep their
// existing wire contract; no global request/session identity is introduced.
func (c *Client) identifyViewMutation(req *http.Request) {
	if c.view.LayerID == "" || req.Method == http.MethodGet || req.Method == http.MethodHead {
		return
	}
	id, _ := req.Context().Value(viewOperationKey{}).(string)
	if id == "" {
		id = "fs-" + rand.Text()
	}
	req.Header.Set(viewprotocol.OperationHeader, id)
}

func (c *Client) viewMutationOutcome(req *http.Request, resp *http.Response, err error) (*http.Response, error) {
	id := req.Header.Get(viewprotocol.OperationHeader)
	if c.view.LayerID == "" || id == "" {
		return resp, err
	}
	if err == nil && resp.StatusCode < 300 {
		return resp, nil
	}
	if err == nil {
		// Error responses are bounded too; retain the stable ID even if the server
		// could not return a complete body describing its outcome.
		original := resp.Body
		resp.Body = io.NopCloser(io.LimitReader(original, 64<<10))
		err = readError(resp)
		_ = original.Close()
	} else if resp != nil {
		_ = resp.Body.Close()
	}
	return nil, &ViewOperationError{LayerID: c.view.LayerID, OperationID: id, Err: err}
}

// Decode a bounded complete mutation response before exposing its result. JSON
// decoding can fail after successful response headers; that is still an unknown
// outcome under the original operation identity. Main retains legacy decoding.
func (c *Client) decodeViewMutation(req *http.Request, resp *http.Response, out any) error {
	if c.view.LayerID == "" || req.Header.Get(viewprotocol.OperationHeader) == "" {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return c.viewMutationError(req, decodeFixedResponse(resp.Body, out))
}

// A syntactically complete JSON prefix is not a complete response. In
// particular, a launch grant is exposed only after bounded EOF and decoding.
func decodeFixedResponse(body io.Reader, out any) error {
	const maxResponse = 4 << 20
	raw, err := io.ReadAll(io.LimitReader(body, maxResponse+1))
	if err == nil && len(raw) > maxResponse {
		err = fmt.Errorf("fixed view response exceeds %d bytes", maxResponse)
	}
	if err == nil {
		err = json.Unmarshal(raw, out)
	}
	return err
}
func (c *Client) viewMutationError(req *http.Request, err error) error {
	if err == nil || c.view.LayerID == "" || req.Header.Get(viewprotocol.OperationHeader) == "" {
		return err
	}
	var existing *ViewOperationError
	if errors.As(err, &existing) {
		return err
	}
	return &ViewOperationError{LayerID: c.view.LayerID, OperationID: req.Header.Get(viewprotocol.OperationHeader), Err: err}
}
