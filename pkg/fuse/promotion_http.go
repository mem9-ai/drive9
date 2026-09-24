package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/mem9-ai/drive9/internal/drivehttp"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

// preparePromotionRequest performs every deterministic client-side admission
// check before the caller persists a prepared journal. Once that journal
// exists, failures must describe a request that may have reached the server.
func (fs *Dat9FS) preparePromotionRequest(ctx context.Context, request promotion.PublishRequest) ([]byte, error) {
	threshold := fs.client.SmallFileThreshold(ctx)
	if threshold <= 0 {
		threshold = client.DefaultSmallFileThreshold
	}
	if err := promotion.Validate(request, threshold); err != nil {
		return nil, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > promotion.MaxRequestBodyBytes {
		return nil, promotion.ErrLimitExceeded
	}
	return body, nil
}

// The promotion wire protocol is an implementation detail of the FUSE mount,
// not part of the public Go SDK. Keep its three calls here so adding the
// preview does not expand Client's public contract.
func (fs *Dat9FS) publishPreparedPromotionRequest(ctx context.Context, body []byte) (*promotion.Result, error) {
	return fs.doPromotionResult(ctx, http.MethodPost, "/v1/fs:promotion", bytes.NewReader(body))
}

func (fs *Dat9FS) getPromotionResult(ctx context.Context, operationID, target string) (*promotion.Result, bool, error) {
	query := url.Values{"operation_id": []string{operationID}, "target": []string{target}}
	callCtx, cancel := context.WithTimeout(ctx, promotionRequestTimeout)
	defer cancel()
	resp, err := fs.doPromotionHTTP(callCtx, http.MethodGet, "/v1/fs:promotion?"+query.Encode(), nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, false, readPromotionHTTPError(resp)
	}
	var result promotion.Result
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(&result); err != nil {
		return nil, false, fmt.Errorf("decode promotion result: %w", err)
	}
	return &result, resp.StatusCode == http.StatusAccepted, nil
}

func (fs *Dat9FS) acknowledgePromotionResult(ctx context.Context, result promotion.Result) error {
	query := url.Values{
		"operation_id":    []string{result.OperationID},
		"target":          []string{result.Target},
		"manifest_sha256": []string{result.ManifestSHA256},
	}
	callCtx, cancel := context.WithTimeout(ctx, promotionRequestTimeout)
	defer cancel()
	resp, err := fs.doPromotionHTTP(callCtx, http.MethodDelete, "/v1/fs:promotion?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return readPromotionHTTPError(resp)
	}
	return nil
}

func (fs *Dat9FS) doPromotionResult(ctx context.Context, method, endpoint string, body io.Reader) (*promotion.Result, error) {
	callCtx, cancel := context.WithTimeout(ctx, promotionRequestTimeout)
	defer cancel()
	resp, err := fs.doPromotionHTTP(callCtx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, readPromotionHTTPError(resp)
	}
	var result promotion.Result
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode promotion result: %w", err)
	}
	return &result, nil
}

func (fs *Dat9FS) doPromotionHTTP(ctx context.Context, method, endpoint string, body io.Reader) (*http.Response, error) {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodDelete:
	default:
		return nil, fmt.Errorf("unsupported promotion HTTP method %q", method)
	}
	req, err := http.NewRequestWithContext(ctx, method, fs.promotionBaseURL+endpoint, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	drivehttp.ApplyCredentials(req, fs.promotionCredential, fs.promotionActor)
	return fs.promotionHTTPClient.Do(req)
}

func readPromotionHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	message, code := drivehttp.DecodeErrorBody(resp.StatusCode, body)
	return &client.StatusError{StatusCode: resp.StatusCode, Message: message, Code: code}
}
