package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

const promotionImportsPath = "/v1/promotions/imports"

// PlanPromotionImport validates and signs a side-effect-free import plan.
func (c *Client) PlanPromotionImport(ctx context.Context, req promotion.PlanImportRequest) (*promotion.ImportPlan, error) {
	req.TenantID = ""
	return doPromotionJSON[promotion.ImportPlan](c, ctx, http.MethodPost, promotionImportsPath+":plan", req)
}

// AllocatePromotionImport allocates one durable migration identity.
func (c *Client) AllocatePromotionImport(ctx context.Context, req promotion.AllocateImportRequest) (*promotion.Allocation, error) {
	return doPromotionJSON[promotion.Allocation](c, ctx, http.MethodPost, promotionImportsPath+":allocate", req)
}

// CreatePromotionImport binds an allocation to one immutable manifest.
func (c *Client) CreatePromotionImport(ctx context.Context, req promotion.CreateImportRequest) (*promotion.ImportStatus, error) {
	return doPromotionJSON[promotion.ImportStatus](c, ctx, http.MethodPost, promotionImportsPath, req)
}

// PutPromotionInlineContent stores one bounded file body in hidden staging.
func (c *Client) PutPromotionInlineContent(ctx context.Context, migrationID string, meta promotion.InlineContentMetadata, body io.Reader) (*promotion.InlineContent, error) {
	endpoint, err := promotionActionPath(migrationID, "put-inline")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("create promotion inline-content request: %w", err)
	}
	req.ContentLength = int64(meta.SizeBytes)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Drive9-Promotion-Owner-Epoch", strconv.FormatUint(meta.OwnerEpoch, 10))
	req.Header.Set("X-Drive9-Promotion-Owner-Token", meta.OwnerToken)
	req.Header.Set("X-Drive9-Promotion-Relative-Path", meta.RelativePath)
	req.Header.Set("X-Drive9-Promotion-Entry-Hash", meta.EntryHash)
	req.Header.Set("X-Drive9-Promotion-Size", strconv.FormatUint(meta.SizeBytes, 10))
	req.Header.Set("X-Drive9-Promotion-Checksum-SHA256", meta.ChecksumSHA256)
	req.Header.Set("X-Drive9-Promotion-Idempotency-Key", meta.IdempotencyKey)
	return doPromotionRequest[promotion.InlineContent](c, req)
}

// VerifyPromotionImport freezes a completely staged manifest.
func (c *Client) VerifyPromotionImport(ctx context.Context, migrationID string, req promotion.VerifyImportRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "verify", req)
}

// CommitPromotionImport synchronously drives an accepted commit to a terminal result.
func (c *Client) CommitPromotionImport(ctx context.Context, migrationID string, req promotion.OwnerRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "commit", req)
}

// AbortPromotionImport synchronously drives cleanup to a terminal result.
func (c *Client) AbortPromotionImport(ctx context.Context, migrationID string, req promotion.OwnerRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "abort", req)
}

// RenewPromotionImport renews a current owner lease without changing its epoch.
func (c *Client) RenewPromotionImport(ctx context.Context, migrationID string, req promotion.OwnerRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "renew", req)
}

// GetPromotionImport performs an authenticated non-disclosing status lookup.
func (c *Client) GetPromotionImport(ctx context.Context, migrationID string, req promotion.GetImportRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "get", req)
}

// TakeOverPromotionImport transfers an expired owner lease.
func (c *Client) TakeOverPromotionImport(ctx context.Context, migrationID string, req promotion.TakeOverImportRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "take-over", req)
}

// RetirePromotionImport serializes an undispatched allocation with CreateImport.
func (c *Client) RetirePromotionImport(ctx context.Context, migrationID string, req promotion.RetireImportRequest) (*promotion.RetireImportResult, error) {
	return doPromotionAction[promotion.RetireImportResult](c, ctx, migrationID, "retire", req)
}

// AcknowledgePromotionImportResult records a durably observed terminal result.
func (c *Client) AcknowledgePromotionImportResult(ctx context.Context, migrationID string, req promotion.AcknowledgeImportResultRequest) (*promotion.ImportStatus, error) {
	return doPromotionAction[promotion.ImportStatus](c, ctx, migrationID, "ack-result", req)
}

func doPromotionAction[T any](c *Client, ctx context.Context, migrationID, action string, body any) (*T, error) {
	endpoint, err := promotionActionPath(migrationID, action)
	if err != nil {
		return nil, err
	}
	return doPromotionJSON[T](c, ctx, http.MethodPost, endpoint, body)
}

func promotionActionPath(migrationID, action string) (string, error) {
	if _, err := promotion.DecodeMigrationID(migrationID); err != nil {
		return "", fmt.Errorf("invalid promotion migration id: %w", err)
	}
	return promotionImportsPath + "/" + url.PathEscape(migrationID) + ":" + action, nil
}

func doPromotionJSON[T any](c *Client, ctx context.Context, method, endpoint string, body any) (*T, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal promotion request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("create promotion request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return doPromotionRequest[T](c, req)
}

func doPromotionRequest[T any](c *Client, req *http.Request) (*T, error) {
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusMultipleChoices {
		return nil, readError(resp)
	}
	var result T
	dec := json.NewDecoder(io.LimitReader(resp.Body, 16<<20))
	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode promotion response: %w", err)
	}
	return &result, nil
}
