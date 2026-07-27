package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const maxResponseBodyBytes = 1 << 20

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type provisionResponse struct {
	TenantID string `json:"tenant_id"`
	APIKey   string `json:"api_key"`
	Status   string `json:"status"`
}

type statusResponse struct {
	Status string `json:"status"`
}

type provisionResult struct {
	space spaceCredential
	err   error
}

type readinessResult struct {
	tenantID string
	err      error
}

func provisionSpace(
	ctx context.Context,
	cfg benchConfig,
	client httpDoer,
	now func() time.Time,
) (spaceCredential, error) {
	requestCtx, cancel := context.WithTimeout(ctx, cfg.ProvisionTimeout)
	defer cancel()
	payload := map[string]any{
		"public_key":               cfg.PublicKey,
		"private_key":              cfg.PrivateKey,
		"tidbcloud_spending_limit": cfg.SpendingLimit,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return spaceCredential{}, fmt.Errorf("encode provision request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		cfg.Server+"/v1/provision",
		bytes.NewReader(raw),
	)
	if err != nil {
		return spaceCredential{}, fmt.Errorf("create provision request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return spaceCredential{}, fmt.Errorf("provision request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readResponseBody(resp.Body)
	if err != nil {
		return spaceCredential{}, fmt.Errorf("read provision response: %w", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		return spaceCredential{}, responseError(
			"provision",
			resp.StatusCode,
			body,
			cfg.PublicKey,
			cfg.PrivateKey,
		)
	}
	var accepted provisionResponse
	if err := json.Unmarshal(body, &accepted); err != nil {
		return spaceCredential{}, fmt.Errorf("decode provision response: %w", err)
	}
	if accepted.TenantID == "" || accepted.APIKey == "" || accepted.Status == "" {
		return spaceCredential{}, fmt.Errorf(
			"provision response missing tenant_id, api_key, or status",
		)
	}
	return spaceCredential{
		TenantID:      accepted.TenantID,
		APIKey:        accepted.APIKey,
		CreatedAt:     now().UTC(),
		SpendingLimit: cfg.SpendingLimit,
	}, nil
}

func ensureSpaceCount(
	ctx context.Context,
	cfg benchConfig,
	state spaceState,
	client httpDoer,
	now func() time.Time,
	progress io.Writer,
) (spaceState, error) {
	shortfall := cfg.SpaceCount - len(state.Spaces)
	if shortfall <= 0 {
		return state, nil
	}
	if cfg.PublicKey == "" || cfg.PrivateKey == "" {
		return state, fmt.Errorf(
			"TiDB Cloud credentials are required to provision %d missing spaces",
			shortfall,
		)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(cfg.ProvisionConcurrency, shortfall)
	jobs := make(chan struct{})
	results := make(chan provisionResult, workers)
	limiter := newPacedLimiter(cfg.ProvisionRPS)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				if err := limiter.Wait(runCtx); err != nil {
					results <- provisionResult{err: err}
					continue
				}
				space, err := provisionSpace(runCtx, cfg, client, now)
				results <- provisionResult{space: space, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for range shortfall {
			select {
			case jobs <- struct{}{}:
			case <-runCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	var runErrors []error
	for result := range results {
		if result.err != nil {
			runErrors = append(runErrors, redactError(
				result.err,
				cfg.PublicKey,
				cfg.PrivateKey,
			))
			continue
		}
		state.Spaces = append(state.Spaces, result.space)
		if err := saveSpaceState(cfg.SpacesFile, state); err != nil {
			runErrors = append(runErrors, err)
			cancel()
			continue
		}
		_, _ = fmt.Fprintf(
			progress,
			"provisioned space %d/%d tenant=%s\n",
			len(state.Spaces),
			cfg.SpaceCount,
			result.space.TenantID,
		)
	}
	if len(runErrors) > 0 {
		return state, fmt.Errorf(
			"provisioned %d of %d requested spaces: %w",
			len(state.Spaces),
			cfg.SpaceCount,
			errors.Join(runErrors...),
		)
	}
	if len(state.Spaces) < cfg.SpaceCount {
		return state, fmt.Errorf(
			"provisioned %d of %d requested spaces",
			len(state.Spaces),
			cfg.SpaceCount,
		)
	}
	return state, nil
}

func waitForAllSpacesReady(
	ctx context.Context,
	cfg benchConfig,
	spaces []spaceCredential,
	client httpDoer,
	progress io.Writer,
) error {
	if len(spaces) == 0 {
		return fmt.Errorf("no spaces selected")
	}
	statusSlots := make(chan struct{}, min(cfg.ProvisionConcurrency, len(spaces)))
	results := make(chan readinessResult, len(spaces))

	var wg sync.WaitGroup
	for _, space := range spaces {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := waitForSpaceReady(ctx, cfg, space, client, statusSlots)
			results <- readinessResult{tenantID: space.TenantID, err: err}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	var runErrors []error
	ready := 0
	for result := range results {
		if result.err != nil {
			runErrors = append(runErrors, fmt.Errorf(
				"tenant %s: %w",
				result.tenantID,
				result.err,
			))
			continue
		}
		ready++
		_, _ = fmt.Fprintf(
			progress,
			"space ready %d/%d tenant=%s\n",
			ready,
			len(spaces),
			result.tenantID,
		)
	}
	if len(runErrors) > 0 || ready != len(spaces) {
		if ctx.Err() != nil {
			runErrors = append(runErrors, ctx.Err())
		}
		if len(runErrors) == 0 {
			return fmt.Errorf("%d of %d spaces are ready", ready, len(spaces))
		}
		return fmt.Errorf(
			"%d of %d spaces are ready: %w",
			ready,
			len(spaces),
			errors.Join(runErrors...),
		)
	}
	return nil
}

func waitForSpaceReady(
	parent context.Context,
	cfg benchConfig,
	space spaceCredential,
	client httpDoer,
	statusSlots chan struct{},
) error {
	ctx, cancel := context.WithTimeout(parent, cfg.ProvisionTimeout)
	defer cancel()
	for {
		select {
		case statusSlots <- struct{}{}:
		case <-ctx.Done():
			return fmt.Errorf("wait for status request slot: %w", ctx.Err())
		}
		status, retryable, err := fetchSpaceStatus(ctx, cfg.Server, space.APIKey, client)
		<-statusSlots
		if err == nil {
			switch status {
			case "active":
				return nil
			case "failed", "suspended", "deleting", "deleted":
				return fmt.Errorf("entered terminal status %q", status)
			}
		} else if !retryable {
			return redactError(err, space.APIKey)
		}

		timer := time.NewTimer(cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for active status: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func fetchSpaceStatus(
	ctx context.Context,
	server, apiKey string,
	client httpDoer,
) (status string, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server+"/v1/status", nil)
	if err != nil {
		return "", false, fmt.Errorf("create status request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return "", true, fmt.Errorf("status request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readResponseBody(resp.Body)
	if err != nil {
		return "", true, fmt.Errorf("read status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		statusErr := responseError("status", resp.StatusCode, body, apiKey)
		return "", retryableHTTPStatus(resp.StatusCode), statusErr
	}
	var decoded statusResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", true, fmt.Errorf("decode status response: %w", err)
	}
	if decoded.Status == "" {
		return "", true, fmt.Errorf("status response missing status")
	}
	return decoded.Status, false, nil
}

type httpStatusError struct {
	operation string
	code      int
	message   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s returned HTTP %d: %s", e.operation, e.code, e.message)
}

func responseError(operation string, code int, body []byte, secrets ...string) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 1024 {
		message = message[:1024] + "..."
	}
	if message == "" {
		message = http.StatusText(code)
	}
	message = redactSecrets(message, secrets...)
	return &httpStatusError{operation: operation, code: code, message: message}
}

func retryableHTTPStatus(code int) bool {
	return code == http.StatusRequestTimeout ||
		code == http.StatusTooManyRequests ||
		code >= http.StatusInternalServerError
}

func readResponseBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, maxResponseBodyBytes))
}

func redactError(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	return errors.New(redactSecrets(err.Error(), secrets...))
}

func redactSecrets(message string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
