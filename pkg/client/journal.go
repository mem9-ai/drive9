package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mem9-ai/dat9/pkg/journal"
)

func (c *Client) CreateJournal(ctx context.Context, req journal.CreateRequest) (*journal.Journal, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/journals", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out journal.Journal
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode journal create: %w", err)
	}
	return &out, nil
}

func (c *Client) AppendJournalEntries(ctx context.Context, journalID, appendID string, entries []journal.EntryInput) (*journal.AppendResponse, error) {
	body, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	u := c.baseURL + "/v1/journals/" + url.PathEscape(journalID) + "/entries"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", appendID)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out journal.AppendResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode journal append: %w", err)
	}
	return &out, nil
}

func (c *Client) ReadJournalEntries(ctx context.Context, journalID string, afterSeq int64, limit int) ([]journal.Entry, error) {
	values := url.Values{}
	if afterSeq > 0 {
		values.Set("after_seq", strconv.FormatInt(afterSeq, 10))
	}
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	u := c.baseURL + "/v1/journals/" + url.PathEscape(journalID) + "/entries"
	if encoded := values.Encode(); encoded != "" {
		u += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out []journal.Entry
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry journal.Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("decode journal entry: %w", err)
		}
		out = append(out, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) SearchJournal(ctx context.Context, req journal.SearchRequest) ([]journal.SearchMatch, error) {
	values := url.Values{}
	if req.Type != "" {
		values.Set("type", req.Type)
	}
	if req.Status != "" {
		values.Set("status", req.Status)
	}
	if req.Kind != "" {
		values.Set("kind", req.Kind)
	}
	if req.ActorType != "" {
		values.Set("actor", req.ActorType+":"+req.ActorID)
	}
	for _, subject := range req.Subjects {
		values.Add("subject", subject)
	}
	for _, label := range journal.NormalizeLabels(req.Labels) {
		values.Add("meta", label.Key+"="+label.Value)
	}
	if req.SinceRaw != "" {
		values.Set("since", req.SinceRaw)
	} else if req.Since != nil {
		values.Set("since", journal.FormatTime(*req.Since))
	}
	if req.UntilRaw != "" {
		values.Set("until", req.UntilRaw)
	} else if req.Until != nil {
		values.Set("until", journal.FormatTime(*req.Until))
	}
	if req.Limit > 0 {
		values.Set("limit", strconv.Itoa(req.Limit))
	}
	if req.Cursor != "" {
		values.Set("cursor", req.Cursor)
	}
	if req.Entries {
		values.Set("include", "entry")
	}
	u := c.baseURL + "/v1/journal-entries"
	if encoded := values.Encode(); encoded != "" {
		u += "?" + encoded
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out []journal.SearchMatch
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var match journal.SearchMatch
		if err := json.Unmarshal([]byte(line), &match); err != nil {
			return nil, fmt.Errorf("decode journal match: %w", err)
		}
		out = append(out, match)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) VerifyJournal(ctx context.Context, journalID string) (*journal.VerifyResult, error) {
	u := c.baseURL + "/v1/journals/" + url.PathEscape(journalID) + "/verify"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out journal.VerifyResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode journal verify: %w", err)
	}
	return &out, nil
}
