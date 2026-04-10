package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ChangeEvent represents a server-sent filesystem change event.
type ChangeEvent struct {
	Seq   uint64 `json:"seq"`
	Path  string `json:"path"`
	Op    string `json:"op"`
	Actor string `json:"actor,omitempty"`
	Ts    int64  `json:"ts"`
}

// ResetEvent is delivered when the server cannot replay events and the client
// must invalidate all caches.
type ResetEvent struct {
	Seq    uint64 `json:"seq"`
	Reason string `json:"reason"`
}

// EventHandler receives SSE events. Exactly one of the pointer arguments is
// non-nil per call.
type EventHandler func(change *ChangeEvent, reset *ResetEvent)

const (
	sseInitialBackoff = 1 * time.Second
	sseMaxBackoff     = 30 * time.Second
)

// WatchEvents connects to the SSE endpoint and delivers events to handler.
// It automatically reconnects with exponential backoff on disconnection.
// The caller should cancel ctx to stop watching.
// actor is the per-mount identifier used for self-event filtering.
func (c *Client) WatchEvents(ctx context.Context, actor string, handler EventHandler) {
	var lastSeq uint64
	backoff := sseInitialBackoff

	for {
		err := c.streamEvents(ctx, lastSeq, actor, func(change *ChangeEvent, reset *ResetEvent) {
			if change != nil && change.Seq > lastSeq {
				lastSeq = change.Seq
			}
			if reset != nil && reset.Seq > lastSeq {
				lastSeq = reset.Seq
			}
			handler(change, reset)
		})

		select {
		case <-ctx.Done():
			return
		default:
		}

		if err != nil {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > sseMaxBackoff {
				backoff = sseMaxBackoff
			}
		} else {
			backoff = sseInitialBackoff
		}
	}
}

func (c *Client) streamEvents(ctx context.Context, since uint64, actor string, handler EventHandler) error {
	url := fmt.Sprintf("%s/v1/events?since=%d", c.baseURL, since)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if actor != "" {
		req.Header.Set("X-Dat9-Actor", actor)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SSE status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	var eventType string
	var dataLine string

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// End of event.
			if dataLine != "" {
				parseAndDispatch(eventType, dataLine, handler)
			}
			eventType = ""
			dataLine = ""
			continue
		}

		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			dataLine = strings.TrimPrefix(line, "data: ")
		}
	}
	return scanner.Err()
}

func parseAndDispatch(eventType, data string, handler EventHandler) {
	switch eventType {
	case "file_changed":
		var ev ChangeEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return
		}
		handler(&ev, nil)
	case "reset":
		var ev ResetEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return
		}
		handler(nil, &ev)
	case "heartbeat":
		// Heartbeats carry seq for liveness but don't need dispatching.
		var ev ResetEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return
		}
		// Update lastSeq via reset path (same struct shape).
		handler(nil, &ev)
	}
}
