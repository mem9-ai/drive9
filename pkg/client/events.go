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
	Path   string `json:"path,omitempty"`
	Op     string `json:"op,omitempty"`
	Actor  string `json:"actor,omitempty"`
}

// HeartbeatEvent is a server stream-current marker. Seq is the latest server
// event sequence flushed before this heartbeat.
type HeartbeatEvent struct {
	Seq uint64 `json:"seq"`
}

// EventHandler receives SSE events. Exactly one of the pointer arguments is
// non-nil per call.
type EventHandler func(change *ChangeEvent, reset *ResetEvent)

// EventLifecycle receives SSE stream lifecycle notifications.
type EventLifecycle struct {
	OnDisconnected func(error)
	OnCurrent      func(seq uint64)
}

const (
	sseInitialBackoff = 1 * time.Second
	sseMaxBackoff     = 30 * time.Second
)

// WatchEvents connects to the SSE endpoint and delivers events to handler.
// It automatically reconnects with exponential backoff on disconnection.
// The caller should cancel ctx to stop watching.
// actor is the per-mount identifier used for self-event filtering.
func (c *Client) WatchEvents(ctx context.Context, actor string, handler EventHandler) {
	c.WatchEventsWithLifecycle(ctx, actor, handler, EventLifecycle{})
}

// WatchEventsWithLifecycle is WatchEvents with optional lifecycle callbacks.
func (c *Client) WatchEventsWithLifecycle(ctx context.Context, actor string, handler EventHandler, lifecycle EventLifecycle) {
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
		}, func(seq uint64) {
			if seq > lastSeq {
				lastSeq = seq
			}
			if lifecycle.OnCurrent != nil {
				lifecycle.OnCurrent(seq)
			}
		})

		select {
		case <-ctx.Done():
			return
		default:
		}
		if lifecycle.OnDisconnected != nil {
			lifecycle.OnDisconnected(err)
		}

		// Always backoff on disconnect (err or clean EOF).
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if err != nil {
			backoff *= 2
			if backoff > sseMaxBackoff {
				backoff = sseMaxBackoff
			}
		} else {
			backoff = sseInitialBackoff
		}
	}
}

func (c *Client) streamEvents(ctx context.Context, since uint64, actor string, handler EventHandler, onCurrent func(seq uint64)) error {
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
	var dataLines []string

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// End of event.
			if len(dataLines) > 0 {
				parseAndDispatch(eventType, strings.Join(dataLines, "\n"), handler, onCurrent)
			}
			eventType = ""
			dataLines = nil
			continue
		}

		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			data := strings.TrimPrefix(line, "data:")
			data = strings.TrimPrefix(data, " ")
			dataLines = append(dataLines, data)
		}
	}
	return scanner.Err()
}

func parseAndDispatch(eventType, data string, handler EventHandler, onCurrent func(seq uint64)) {
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
		var ev HeartbeatEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return
		}
		if onCurrent != nil {
			onCurrent(ev.Seq)
		}
	}
}
