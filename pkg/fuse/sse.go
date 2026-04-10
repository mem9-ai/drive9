package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"time"
)

// sseEvent represents a parsed SSE event.
type sseEvent struct {
	Type string // "file_changed", "reset", "heartbeat"
	Data json.RawMessage
}

// changeEvent mirrors the server-side ChangeEvent.
type changeEvent struct {
	Seq   uint64 `json:"seq"`
	Path  string `json:"path"`
	Op    string `json:"op"`
	Actor string `json:"actor"`
	Ts    int64  `json:"ts"`
}

// resetEvent represents a server-sent reset notification.
type resetEvent struct {
	Seq    uint64 `json:"seq"`
	Reason string `json:"reason"`
}

// heartbeatEvent represents a server-sent heartbeat.
type heartbeatEvent struct {
	Seq uint64 `json:"seq"`
}

const (
	sseReconnectMin = 1 * time.Second
	sseReconnectMax = 30 * time.Second
)

// SSEWatcher connects to the dat9 server SSE endpoint and invalidates
// local FUSE caches when remote changes are detected.
type SSEWatcher struct {
	fs       *Dat9FS
	baseURL  string
	apiKey   string
	actor    string // our own actor ID for self-filtering
	lastSeq  uint64
	cancel   context.CancelFunc
	doneCh   chan struct{}
}

// StartSSEWatcher starts a background goroutine that connects to the
// server's /v1/events SSE endpoint and invalidates local caches.
func StartSSEWatcher(fs *Dat9FS, baseURL, apiKey, actor string) *SSEWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &SSEWatcher{
		fs:      fs,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		actor:   actor,
		cancel:  cancel,
		doneCh:  make(chan struct{}),
	}
	go w.run(ctx)
	return w
}

// Stop signals the watcher to disconnect and waits for it to finish.
func (w *SSEWatcher) Stop() {
	w.cancel()
	<-w.doneCh
}

func (w *SSEWatcher) run(ctx context.Context) {
	defer close(w.doneCh)

	backoff := sseReconnectMin
	for {
		err := w.connect(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("dat9: SSE connection error: %v (reconnecting in %v)", err, backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Exponential backoff capped at sseReconnectMax.
		backoff = backoff * 2
		if backoff > sseReconnectMax {
			backoff = sseReconnectMax
		}
	}
}

func (w *SSEWatcher) connect(ctx context.Context) error {
	url := fmt.Sprintf("%s/v1/events?since=%d", w.baseURL, w.lastSeq)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if w.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+w.apiKey)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	// Reset backoff on successful connection.
	scanner := bufio.NewScanner(resp.Body)
	// SSE max line is typically small, but allow generous buffer.
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)

	var eventType string
	var dataLines []string

	for scanner.Scan() {
		if ctx.Err() != nil {
			return nil
		}
		line := scanner.Text()

		if line == "" {
			// Empty line = end of event.
			if eventType != "" && len(dataLines) > 0 {
				data := strings.Join(dataLines, "\n")
				w.handleEvent(sseEvent{
					Type: eventType,
					Data: json.RawMessage(data),
				})
			}
			eventType = ""
			dataLines = nil
			continue
		}

		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		}
		// Ignore "id:", "retry:", comments (":"), etc.
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	return fmt.Errorf("stream closed by server")
}

func (w *SSEWatcher) handleEvent(ev sseEvent) {
	switch ev.Type {
	case "file_changed":
		var ce changeEvent
		if err := json.Unmarshal(ev.Data, &ce); err != nil {
			log.Printf("dat9: SSE unmarshal file_changed: %v", err)
			return
		}
		if ce.Seq > w.lastSeq {
			w.lastSeq = ce.Seq
		}
		// Skip events from our own mount.
		if w.actor != "" && ce.Actor == w.actor {
			return
		}
		w.handleChange(ce)

	case "reset":
		var re resetEvent
		if err := json.Unmarshal(ev.Data, &re); err != nil {
			log.Printf("dat9: SSE unmarshal reset: %v", err)
			return
		}
		if re.Seq > w.lastSeq {
			w.lastSeq = re.Seq
		}
		w.handleReset()

	case "heartbeat":
		var he heartbeatEvent
		if err := json.Unmarshal(ev.Data, &he); err != nil {
			return
		}
		if he.Seq > w.lastSeq {
			w.lastSeq = he.Seq
		}
	}
}

func (w *SSEWatcher) handleChange(ce changeEvent) {
	p := ce.Path
	if p == "" {
		return
	}

	// Invalidate read cache for the changed file.
	w.fs.readCache.Invalidate(p)

	// Invalidate directory cache for the parent directory.
	w.fs.dirCache.Invalidate(parentDir(p))

	// Notify kernel about the change.
	if ino, ok := w.fs.inodes.GetInode(p); ok {
		w.fs.notifyInode(ino)
	}
	if parentIno, ok := w.fs.inodes.GetInode(parentDir(p)); ok {
		w.fs.notifyEntry(parentIno, path.Base(p))
	}
}

func (w *SSEWatcher) handleReset() {
	// 1. Clear all user-space caches.
	w.fs.readCache.InvalidateAll()
	w.fs.dirCache.InvalidateAll()

	// 2. Best-effort kernel cache invalidation for all known inodes.
	//    InodeToPath is kept intact (stale but resolvable).
	//    Kernel will re-Lookup on next access, which re-validates against server.
	w.fs.inodes.ForEach(func(ino uint64, entry InodeEntry) {
		// InodeNotify invalidates cached attrs and page data.
		w.fs.notifyInode(ino)

		// EntryNotify invalidates parent's dentry cache for this name.
		if entry.Path != "/" {
			parent := parentDir(entry.Path)
			if parentIno, ok := w.fs.inodes.GetInode(parent); ok {
				w.fs.notifyEntry(parentIno, path.Base(entry.Path))
			}
		}
	})
}
