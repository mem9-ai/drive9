import { requestUrl } from "obsidian";
import type { Drive9Settings } from "./types";

/** SSE event: a single file change. */
export interface ChangeEvent {
  seq: number;
  path: string;
  op: "write" | "delete" | "rename" | "mkdir" | "copy";
  actor: string;
  ts: number;
}

/** SSE event: client must re-sync everything. */
export interface ResetEvent {
  seq: number;
  reason: string;
}

export interface SSEWatcherCallbacks {
  onRemoteChange(path: string, op: string): void;
  onFullSync(): void;
}

/**
 * SSEWatcher connects to GET /v1/events?since={seq} for real-time remote
 * change detection. When SSE is unavailable (server doesn't support it yet,
 * network error, etc.), it degrades to polling mode automatically.
 *
 * Reconnect uses exponential backoff: 1s → 2s → 4s → ... → 60s max.
 */
export class SSEWatcher {
  private lastSeq = 0;
  private actorId: string;
  private abortController: AbortController | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private backoffMs = 1000;
  private stopped = false;
  private _usePolling = false;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private serverUrl: string,
    private apiKey: string,
    actorId: string,
    private callbacks: SSEWatcherCallbacks,
    private pollIntervalMs = 30000,
  ) {
    this.actorId = actorId;
  }

  get usePolling(): boolean {
    return this._usePolling;
  }

  /** Start watching. Attempts SSE first, falls back to polling. */
  start(): void {
    this.stopped = false;
    this.connectSSE();
  }

  /** Stop watching and clean up all timers/connections. */
  stop(): void {
    this.stopped = true;
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
  }

  updateConfig(serverUrl: string, apiKey: string): void {
    this.serverUrl = serverUrl;
    this.apiKey = apiKey;
  }

  private async connectSSE(): Promise<void> {
    if (this.stopped) return;

    const url = `${this.serverUrl}/v1/events?since=${this.lastSeq}`;

    try {
      // Obsidian's requestUrl doesn't support SSE streaming natively.
      // We attempt a regular request to check if the endpoint exists.
      // If it returns 404 or any error, we degrade to polling.
      const resp = await requestUrl({
        url,
        method: "GET",
        headers: {
          Authorization: this.apiKey ? `Bearer ${this.apiKey}` : "",
          "X-Dat9-Actor": this.actorId,
          Accept: "text/event-stream",
        },
        throw: false,
      });

      if (resp.status === 404 || resp.status === 405) {
        // SSE endpoint not available — degrade to polling.
        this.degradeToPolling();
        return;
      }

      if (resp.status >= 400) {
        // Transient error — retry with backoff.
        this.scheduleReconnect();
        return;
      }

      // If we got a 200 response, try to parse SSE events from the body.
      // Note: Obsidian requestUrl buffers the full response, so this is
      // not true streaming. We parse what we got and reconnect for more.
      this.backoffMs = 1000; // reset backoff on success
      this.parseSSEResponse(resp.text);

      // Reconnect immediately for more events (long-poll style).
      if (!this.stopped) {
        this.connectSSE();
      }
    } catch {
      // Network error or requestUrl failure — degrade to polling.
      this.degradeToPolling();
    }
  }

  private parseSSEResponse(text: string): void {
    const lines = text.split("\n");
    let eventType = "";
    let dataStr = "";

    for (const line of lines) {
      if (line.startsWith("event: ")) {
        eventType = line.slice(7).trim();
      } else if (line.startsWith("data: ")) {
        dataStr = line.slice(6).trim();
      } else if (line === "" && dataStr) {
        this.handleSSEEvent(eventType, dataStr);
        eventType = "";
        dataStr = "";
      }
    }

    // Handle trailing event without empty line terminator.
    if (dataStr) {
      this.handleSSEEvent(eventType, dataStr);
    }
  }

  private handleSSEEvent(eventType: string, dataStr: string): void {
    try {
      const data = JSON.parse(dataStr);

      if (eventType === "reset" || data.reason) {
        const reset = data as ResetEvent;
        if (reset.seq) this.lastSeq = reset.seq;
        this.callbacks.onFullSync();
        return;
      }

      if (eventType === "heartbeat") {
        // Update seq from heartbeat but don't trigger any action.
        if (data.server_revision) {
          this.lastSeq = data.server_revision;
        }
        return;
      }

      // file_changed or default.
      const change = data as ChangeEvent;
      if (change.seq) this.lastSeq = change.seq;

      // Self-filtering: ignore our own changes.
      if (change.actor === this.actorId) return;

      // Structural ops trigger full sync.
      if (change.op === "rename" || change.op === "mkdir" || change.op === "copy") {
        this.callbacks.onFullSync();
        return;
      }

      this.callbacks.onRemoteChange(change.path, change.op);
    } catch {
      // Malformed event — skip.
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectSSE();
    }, this.backoffMs);
    this.backoffMs = Math.min(this.backoffMs * 2, 60000);
  }

  private degradeToPolling(): void {
    if (this.stopped) return;
    this._usePolling = true;
    console.log("[drive9] SSE unavailable, using polling fallback");
    this.schedulePoll();
  }

  private schedulePoll(): void {
    if (this.stopped) return;
    if (this.pollTimer) clearTimeout(this.pollTimer);
    this.pollTimer = setTimeout(() => {
      this.pollTimer = null;
      this.doPoll();
    }, this.pollIntervalMs);
  }

  private async doPoll(): Promise<void> {
    if (this.stopped) return;
    try {
      // Trigger a full sync check — the SyncEngine will diff remote vs local.
      this.callbacks.onFullSync();
    } finally {
      this.schedulePoll();
    }
  }
}
