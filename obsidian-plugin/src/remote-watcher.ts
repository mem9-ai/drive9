import { Platform } from "obsidian";
import { Drive9Client } from "./client";
import type { ChangeEvent, ResetEvent } from "./types";

const POLL_INTERVAL_MS = 30_000;
const SSE_INITIAL_BACKOFF_MS = 1_000;
const SSE_MAX_BACKOFF_MS = 30_000;

type NodeRequire = (name: string) => unknown;

export class RemoteWatcher {
  private lastSeq = 0;
  private stopped = true;
  private loopRunning = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private pollInFlight = false;
  private backoffMs = SSE_INITIAL_BACKOFF_MS;
  private activeRequest: { destroy: () => void } | null = null;

  constructor(
    private client: Drive9Client,
    private callbacks: {
      onChange: (event: ChangeEvent) => Promise<void> | void;
      onReset: (event: ResetEvent) => Promise<void> | void;
      onPoll: () => Promise<void> | void;
    },
  ) {}

  start(): void {
    if (this.stopped) {
      this.stopped = false;
      this.backoffMs = SSE_INITIAL_BACKOFF_MS;
      if (this.canUseSSE()) {
        void this.connectLoop();
      } else {
        this.startPolling(true);
      }
    }
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    this.activeRequest?.destroy();
    this.activeRequest = null;
  }

  restart(): void {
    this.stop();
    this.start();
  }

  private canUseSSE(): boolean {
    return Platform.isDesktopApp && typeof this.nodeRequire() === "function";
  }

  private nodeRequire(): NodeRequire | undefined {
    const value = (globalThis as { require?: NodeRequire }).require;
    return typeof value === "function" ? value : undefined;
  }

  private async connectLoop(): Promise<void> {
    if (this.loopRunning) return;
    this.loopRunning = true;
    try {
      while (!this.stopped) {
        try {
          await this.streamOnce();
        } catch (error) {
          console.warn("[drive9] SSE disconnected", error);
        }
        if (this.stopped) return;

        this.startPolling(true);
        await this.sleep(this.backoffMs);
        this.backoffMs = Math.min(this.backoffMs * 2, SSE_MAX_BACKOFF_MS);
      }
    } finally {
      this.loopRunning = false;
    }
  }

  private startPolling(runImmediately: boolean): void {
    if (!this.pollTimer) {
      this.pollTimer = setInterval(() => {
        void this.runPoll();
      }, POLL_INTERVAL_MS);
    }
    if (runImmediately) {
      void this.runPoll();
    }
  }

  private stopPolling(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private async runPoll(): Promise<void> {
    if (this.stopped || this.pollInFlight) return;
    this.pollInFlight = true;
    try {
      await this.callbacks.onPoll();
    } catch (error) {
      console.warn("[drive9] polling sync failed", error);
    } finally {
      this.pollInFlight = false;
    }
  }

  private async streamOnce(): Promise<void> {
    const nodeRequire = this.nodeRequire();
    if (!nodeRequire) {
      throw new Error("SSE requires desktop runtime");
    }

    const baseURL = this.client.getServerUrl();
    if (!baseURL) {
      throw new Error("server URL is not configured");
    }

    const url = new URL(`/v1/events?since=${this.lastSeq}`, ensureBaseURL(baseURL));
    const transport = url.protocol === "https:" ? nodeRequire("https") : nodeRequire("http");
    if (!transport || typeof (transport as { request?: unknown }).request !== "function") {
      throw new Error(`unsupported SSE transport for ${url.protocol}`);
    }

    await new Promise<void>((resolve, reject) => {
      let buffer = "";
      let eventType = "";
      let dataLine = "";
      let settled = false;

      const finish = (fn: () => void): void => {
        if (settled) return;
        settled = true;
        this.activeRequest = null;
        fn();
      };

      const request = (transport as {
        request: (
          url: URL,
          opts: { method: string; headers: Record<string, string> },
          cb: (resp: {
            statusCode?: number;
            on: (name: string, listener: (...args: unknown[]) => void) => void;
            setEncoding: (enc: string) => void;
            resume: () => void;
          }) => void,
        ) => { on: (name: string, listener: (...args: unknown[]) => void) => void; end: () => void; destroy: () => void };
      }).request(
        url,
        {
          method: "GET",
          headers: this.streamHeaders(),
        },
        (resp) => {
          const status = resp.statusCode ?? 0;
          if (status !== 200) {
            resp.resume();
            finish(() => reject(new Error(`SSE status ${status}`)));
            return;
          }

          this.backoffMs = SSE_INITIAL_BACKOFF_MS;
          this.stopPolling();
          resp.setEncoding("utf8");

          const flushEvent = (): void => {
            if (!dataLine) return;
            this.dispatchEvent(eventType, dataLine);
            eventType = "";
            dataLine = "";
          };

          resp.on("data", (chunk) => {
            buffer += String(chunk);
            while (true) {
              const newline = buffer.indexOf("\n");
              if (newline < 0) break;
              const line = buffer.slice(0, newline).replace(/\r$/, "");
              buffer = buffer.slice(newline + 1);

              if (line === "") {
                flushEvent();
                continue;
              }
              if (line.startsWith("event: ")) {
                eventType = line.slice("event: ".length);
              } else if (line.startsWith("data: ")) {
                dataLine = line.slice("data: ".length);
              }
            }
          });

          resp.on("end", () => finish(resolve));
          resp.on("close", () => finish(resolve));
          resp.on("error", (error) => finish(() => reject(error instanceof Error ? error : new Error(String(error)))));
        },
      );

      this.activeRequest = request;
      request.on("error", (error) => finish(() => reject(error instanceof Error ? error : new Error(String(error)))));
      request.end();
    });
  }

  private streamHeaders(): Record<string, string> {
    return {
      Accept: "text/event-stream",
      ...(this.client.getAPIKey() ? { Authorization: `Bearer ${this.client.getAPIKey()}` } : {}),
      ...(this.client.getActorId() ? { "X-Dat9-Actor": this.client.getActorId() } : {}),
    };
  }

  private dispatchEvent(eventType: string, data: string): void {
    switch (eventType) {
      case "file_changed": {
        const event = safeJSONParse<ChangeEvent>(data);
        if (!event) return;
        if (event.seq > this.lastSeq) {
          this.lastSeq = event.seq;
        }
        if (event.actor && event.actor === this.client.getActorId()) {
          return;
        }
        void Promise.resolve(this.callbacks.onChange(event));
        return;
      }
      case "reset": {
        const event = safeJSONParse<ResetEvent>(data);
        if (!event) return;
        if (event.seq > this.lastSeq) {
          this.lastSeq = event.seq;
        }
        void Promise.resolve(this.callbacks.onReset(event));
        return;
      }
      case "heartbeat":
        return;
      default:
        return;
    }
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        resolve();
      }, ms);
    });
  }
}

function ensureBaseURL(baseURL: string): string {
  return baseURL.endsWith("/") ? baseURL : `${baseURL}/`;
}

function safeJSONParse<T>(value: string): T | null {
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}
