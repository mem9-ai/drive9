import type { Client } from "./client.js";
import { StatusError } from "./error.js";
import type { ChangeEvent, EventHandler, EventLifecycle, HeartbeatEvent, ResetEvent, WatchEventsOptions } from "./models.js";

const DEFAULT_INITIAL_BACKOFF_MS = 1000;
const DEFAULT_MAX_BACKOFF_MS = 30000;

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason || new Error("aborted"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(signal.reason || new Error("aborted"));
      },
      { once: true }
    );
  });
}

export async function watchEvents(
  client: Client,
  actor: string,
  handler: EventHandler,
  options: WatchEventsOptions = {}
): Promise<void> {
  return watchEventsWithLifecycle(client, actor, handler, {}, options);
}

export async function watchEventsWithLifecycle(
  client: Client,
  actor: string,
  handler: EventHandler,
  lifecycle: EventLifecycle = {},
  options: WatchEventsOptions = {}
): Promise<void> {
  let lastSeq = options.initialSince || 0;
  let backoff = options.initialBackoffMs || DEFAULT_INITIAL_BACKOFF_MS;
  const maxBackoff = options.maxBackoffMs || DEFAULT_MAX_BACKOFF_MS;
  const signal = options.signal;

  while (!signal?.aborted) {
    let streamErr: Error | undefined;
    try {
      await streamEvents(client, lastSeq, actor || options.actor || "", signal, async (eventType, data) => {
        if (eventType === "file_changed") {
          const ev = JSON.parse(data) as ChangeEvent;
          if (ev.seq > lastSeq) lastSeq = ev.seq;
          await handler(ev, undefined);
        } else if (eventType === "reset") {
          const ev = JSON.parse(data) as ResetEvent;
          if (ev.seq > lastSeq) lastSeq = ev.seq;
          await handler(undefined, ev);
        } else if (eventType === "heartbeat") {
          const ev = JSON.parse(data) as HeartbeatEvent;
          if (ev.seq > lastSeq) lastSeq = ev.seq;
          await lifecycle.onCurrent?.(ev.seq);
        }
      });
    } catch (err) {
      if (signal?.aborted) break;
      streamErr = err instanceof Error ? err : new Error(String(err));
    }
    if (signal?.aborted) break;
    await lifecycle.onDisconnected?.(streamErr);
    await sleep(backoff, signal);
    backoff = streamErr ? Math.min(backoff * 2, maxBackoff) : DEFAULT_INITIAL_BACKOFF_MS;
  }
}

async function streamEvents(
  client: Client,
  since: number,
  actor: string,
  signal: AbortSignal | undefined,
  dispatch: (eventType: string, data: string) => Promise<void>
): Promise<void> {
  const resp = await fetch(`${client.baseUrl}/v1/events?since=${encodeURIComponent(String(since))}`, {
    headers: client.authHeaders({
      Accept: "text/event-stream",
      ...(actor ? { "X-Dat9-Actor": actor } : {}),
    }),
    signal,
  });
  if (!resp.ok) {
    throw new StatusError(`SSE status ${resp.status}`, resp.status);
  }
  if (!resp.body) {
    throw new Error("SSE response had no body");
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let eventType = "";
  let data = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let idx: number;
    while ((idx = buffer.indexOf("\n")) >= 0) {
      const rawLine = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
      if (line === "") {
        if (data) {
          await dispatch(eventType, data);
        }
        eventType = "";
        data = "";
      } else if (line.startsWith("event: ")) {
        eventType = line.slice("event: ".length);
      } else if (line.startsWith("data: ")) {
        data = data ? `${data}\n${line.slice("data: ".length)}` : line.slice("data: ".length);
      }
    }
  }
}
