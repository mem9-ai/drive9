import type { Client } from "./client.js";
import { checkError } from "./error.js";
import type {
  Journal,
  JournalAppendResponse,
  JournalCreateRequest,
  JournalEntry,
  JournalEntryInput,
  JournalSearchMatch,
  JournalSearchRequest,
  JournalVerifyResult,
} from "./models.js";

async function json<T>(resp: Response): Promise<T> {
  await checkError(resp);
  return (await resp.json()) as T;
}

async function ndjson<T>(resp: Response): Promise<T[]> {
  await checkError(resp);
  const text = await resp.text();
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}

export async function createJournal(client: Client, req: JournalCreateRequest): Promise<Journal> {
  return json<Journal>(
    await fetch(`${client.baseUrl}/v1/journals`, {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function appendJournalEntries(
  client: Client,
  journalId: string,
  appendId: string,
  entries: JournalEntryInput[]
): Promise<JournalAppendResponse> {
  return json<JournalAppendResponse>(
    await fetch(`${client.baseUrl}/v1/journals/${encodeURIComponent(journalId)}/entries`, {
      method: "POST",
      headers: client.authHeaders({
        "Content-Type": "application/json",
        "Idempotency-Key": appendId,
      }),
      body: JSON.stringify(entries),
    })
  );
}

export async function readJournalEntries(
  client: Client,
  journalId: string,
  afterSeq = 0,
  limit = 0
): Promise<JournalEntry[]> {
  const qs = new URLSearchParams();
  if (afterSeq > 0) qs.set("after_seq", String(afterSeq));
  if (limit > 0) qs.set("limit", String(limit));
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return ndjson<JournalEntry>(
    await fetch(`${client.baseUrl}/v1/journals/${encodeURIComponent(journalId)}/entries${suffix}`, {
      headers: client.authHeaders(),
    })
  );
}

export async function searchJournal(client: Client, req: JournalSearchRequest): Promise<JournalSearchMatch[]> {
  const qs = new URLSearchParams();
  if (req.type) qs.set("type", req.type);
  if (req.status) qs.set("status", req.status);
  if (req.kind) qs.set("kind", req.kind);
  if (req.actor_type) qs.set("actor", `${req.actor_type}:${req.actor_id || ""}`);
  for (const subject of req.subjects || []) qs.append("subject", subject);
  for (const label of req.labels || []) qs.append("meta", `${label.key}=${label.value}`);
  if (req.since) qs.set("since", req.since);
  if (req.until) qs.set("until", req.until);
  if (req.limit && req.limit > 0) qs.set("limit", String(req.limit));
  if (req.cursor) qs.set("cursor", req.cursor);
  if (req.entries) qs.set("include", "entry");
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return ndjson<JournalSearchMatch>(
    await fetch(`${client.baseUrl}/v1/journal-entries${suffix}`, { headers: client.authHeaders() })
  );
}

export async function verifyJournal(client: Client, journalId: string): Promise<JournalVerifyResult> {
  return json<JournalVerifyResult>(
    await fetch(`${client.baseUrl}/v1/journals/${encodeURIComponent(journalId)}/verify`, {
      headers: client.authHeaders(),
    })
  );
}
