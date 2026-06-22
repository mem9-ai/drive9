import type { Client } from "./client.js";
import { bodyInit } from "./compat.js";
import { checkError, StatusError } from "./error.js";
import type {
  FSLayer,
  FSLayerCheckpoint,
  FSLayerCheckpointRequest,
  FSLayerCommit,
  FSLayerEntry,
  FSLayerEntryRequest,
  FSLayerEvent,
  FSLayerCreateRequest,
} from "./models.js";

function layerURL(client: Client, layerId: string, suffix = ""): string {
  return `${client.baseUrl}/v1/layers/${encodeURIComponent(layerId)}${suffix}`;
}

function withBase64Content<T extends { content?: Uint8Array | string }>(req: T): T {
  if (req.content instanceof Uint8Array) {
    return { ...req, content: Buffer.from(req.content).toString("base64") };
  }
  return req;
}

async function decodeJSON<T>(resp: Response): Promise<T> {
  await checkError(resp);
  return (await resp.json()) as T;
}

export async function createFSLayer(client: Client, req: FSLayerCreateRequest): Promise<FSLayer> {
  return decodeJSON<FSLayer>(
    await fetch(`${client.baseUrl}/v1/layers`, {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function listFSLayers(client: Client): Promise<FSLayer[]> {
  const body = await decodeJSON<{ layers?: FSLayer[] }>(
    await fetch(`${client.baseUrl}/v1/layers`, { headers: client.authHeaders() })
  );
  return body.layers || [];
}

export async function getFSLayer(client: Client, layerId: string): Promise<FSLayer> {
  return decodeJSON<FSLayer>(await fetch(layerURL(client, layerId), { headers: client.authHeaders() }));
}

export async function diffFSLayer(client: Client, layerId: string, maxSeq?: number, replay = false): Promise<FSLayerEntry[]> {
  const qs = new URLSearchParams();
  if (maxSeq != null) qs.set("max_seq", String(maxSeq));
  if (replay) qs.set("replay", "1");
  const suffix = `/diff${qs.toString() ? `?${qs.toString()}` : ""}`;
  const body = await decodeJSON<{ entries?: FSLayerEntry[] }>(
    await fetch(layerURL(client, layerId, suffix), { headers: client.authHeaders() })
  );
  return body.entries || [];
}

export async function upsertFSLayerEntry(client: Client, layerId: string, req: FSLayerEntryRequest): Promise<FSLayerEntry> {
  return decodeJSON<FSLayerEntry>(
    await fetch(layerURL(client, layerId, "/entries"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(withBase64Content(req)),
    })
  );
}

export async function uploadFSLayerFile(
  client: Client,
  layerId: string,
  path: string,
  data: Uint8Array,
  opts: { baseRevision?: number; mode?: number } = {}
): Promise<FSLayerEntry> {
  const qs = new URLSearchParams({ path, size: String(data.length) });
  if (opts.baseRevision && opts.baseRevision > 0) qs.set("base_revision", String(opts.baseRevision));
  if (opts.mode != null) qs.set("mode", (opts.mode & 0o777).toString(8));
  return decodeJSON<FSLayerEntry>(
    await fetch(layerURL(client, layerId, `/objects?${qs.toString()}`), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/octet-stream" }),
      body: bodyInit(data),
    })
  );
}

export async function readFSLayerFile(
  client: Client,
  layerId: string,
  path: string,
  maxSeq?: number
): Promise<Uint8Array> {
  const stream = await readFSLayerFileStream(client, layerId, path, maxSeq);
  const resp = new Response(stream);
  return new Uint8Array(await resp.arrayBuffer());
}

export async function readFSLayerFileStream(
  client: Client,
  layerId: string,
  path: string,
  maxSeq?: number
): Promise<ReadableStream<Uint8Array>> {
  const qs = new URLSearchParams({ path });
  if (maxSeq != null) qs.set("max_seq", String(maxSeq));
  const resp = await fetch(layerURL(client, layerId, `/objects?${qs.toString()}`), {
    headers: client.authHeaders(),
  });
  await checkError(resp);
  if (!resp.body) throw new Error("empty response body");
  return resp.body;
}

export async function getFSLayerEntry(client: Client, layerId: string, path: string, maxSeq?: number): Promise<FSLayerEntry> {
  const qs = new URLSearchParams({ path });
  if (maxSeq != null) qs.set("max_seq", String(maxSeq));
  return decodeJSON<FSLayerEntry>(
    await fetch(layerURL(client, layerId, `/entries?${qs.toString()}`), { headers: client.authHeaders() })
  );
}

export async function checkpointFSLayer(
  client: Client,
  layerId: string,
  req: FSLayerCheckpointRequest
): Promise<FSLayerCheckpoint> {
  return decodeJSON<FSLayerCheckpoint>(
    await fetch(layerURL(client, layerId, "/checkpoints"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function getFSLayerCheckpoint(client: Client, checkpointId: string): Promise<FSLayerCheckpoint> {
  return decodeJSON<FSLayerCheckpoint>(
    await fetch(`${client.baseUrl}/v1/layer-checkpoints/${encodeURIComponent(checkpointId)}`, {
      headers: client.authHeaders(),
    })
  );
}

export async function listFSLayerEvents(client: Client, layerId: string, since = 0): Promise<FSLayerEvent[]> {
  const qs = since > 0 ? `?since=${encodeURIComponent(String(since))}` : "";
  const body = await decodeJSON<{ events?: FSLayerEvent[] }>(
    await fetch(layerURL(client, layerId, `/events${qs}`), { headers: client.authHeaders() })
  );
  return body.events || [];
}

export async function rollbackFSLayer(client: Client, layerId: string): Promise<void> {
  await checkError(
    await fetch(layerURL(client, layerId, "/rollback"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: "{}",
    })
  );
}

export async function commitFSLayer(client: Client, layerId: string): Promise<FSLayerCommit> {
  const resp = await fetch(layerURL(client, layerId, "/commit"), {
    method: "POST",
    headers: client.authHeaders({ "Content-Type": "application/json" }),
    body: "{}",
  });
  if (resp.status === 409) {
    const body = (await resp.json().catch(() => undefined)) as FSLayerCommit | undefined;
    if (body) {
      throw new StatusError("fs layer commit conflict", 409);
    }
  }
  return decodeJSON<FSLayerCommit>(resp);
}
