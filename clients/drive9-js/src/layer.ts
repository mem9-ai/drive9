import type { Client } from "./client.js";
import { checkError } from "./error.js";
import type {
  LayerCreateRequest,
  LayerDeleteOptions,
  LayerDeleteResult,
  LayerForkRequest,
  LayerInfo,
  LayerListOptions,
  LayerListPage,
  LayerMergeResult,
  LayerMountBindRequest,
  LayerMountLease,
  LayerMountRenewRequest,
  LayerMountUnbindRequest,
  LayerMountUnbindResult,
} from "./models.js";
import { clientFetch, normalizeLayerRef, validateLayerId } from "./view.js";

function layerRefURL(client: Client, ref: string, suffix = ""): string {
  const norm = normalizeLayerRef(ref);
  return `${client.baseUrl}/v1/layers/${encodeURIComponent(norm)}${suffix}`;
}

function canonicalLayerURL(client: Client, layerId: string, suffix: string): string {
  validateLayerId(layerId);
  return `${client.baseUrl}/v1/layers/${layerId}${suffix}`;
}

async function decodeJSON<T>(resp: Response): Promise<T> {
  await checkError(resp);
  return (await resp.json()) as T;
}

export async function createLayer(client: Client, req: LayerCreateRequest = {}): Promise<LayerInfo> {
  return decodeJSON<LayerInfo>(
    await clientFetch(client, `${client.baseUrl}/v1/layers`, {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function listLayers(client: Client, options: LayerListOptions = {}): Promise<LayerListPage> {
  const qs = new URLSearchParams();
  if (options.limit != null && options.limit > 0) {
    qs.set("limit", String(options.limit));
  }
  if (options.after) {
    qs.set("after", options.after);
  }
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  const page = await decodeJSON<LayerListPage>(
    await clientFetch(client, `${client.baseUrl}/v1/layers${suffix}`, {
      headers: client.authHeaders(),
    })
  );
  return {
    layers: page.layers || [],
    next_after: page.next_after || undefined,
  };
}

export async function listAllLayers(client: Client): Promise<LayerInfo[]> {
  const all: LayerInfo[] = [];
  let after: string | undefined;
  while (true) {
    const page = await listLayers(client, { limit: 200, after });
    all.push(...page.layers);
    if (!page.next_after) break;
    after = page.next_after;
  }
  return all;
}

export async function getLayer(client: Client, ref: string): Promise<LayerInfo> {
  return decodeJSON<LayerInfo>(
    await clientFetch(client, layerRefURL(client, ref), { headers: client.authHeaders() })
  );
}

export async function forkLayer(client: Client, ref: string, req: LayerForkRequest = {}): Promise<LayerInfo> {
  return decodeJSON<LayerInfo>(
    await clientFetch(client, layerRefURL(client, ref, "/fork"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function mergeLayer(client: Client, ref: string): Promise<LayerMergeResult> {
  return decodeJSON<LayerMergeResult>(
    await clientFetch(client, layerRefURL(client, ref, "/merge"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: "{}",
    })
  );
}

export async function deleteLayer(
  client: Client,
  ref: string,
  options?: LayerDeleteOptions | boolean
): Promise<LayerDeleteResult> {
  const force = typeof options === "boolean" ? options : Boolean(options?.force);
  const query = force ? "?force=true" : "";
  return decodeJSON<LayerDeleteResult>(
    await clientFetch(client, `${layerRefURL(client, ref)}${query}`, {
      method: "DELETE",
      headers: client.authHeaders(),
    })
  );
}

export async function bindLayerMount(
  client: Client,
  layerId: string,
  req: LayerMountBindRequest
): Promise<LayerMountLease> {
  return decodeJSON<LayerMountLease>(
    await clientFetch(client, canonicalLayerURL(client, layerId, "/bind-mount"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function renewLayerMount(
  client: Client,
  layerId: string,
  req: LayerMountRenewRequest
): Promise<LayerMountLease> {
  return decodeJSON<LayerMountLease>(
    await clientFetch(client, canonicalLayerURL(client, layerId, "/renew-mount"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function unbindLayerMount(
  client: Client,
  layerId: string,
  req: LayerMountUnbindRequest
): Promise<LayerMountUnbindResult> {
  return decodeJSON<LayerMountUnbindResult>(
    await clientFetch(client, canonicalLayerURL(client, layerId, "/unbind-mount"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}
