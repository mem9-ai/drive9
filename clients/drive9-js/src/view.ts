import { Drive9Error, StatusError } from "./error.js";
import type { ViewBinding } from "./models.js";

export const VIEW_ROUTE_PREFIX = "/v1/views/";
export const LAYER_FENCE_HEADER = "X-FS-Layer-Fence";
export const MAX_VIEW_DOWNLOAD_REDIRECTS = 10;

const CANONICAL_LAYER_ID_RE = /^layer_[0-9a-hjkmnp-tv-z]{26}$/;
const MAX_LAYER_REF_BYTES = 320;

export class UnsupportedInLayerViewError extends StatusError {
  constructor(message = "operation unsupported in layer view") {
    super(message, 501, "UNSUPPORTED_IN_LAYER_VIEW");
    this.name = "UnsupportedInLayerViewError";
  }
}

export class InvalidLayerViewRequestError extends Drive9Error {
  constructor(message = "invalid layer view request") {
    super(message);
    this.name = "InvalidLayerViewRequestError";
  }
}

export interface NormalizedViewBinding {
  readonly layerId: string;
  readonly fence: number;
  readonly baseOrigin?: URL;
  readonly writeGuard?: () => void;
}

export function isCanonicalLayerId(id: string): boolean {
  return CANONICAL_LAYER_ID_RE.test(id);
}

export function validateLayerId(id: string): void {
  if (!isCanonicalLayerId(id)) {
    throw new InvalidLayerViewRequestError(`invalid layer id: ${JSON.stringify(id)}`);
  }
}

export function normalizeLayerRef(raw: string): string {
  const ref = raw.trim();
  if (!ref || Buffer.byteLength(ref, "utf8") > MAX_LAYER_REF_BYTES || ref.toLowerCase() === "main") {
    throw new InvalidLayerViewRequestError(`invalid layer ref: ${JSON.stringify(raw)}`);
  }
  for (let i = 0; i < ref.length; i++) {
    const code = ref.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) {
      throw new InvalidLayerViewRequestError("invalid layer ref: control character");
    }
  }
  return ref;
}

export function parseCanonicalBaseOrigin(rawBase: string): URL {
  let u: URL;
  try {
    u = new URL(rawBase.trim().replace(/\/+$/, ""));
  } catch {
    throw new InvalidLayerViewRequestError("invalid client base URL");
  }
  if ((u.protocol !== "http:" && u.protocol !== "https:") || !u.hostname || u.username || u.password) {
    throw new InvalidLayerViewRequestError("invalid client base URL");
  }
  return u;
}

function canonicalOriginParts(u: URL): { scheme: string; host: string; port: string } | undefined {
  if ((u.protocol !== "http:" && u.protocol !== "https:") || !u.hostname || u.username || u.password) {
    return undefined;
  }
  const scheme = u.protocol.toLowerCase();
  const host = u.hostname.toLowerCase().replace(/\.$/, "");
  if (!host) return undefined;
  const port = u.port || (scheme === "https:" ? "443" : "80");
  return { scheme, host, port };
}

function sameCanonicalOrigin(base: URL, target: URL): boolean {
  const b = canonicalOriginParts(base);
  const t = canonicalOriginParts(target);
  return Boolean(b && t && b.scheme === t.scheme && b.host === t.host && b.port === t.port);
}

function stripBaseOriginPrefix(baseOrigin: URL, escapedPath: string): { basePrefix: string; innerEscaped: string } | undefined {
  const rawBasePath = baseOrigin.pathname.replace(/\/+$/, "");
  if (rawBasePath && rawBasePath !== "/") {
    if (!escapedPath.startsWith(`${rawBasePath}/`)) {
      return undefined;
    }
    return { basePrefix: rawBasePath, innerEscaped: escapedPath.slice(rawBasePath.length) };
  }
  if (!escapedPath.startsWith("/")) return undefined;
  return { basePrefix: "", innerEscaped: escapedPath };
}

export function validateViewBinding(binding: ViewBinding | undefined, baseUrl: string): NormalizedViewBinding {
  const layerId = binding?.layerId ?? "";
  const fence = binding?.fence ?? 0;
  if (!layerId) {
    if (fence !== 0) {
      throw new InvalidLayerViewRequestError(`main view cannot carry non-zero fence ${String(fence)}`);
    }
    return { layerId: "", fence: 0 };
  }
  validateLayerId(layerId);
  if (!Number.isSafeInteger(fence) || fence < 0 || (fence > 0 && fence % 2 === 0)) {
    throw new InvalidLayerViewRequestError(`invalid layer fence: ${String(fence)}`);
  }
  const baseOrigin = parseCanonicalBaseOrigin(baseUrl);
  return { layerId, fence, baseOrigin, writeGuard: binding?.writeGuard };
}

const VIEW_BINDINGS = new WeakMap<object, NormalizedViewBinding>();

export function setClientViewBinding(client: object, binding: NormalizedViewBinding): void {
  VIEW_BINDINGS.set(client, binding);
}

export function getClientViewBinding(client: object): NormalizedViewBinding {
  return VIEW_BINDINGS.get(client) ?? { layerId: "", fence: 0 };
}

export function hasActiveLayerView(client: object): boolean {
  return Boolean(VIEW_BINDINGS.get(client)?.layerId);
}

type RouteAction = "pass_through" | "rewrite_view" | "reject_unsupported" | "reject_invalid";

function queryKeysSubsetOf(q: URLSearchParams, allowed: string[]): boolean {
  const set = new Set(allowed);
  for (const key of q.keys()) {
    if (!set.has(key)) return false;
  }
  return true;
}

function queryEmpty(q: URLSearchParams): boolean {
  for (const _ of q.keys()) return false;
  return true;
}

function isPassThroughControlRoute(method: string, escapedPath: string, q: URLSearchParams): boolean {
  if (escapedPath === "/v1/status") return method === "GET" && queryEmpty(q);
  if (escapedPath === "/v1/layers") {
    if (method === "POST") return queryEmpty(q);
    if (method === "GET") return queryKeysSubsetOf(q, ["limit", "after"]);
    return false;
  }
  if (escapedPath.startsWith("/v1/layers/")) {
    const rest = escapedPath.slice("/v1/layers/".length);
    if (!rest || rest.endsWith("/")) return false;
    const slash = rest.indexOf("/");
    if (slash < 0) {
      if (method === "GET") return queryEmpty(q);
      if (method === "DELETE") return queryKeysSubsetOf(q, ["force"]);
      return false;
    }
    const ref = rest.slice(0, slash);
    const action = rest.slice(slash + 1);
    if (!ref || action.includes("/") || method !== "POST" || !queryEmpty(q)) return false;
    return action === "fork" || action === "merge" || action === "bind-mount" || action === "renew-mount" || action === "unbind-mount";
  }
  if (escapedPath === "/v1/mounts") return method === "POST" && queryEmpty(q);
  if (escapedPath.startsWith("/v1/mounts/")) {
    if (!queryEmpty(q)) return false;
    const rest = escapedPath.slice("/v1/mounts/".length);
    if (!rest || rest.endsWith("/")) return false;
    if (rest.endsWith("/heartbeat")) {
      const id = rest.slice(0, -"/heartbeat".length);
      return Boolean(id && !id.includes("/") && method === "POST");
    }
    return !rest.includes("/") && method === "DELETE";
  }
  return false;
}

function classifyFSPostViewQuery(q: URLSearchParams): RouteAction {
  const selectors = ["append-log", "append", "copy", "rename", "mkdir", "chmod", "setmeta", "create", "symlink", "hardlink"];
  let count = 0;
  let selected = "";
  for (const k of selectors) {
    if (q.has(k)) {
      count++;
      selected = k;
    }
  }
  if (count !== 1) {
    return count === 0 && !queryEmpty(q) ? "reject_unsupported" : "reject_invalid";
  }
  if (selected === "append" || selected === "append-log") return "reject_unsupported";
  if (selected === "create") return queryKeysSubsetOf(q, ["create", "layout"]) ? "rewrite_view" : "reject_invalid";
  if (selected === "mkdir") return queryKeysSubsetOf(q, ["mkdir", "mode"]) ? "rewrite_view" : "reject_invalid";
  return queryKeysSubsetOf(q, [selected]) ? "rewrite_view" : "reject_invalid";
}

function classifyViewDataRoute(method: string, escapedPath: string, q: URLSearchParams): RouteAction {
  if (escapedPath === "/v1/views" || escapedPath.startsWith(VIEW_ROUTE_PREFIX)) return "reject_invalid";
  if (escapedPath === "/v1/fs:batch-stat" || escapedPath === "/v1/fs:batch-read-small" || escapedPath === "/v1/fs:batch-write") {
    return method === "POST" && queryEmpty(q) ? "rewrite_view" : "reject_unsupported";
  }
  if (escapedPath === "/v1/fs" || escapedPath.startsWith("/v1/fs/")) {
    if (method === "HEAD" || method === "PUT" || method === "PATCH") return queryEmpty(q) ? "rewrite_view" : "reject_unsupported";
    if (method === "DELETE") return queryKeysSubsetOf(q, ["kind", "recursive"]) ? "rewrite_view" : "reject_unsupported";
    if (method === "GET") {
      if (q.has("grep") || q.has("find") || q.has("tasks")) return "reject_unsupported";
      if (q.has("stat")) return queryKeysSubsetOf(q, ["stat"]) ? "rewrite_view" : "reject_unsupported";
      if (q.has("list")) return queryKeysSubsetOf(q, ["list", "limit", "cursor"]) ? "rewrite_view" : "reject_unsupported";
      return queryEmpty(q) ? "rewrite_view" : "reject_unsupported";
    }
    if (method === "POST") return classifyFSPostViewQuery(q);
    return "reject_unsupported";
  }
  if (escapedPath === "/v1/uploads" || escapedPath.startsWith("/v1/uploads/")) {
    if (escapedPath === "/v1/uploads") {
      if (method === "POST" && queryEmpty(q)) return "rewrite_view";
      if (method === "GET" && queryKeysSubsetOf(q, ["path", "status"])) return "rewrite_view";
      return "reject_unsupported";
    }
    if (escapedPath === "/v1/uploads/initiate") return method === "POST" && queryEmpty(q) ? "rewrite_view" : "reject_unsupported";
    const rest = escapedPath.slice("/v1/uploads/".length);
    if (!rest || rest.endsWith("/")) return "reject_unsupported";
    const slash = rest.indexOf("/");
    if (slash < 0) return method === "DELETE" && queryEmpty(q) ? "rewrite_view" : "reject_unsupported";
    const id = rest.slice(0, slash);
    const action = rest.slice(slash + 1);
    if (!id || action.includes("/") || !queryEmpty(q)) return "reject_unsupported";
    if (action === "complete") return method === "POST" ? "rewrite_view" : "reject_unsupported";
    if (action === "resume") return method === "GET" || method === "POST" ? "rewrite_view" : "reject_unsupported";
    return "reject_unsupported";
  }
  if (escapedPath.startsWith("/v2/uploads/")) {
    if (method !== "POST" || !queryEmpty(q)) return "reject_unsupported";
    const rest = escapedPath.slice("/v2/uploads/".length);
    if (!rest || rest.endsWith("/")) return "reject_unsupported";
    if (rest === "initiate") return "rewrite_view";
    const slash = rest.indexOf("/");
    if (slash < 0) return "reject_unsupported";
    const id = rest.slice(0, slash);
    const action = rest.slice(slash + 1);
    if (!id || id === "initiate" || action.includes("/")) return "reject_unsupported";
    return action === "presign" || action === "presign-batch" || action === "complete" || action === "abort"
      ? "rewrite_view"
      : "reject_unsupported";
  }
  return "reject_unsupported";
}

function classifyClientRoute(method: string, escapedPath: string, q: URLSearchParams): RouteAction {
  if (escapedPath === "/v1/views" || escapedPath.startsWith(VIEW_ROUTE_PREFIX)) return "reject_invalid";
  if (isPassThroughControlRoute(method, escapedPath, q)) return "pass_through";
  return classifyViewDataRoute(method, escapedPath, q);
}

export function rewriteRequestForView(
  view: NormalizedViewBinding | undefined,
  urlStr: string,
  init?: RequestInit
): { url: string; init: RequestInit } {
  if (!view || !view.layerId) {
    return { url: urlStr, init: init ?? {} };
  }
  if (!view.baseOrigin) {
    throw new InvalidLayerViewRequestError("missing view base origin");
  }
  let targetUrl: URL;
  try {
    targetUrl = new URL(urlStr);
  } catch {
    throw new InvalidLayerViewRequestError("invalid request URL");
  }
  if (!sameCanonicalOrigin(view.baseOrigin, targetUrl)) {
    throw new InvalidLayerViewRequestError("request does not match client base origin");
  }
  const stripped = stripBaseOriginPrefix(view.baseOrigin, targetUrl.pathname);
  if (!stripped) {
    throw new InvalidLayerViewRequestError("request path outside base prefix");
  }
  const method = (init?.method || "GET").toUpperCase();
  const action = classifyClientRoute(method, stripped.innerEscaped, targetUrl.searchParams);
  const headers = new Headers(init?.headers);

  switch (action) {
    case "pass_through":
      headers.delete(LAYER_FENCE_HEADER);
      return { url: urlStr, init: { ...init, headers } };
    case "rewrite_view": {
      const isGetUploadResume =
        method === "GET" &&
        stripped.innerEscaped.startsWith("/v1/uploads/") &&
        stripped.innerEscaped.endsWith("/resume");
      const isBatchRead = method === "POST" &&
        (stripped.innerEscaped === "/v1/fs:batch-stat" || stripped.innerEscaped === "/v1/fs:batch-read-small");
      if (((method !== "GET" && method !== "HEAD" && !isBatchRead) || isGetUploadResume) && view.writeGuard) {
        view.writeGuard();
      }
      if (headers.get("X-Dat9-Description")) {
        throw new UnsupportedInLayerViewError("description metadata is unsupported in layer view");
      }
      const nextUrl = new URL(targetUrl.toString());
      nextUrl.pathname = `${stripped.basePrefix}${VIEW_ROUTE_PREFIX}${view.layerId}${stripped.innerEscaped}`;
      if (view.fence > 0) {
        headers.set(LAYER_FENCE_HEADER, String(view.fence));
      } else {
        headers.delete(LAYER_FENCE_HEADER);
      }
      return { url: nextUrl.toString(), init: { ...init, headers } };
    }
    case "reject_invalid":
      throw new InvalidLayerViewRequestError();
    default:
      throw new UnsupportedInLayerViewError();
  }
}

export async function clientFetch(
  client: { readonly baseUrl: string },
  urlStr: string,
  init?: RequestInit
): Promise<Response> {
  const view = VIEW_BINDINGS.get(client);
  const rewritten = rewriteRequestForView(view, urlStr, init);
  return fetch(rewritten.url, rewritten.init);
}

const ALLOWED_REDIRECT_HEADERS = new Set([
  "range",
  "if-range",
  "if-match",
  "if-none-match",
  "if-modified-since",
  "if-unmodified-since",
  "accept",
  "accept-encoding",
  "user-agent",
]);

function filterAllowedRedirectHeaders(raw?: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  if (!raw) return out;
  for (const [k, v] of Object.entries(raw)) {
    if (ALLOWED_REDIRECT_HEADERS.has(k.toLowerCase())) {
      out[k] = v;
    }
  }
  return out;
}

function isSameOrSubdomainHost(host: string, baseHost: string): boolean {
  return Boolean(host && baseHost && (host === baseHost || host.endsWith(`.${baseHost}`)));
}

function isDrive9ApiRoutePath(lowerPath: string): boolean {
  return lowerPath === "/v1" || lowerPath.startsWith("/v1/") || lowerPath === "/v2" || lowerPath.startsWith("/v2/");
}

function posixCleanPath(rawPath: string): string {
  const parts = rawPath.split("/");
  const stack: string[] = [];
  for (const seg of parts) {
    if (!seg || seg === ".") continue;
    if (seg === "..") {
      stack.pop();
    } else {
      stack.push(seg);
    }
  }
  return `/${stack.join("/")}`;
}

function validateViewRedirectTarget(baseOrigin: URL, sourceUrl: URL, resolved: URL): void {
  const baseParts = canonicalOriginParts(baseOrigin);
  const srcParts = canonicalOriginParts(sourceUrl);
  const dstParts = canonicalOriginParts(resolved);
  if (!baseParts || !srcParts || !dstParts) {
    throw new InvalidLayerViewRequestError("redirect not allowed for layer view request");
  }
  const matchesBaseHostOrSubdomain = isSameOrSubdomainHost(dstParts.host, baseParts.host);
  const matchesSrcHostOrSubdomain = isSameOrSubdomainHost(dstParts.host, srcParts.host);
  if ((srcParts.scheme === "https:" || baseParts.scheme === "https:") && dstParts.scheme !== "https:") {
    if (matchesBaseHostOrSubdomain || matchesSrcHostOrSubdomain) {
      throw new InvalidLayerViewRequestError("HTTPS downgrade redirect not allowed for layer view request");
    }
  }
  if (matchesBaseHostOrSubdomain) {
    const esc = resolved.pathname;
    if (/%2f|%5c/i.test(esc)) {
      throw new InvalidLayerViewRequestError("encoded separator not allowed in layer view redirect");
    }
    let decoded: string;
    let decodedBase: string;
    try {
      decoded = decodeURIComponent(esc);
      decodedBase = decodeURIComponent(baseOrigin.pathname);
    } catch {
      throw new InvalidLayerViewRequestError("invalid redirect path in layer view request");
    }
    if (decoded.includes("\\") || decoded.includes("\0")) {
      throw new InvalidLayerViewRequestError("invalid redirect path in layer view request");
    }
    const prefixes = new Set([baseOrigin.pathname, decodedBase].map(prefix => posixCleanPath(prefix).toLowerCase()));
    const candidates = [
      esc,
      `/${esc.replace(/^\/+/, "")}`,
      decoded,
      `/${decoded.replace(/^\/+/, "")}`,
      posixCleanPath(`/${decoded.replace(/^\/+/, "")}`),
    ];
    for (const p of candidates) {
      const lp = p.toLowerCase();
      if (isDrive9ApiRoutePath(lp)) {
        throw new InvalidLayerViewRequestError("redirect to API route not allowed for layer view request");
      }
      for (const prefix of prefixes) {
        if (prefix === "/" || !lp.startsWith(`${prefix}/`)) continue;
        const stripped = lp.slice(prefix.length);
        const normStripped = `/${stripped.replace(/^\/+/, "")}`;
        if (isDrive9ApiRoutePath(stripped) || isDrive9ApiRoutePath(normStripped) || isDrive9ApiRoutePath(posixCleanPath(normStripped))) {
          throw new InvalidLayerViewRequestError("redirect to API route not allowed for layer view request");
        }
      }
    }
  }
}

export async function fetchViewObjectRedirect(
  client: { readonly baseUrl: string },
  sourceUrlStr: string,
  rawLocation: string,
  headers?: Record<string, string>
): Promise<Response> {
  const view = VIEW_BINDINGS.get(client);
  if (!view || !view.layerId) {
    const absLocation = new URL(rawLocation, sourceUrlStr).toString();
    return fetch(absLocation, { method: "GET", headers });
  }
  if (!view.baseOrigin) {
    throw new InvalidLayerViewRequestError("missing view base origin");
  }
  const safeHeaders = filterAllowedRedirectHeaders(headers);
  let curSource = new URL(sourceUrlStr);
  let nextRaw = rawLocation;
  for (let hop = 0; hop < MAX_VIEW_DOWNLOAD_REDIRECTS; hop++) {
    let resolved: URL;
    try {
      resolved = new URL(nextRaw, curSource);
    } catch {
      throw new InvalidLayerViewRequestError("invalid redirect location in layer view request");
    }
    validateViewRedirectTarget(view.baseOrigin, curSource, resolved);
    const resp = await fetch(resolved.toString(), {
      method: "GET",
      headers: safeHeaders,
      redirect: "manual",
    });
    if (resp.status === 301 || resp.status === 302 || resp.status === 303 || resp.status === 307 || resp.status === 308) {
      const loc = resp.headers.get("location") || resp.headers.get("Location");
      void resp.body?.cancel().catch(() => {});
      if (!loc) {
        throw new InvalidLayerViewRequestError("redirect without Location header in layer view request");
      }
      curSource = resolved;
      nextRaw = loc;
      continue;
    }
    return resp;
  }
  throw new InvalidLayerViewRequestError("too many redirects in layer view request");
}

export async function fetchViewObjectTargetUrl(
  client: { readonly baseUrl: string },
  rawObjectUrl: string,
  headers?: Record<string, string>
): Promise<Response> {
  return fetchViewObjectRedirect(client, `${client.baseUrl}/`, rawObjectUrl, headers);
}
