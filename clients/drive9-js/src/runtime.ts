import { createHash } from "node:crypto";

import type { Client } from "./client.js";
import { Drive9Error, StatusError } from "./error.js";

export const RuntimeProtocolVersion = "runtime.v1";
const runtimeDurability = "durable_per_operation";
const runtimeWorkspaceConsistency = "per_file_capture";
const runtimeExecutionEffects = "workspace_and_external_no_automatic_replay";
const maxRuntimeArtifactBytes = 1 << 30;
const maxRuntimeEventFrameCharacters = 1 << 20;
const maxRuntimeJSONResponseBytes = 16 << 20;
const maxRuntimeErrorResponseBytes = 1 << 20;

export class RuntimeUnsupportedError extends Drive9Error {
  constructor(message = "Drive9 Runtime is unavailable") {
    super(message);
    this.name = "RuntimeUnsupportedError";
  }
}

export class RuntimeStatusError extends StatusError {
  readonly code?: string;

  constructor(message: string, statusCode: number, code?: string) {
    super(message, statusCode);
    this.name = "RuntimeStatusError";
    this.code = code;
  }
}

export interface RuntimeCapabilities {
  enabled: boolean;
  protocol_version: string;
  durability?: string;
  workspace_consistency?: string;
  execution_effects?: string;
  file_actions?: string[];
  profiles?: string[];
  default_profile?: string;
  supports_arbitrary_command_replay?: boolean;
  supports_sandbox_crud?: boolean;
  supports_mounted_mode?: boolean;
  limits: RuntimeLimits;
}

export interface RuntimeLimits {
  max_tenant_queued_operations: number;
  max_principal_queued_operations: number;
  max_tenant_active_operations: number;
  max_principal_active_operations: number;
  max_workspace_entries: number;
  max_workspace_bytes: number;
  max_single_file_bytes: number;
  max_path_bytes: number;
  max_path_depth: number;
  max_manifest_bytes: number;
  max_read_result_bytes: number;
  max_list_entries: number;
  max_search_file_bytes: number;
  max_search_result_bytes: number;
  max_operation_result_bytes: number;
  max_log_bytes: number;
  max_changed_bytes: number;
  max_execution_seconds: number;
  capture_deadline_seconds: number;
  checkpoint_deadline_seconds: number;
}

const runtimeLimitNames: readonly (keyof RuntimeLimits)[] = [
  "max_tenant_queued_operations",
  "max_principal_queued_operations",
  "max_tenant_active_operations",
  "max_principal_active_operations",
  "max_workspace_entries",
  "max_workspace_bytes",
  "max_single_file_bytes",
  "max_path_bytes",
  "max_path_depth",
  "max_manifest_bytes",
  "max_read_result_bytes",
  "max_list_entries",
  "max_search_file_bytes",
  "max_search_result_bytes",
  "max_operation_result_bytes",
  "max_log_bytes",
  "max_changed_bytes",
  "max_execution_seconds",
  "capture_deadline_seconds",
  "checkpoint_deadline_seconds",
];

function validateRuntimeCapabilities(capabilities: RuntimeCapabilities): void {
  if (!capabilities.enabled || capabilities.protocol_version !== RuntimeProtocolVersion) {
    throw new RuntimeUnsupportedError(
      `incompatible Runtime protocol ${capabilities.protocol_version || "missing"}`
    );
  }
  if (
    capabilities.durability !== runtimeDurability ||
    capabilities.workspace_consistency !== runtimeWorkspaceConsistency ||
    capabilities.execution_effects !== runtimeExecutionEffects ||
    capabilities.supports_arbitrary_command_replay !== false ||
    capabilities.supports_sandbox_crud !== false ||
    capabilities.supports_mounted_mode !== false
  ) {
    throw new RuntimeUnsupportedError("incompatible Runtime effect boundaries");
  }
  if (
    !capabilities.default_profile ||
    !Array.isArray(capabilities.profiles) ||
    !capabilities.profiles.includes(capabilities.default_profile) ||
    capabilities.profiles.some((profile) => typeof profile !== "string" || profile.trim() === "")
  ) {
    throw new RuntimeUnsupportedError("Runtime capabilities have missing or invalid profiles");
  }
  if (
    !capabilities.limits ||
    runtimeLimitNames.some((name) => {
      const value = capabilities.limits[name];
      return !Number.isSafeInteger(value) || value <= 0;
    }) ||
    capabilities.limits.max_log_bytes > maxRuntimeArtifactBytes
  ) {
    throw new RuntimeUnsupportedError("Runtime capabilities have missing or invalid limits");
  }
}

function supportsRuntimeFileAction(capabilities: RuntimeCapabilities, action: string): boolean {
  return capabilities.file_actions?.includes(action) === true;
}

export interface RuntimeWorkspaceInput {
  client_scope_key: string;
  source: { root: string };
}

export interface RuntimeExecutionRequest {
  workspace_ref?: string;
  workspace?: RuntimeWorkspaceInput;
  profile?: string;
  execution: {
    argv?: string[];
    shell?: string;
    working_directory?: string;
    environment?: Record<string, string>;
    timeout_seconds?: number;
  };
}

export interface RuntimeFileOperationRequest {
  workspace_ref?: string;
  workspace?: RuntimeWorkspaceInput;
  profile?: string;
  operation: {
    action: "read" | "write" | "edit" | "delete" | "list" | "find" | "grep";
    path: string;
    data_base64?: string;
    expected_sha256?: string;
    pattern?: string;
    limit?: number;
  };
}

export interface RuntimeOperation {
  id: string;
  kind: "execution" | "file_operation";
  workspace_ref: string;
  profile: string;
  workspace_seq: number;
  state: "queued" | "preparing" | "running" | "finalizing" | "succeeded" | "failed" | "canceled" | "timed_out" | "outcome_unknown";
  attempt: number;
  cancel_requested: boolean;
  recovery_required: boolean;
  cleanup_pending: boolean;
  result?: unknown;
  usage: RuntimeUsage;
  error_code?: string;
  error_message?: string;
  checkpoint_id?: string;
  log_artifact_id?: string;
  created_at: string;
  updated_at: string;
  completed_at?: string;
}

export interface RuntimeUsage {
  runtime_ms: number;
  bytes_scanned: number;
  entries_scanned: number;
  changed_bytes: number;
  log_bytes: number;
}

export interface RuntimeEvent {
  operation_id: string;
  cursor: number;
  kind: string;
  payload?: unknown;
  created_at: string;
}

export interface RuntimeRecovery {
  recovery_id: string;
  operation_id: string;
  workspace_ref: string;
  state: string;
  action: "reconcile" | "discard_uncommitted";
  error?: string;
  created_at: string;
  updated_at: string;
}

type RuntimeCollection = "executions" | "file-operations";

async function runtimeError(response: Response): Promise<never> {
  let message = `HTTP ${response.status}`;
  let code: string | undefined;
  try {
    const body = await readRuntimeJSON<{ error?: string; message?: string }>(response, maxRuntimeErrorResponseBytes);
    code = body.error;
    message = body.message || body.error || message;
  } catch {
    // Preserve the status-only error when the response is not JSON.
  }
  throw new RuntimeStatusError(message, response.status, code);
}

async function runtimeJSON<T>(response: Response): Promise<T> {
  if (!response.ok) return runtimeError(response);
  return readRuntimeJSON<T>(response, maxRuntimeJSONResponseBytes);
}

async function readRuntimeJSON<T>(response: Response, maxBytes: number): Promise<T> {
  const declared = response.headers.get("Content-Length");
  if (declared !== null) {
    const declaredBytes = Number(declared);
    if (!Number.isSafeInteger(declaredBytes) || declaredBytes < 0) {
      throw new Drive9Error("Runtime response has invalid Content-Length");
    }
    if (declaredBytes > maxBytes) {
      throw new Drive9Error(`Runtime response exceeds ${maxBytes} bytes`);
    }
  }
  const reader = response.body?.getReader();
  if (!reader) throw new Drive9Error("Runtime response has no body");
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > maxBytes) {
      await reader.cancel();
      throw new Drive9Error(`Runtime response exceeds ${maxBytes} bytes`);
    }
    chunks.push(value);
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(new TextDecoder().decode(body)) as T;
}

export async function runtimeCapabilities(client: Client): Promise<RuntimeCapabilities> {
  const response = await fetch(`${client.baseUrl}/v1/runtime/capabilities`, { headers: client.authHeaders() });
  if (response.status === 404 || response.status === 503) {
    throw new RuntimeUnsupportedError();
  }
  const capabilities = await runtimeJSON<RuntimeCapabilities>(response);
  validateRuntimeCapabilities(capabilities);
  return capabilities;
}

async function submitRuntimeOperation<T>(client: Client, collection: RuntimeCollection, idempotencyKey: string, input: T, requiredFileAction?: string): Promise<RuntimeOperation> {
  const capabilities = await runtimeCapabilities(client);
  if (requiredFileAction && !supportsRuntimeFileAction(capabilities, requiredFileAction)) {
    throw new RuntimeUnsupportedError(`Runtime file action ${JSON.stringify(requiredFileAction)} is unavailable`);
  }
  if (!idempotencyKey.trim()) throw new Drive9Error("Runtime Idempotency-Key is required");
  return runtimeJSON<RuntimeOperation>(
    await fetch(`${client.baseUrl}/v1/runtime/${collection}`, {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json", "Idempotency-Key": idempotencyKey }),
      body: JSON.stringify(input),
    })
  );
}

export function submitRuntimeExecution(client: Client, idempotencyKey: string, input: RuntimeExecutionRequest): Promise<RuntimeOperation> {
  return submitRuntimeOperation(client, "executions", idempotencyKey, input);
}

export function submitRuntimeFileOperation(client: Client, idempotencyKey: string, input: RuntimeFileOperationRequest): Promise<RuntimeOperation> {
  return submitRuntimeOperation(client, "file-operations", idempotencyKey, input, input.operation.action);
}

export function getRuntimeOperation(client: Client, collection: RuntimeCollection, id: string): Promise<RuntimeOperation> {
  return fetch(`${client.baseUrl}/v1/runtime/${collection}/${encodeURIComponent(id)}`, { headers: client.authHeaders() }).then(runtimeJSON<RuntimeOperation>);
}

export function cancelRuntimeOperation(client: Client, collection: RuntimeCollection, id: string): Promise<RuntimeOperation> {
  return fetch(`${client.baseUrl}/v1/runtime/${collection}/${encodeURIComponent(id)}/cancel`, {
    method: "POST",
    headers: client.authHeaders({ "Content-Type": "application/json" }),
    body: "{}",
  }).then(runtimeJSON<RuntimeOperation>);
}

export async function recoverRuntimeOperation(client: Client, collection: RuntimeCollection, id: string, idempotencyKey: string, action: RuntimeRecovery["action"]): Promise<RuntimeRecovery> {
  await runtimeCapabilities(client);
  if (!idempotencyKey.trim()) throw new Drive9Error("Runtime recovery Idempotency-Key is required");
  return runtimeJSON<RuntimeRecovery>(
    await fetch(`${client.baseUrl}/v1/runtime/${collection}/${encodeURIComponent(id)}/recover`, {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json", "Idempotency-Key": idempotencyKey }),
      body: JSON.stringify({ action }),
    })
  );
}

export function getRuntimeRecovery(client: Client, id: string): Promise<RuntimeRecovery> {
  return fetch(`${client.baseUrl}/v1/runtime/recoveries/${encodeURIComponent(id)}`, { headers: client.authHeaders() }).then(runtimeJSON<RuntimeRecovery>);
}

function runtimeTerminal(operation: RuntimeOperation): boolean {
  return ["succeeded", "failed", "canceled", "timed_out", "outcome_unknown"].includes(operation.state);
}

async function readEventWindow(
  response: Response,
  cursor: { value: number },
  onEvent: (event: RuntimeEvent) => void | Promise<void>
): Promise<void> {
  if (!response.ok) return runtimeError(response);
  if (!response.body) throw new Drive9Error("Runtime event response has no body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value, { stream: !done });
    for (;;) {
      const separator = buffer.indexOf("\n\n");
      if (separator < 0) break;
      const frame = buffer.slice(0, separator);
      buffer = buffer.slice(separator + 2);
      if (frame.length > maxRuntimeEventFrameCharacters) {
        throw new Drive9Error(`Runtime event frame exceeds ${maxRuntimeEventFrameCharacters} characters`);
      }
      const line = frame.split("\n").find((entry) => entry.startsWith("data: "));
      if (!line) continue;
      const event = JSON.parse(line.slice(6)) as RuntimeEvent;
      if (!Number.isSafeInteger(event.cursor) || event.cursor < 1) {
        throw new Drive9Error(`invalid Runtime event cursor ${String(event.cursor)}`);
      }
      if (event.cursor <= cursor.value) continue;
      await onEvent(event);
      cursor.value = event.cursor;
    }
    if (buffer.length > maxRuntimeEventFrameCharacters) {
      throw new Drive9Error(`Runtime event frame exceeds ${maxRuntimeEventFrameCharacters} characters`);
    }
    if (done) return;
  }
}

function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason || new Error("aborted"));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    if (signal?.aborted) {
      onAbort();
      return;
    }
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export async function watchRuntimeEvents(client: Client, collection: RuntimeCollection, id: string, after: number, onEvent: (event: RuntimeEvent) => void | Promise<void>, signal?: AbortSignal): Promise<void> {
  if (!Number.isSafeInteger(after) || after < 0) {
    throw new Drive9Error(`Runtime event cursor must be a non-negative safe integer`);
  }
  const cursor = { value: after };
  for (;;) {
    try {
      const query = cursor.value > 0 ? `?after=${cursor.value}` : "";
      await readEventWindow(
        await fetch(`${client.baseUrl}/v1/runtime/${collection}/${encodeURIComponent(id)}/events${query}`, { headers: client.authHeaders(), signal }),
        cursor,
        onEvent
      );
      const operation = await getRuntimeOperation(client, collection, id);
      if (runtimeTerminal(operation)) return;
    } catch (error) {
      if (!retryableRuntimeWatchError(error, signal)) throw error;
    }
    await abortableDelay(250, signal);
  }
}

function retryableRuntimeWatchError(error: unknown, signal?: AbortSignal): boolean {
  if (signal?.aborted) return false;
  if (error instanceof RuntimeStatusError) return error.statusCode === 429 || error.statusCode >= 500;
  if (error instanceof TypeError) return true;
  return error instanceof DOMException && error.name !== "AbortError";
}

export async function downloadRuntimeArtifact(client: Client, id: string): Promise<Uint8Array> {
  const response = await fetch(`${client.baseUrl}/v1/runtime/artifacts/${encodeURIComponent(id)}`, { headers: client.authHeaders() });
  if (!response.ok) return runtimeError(response);
  const expected = response.headers.get("X-Content-SHA256")?.toLowerCase();
  if (!expected) throw new Drive9Error("Runtime artifact response omitted X-Content-SHA256");
  const declaredHeader = response.headers.get("Content-Length");
  if (declaredHeader === null) throw new Drive9Error("Runtime artifact response omitted Content-Length");
  const declaredLength = Number(declaredHeader);
  if (!Number.isSafeInteger(declaredLength) || declaredLength < 0) {
    throw new Drive9Error("Runtime artifact response has invalid Content-Length");
  }
  if (declaredLength > maxRuntimeArtifactBytes) {
    throw new Drive9Error(`Runtime artifact exceeds ${maxRuntimeArtifactBytes} bytes`);
  }
  const reader = response.body?.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  if (reader) {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxRuntimeArtifactBytes) {
        await reader.cancel();
        throw new Drive9Error(`Runtime artifact exceeds ${maxRuntimeArtifactBytes} bytes`);
      }
      chunks.push(value);
    }
  }
  const content = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    content.set(chunk, offset);
    offset += chunk.byteLength;
  }
  if (size !== declaredLength) {
    throw new Drive9Error(`Runtime artifact Content-Length mismatch: declared ${declaredLength}, read ${size}`);
  }
  const actual = createHash("sha256").update(content).digest("hex");
  if (actual !== expected) throw new Drive9Error("Runtime artifact checksum mismatch");
  return content;
}
