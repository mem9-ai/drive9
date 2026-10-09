const MAX_RUNTIME_FRAME_BYTES = 1 << 20;
const RUNTIME_CANCEL_TIMEOUT_MS = 20_000;

export type RuntimePersistence = "full_root" | "workspace";

export interface RuntimeWorkspace {
  root: string;
  layer_id?: string;
  read_only?: boolean;
  persistence?: RuntimePersistence;
}

export interface ExecRequest {
  argv: string[];
  cwd?: string;
  env?: Record<string, string>;
  workspace: RuntimeWorkspace;
  timeout_ms?: number;
}

export interface ExecStarted {
  executionId: string;
}

export interface ExecOptions {
  signal?: AbortSignal;
  onStarted?: (event: ExecStarted) => void | Promise<void>;
  onStdout?: (chunk: Uint8Array) => void | Promise<void>;
  onStderr?: (chunk: Uint8Array) => void | Promise<void>;
}

export interface ExecResult {
  executionId: string;
  exitCode: number;
  durationMs: number;
}

export interface RuntimeCapabilities {
  version: number;
  streaming: boolean;
  separate_stdout_stderr: boolean;
  cancel: boolean;
  cancel_scope: string;
  detached: boolean;
  replay: boolean;
  providers: RuntimeProviderCapability[];
}

export interface RuntimeProviderCapability {
  provider: string;
  profiles: string[];
  candidates?: RuntimeCandidateCapability[];
}

export interface RuntimeCandidateCapability {
  profile: string;
  execution_class: string;
  persistence: RuntimePersistence;
  rootfs: RuntimeRootFSIdentity;
  capabilities: Record<string, boolean>;
  production_eligible: boolean;
  bounded_selection_eligible: boolean;
}

export interface RuntimeRootFSIdentity {
  driver: string;
  capability_version: string;
  config_hash: string;
  lower_image_digest: string;
}

export class RuntimeExecutionError extends Error {
  readonly executionId: string;
  readonly code: string;

  constructor(code: string, message: string, executionId = "", options?: ErrorOptions) {
    super(message || `runtime execution failed: ${code}`, options);
    this.name = "RuntimeExecutionError";
    this.code = code;
    this.executionId = executionId;
  }
}

export class RuntimeStartError extends RuntimeExecutionError {
  constructor(code: string, message: string, executionId = "", options?: ErrorOptions) {
    super(code, message, executionId, options);
    this.name = "RuntimeStartError";
  }
}

export class RuntimeTimeoutError extends RuntimeExecutionError {
  constructor(message: string, executionId: string) {
    super("timed_out", message, executionId);
    this.name = "RuntimeTimeoutError";
  }
}

export class RuntimeCanceledError extends RuntimeExecutionError {
  constructor(message: string, executionId: string) {
    super("canceled", message, executionId);
    this.name = "RuntimeCanceledError";
  }
}

export class RuntimeOutcomeUnknownError extends Error {
  readonly executionId: string;
  override readonly cause: unknown;

  constructor(message: string, executionId = "", cause?: unknown) {
    super(message, { cause });
    this.name = "RuntimeOutcomeUnknownError";
    this.executionId = executionId;
    this.cause = cause;
  }
}

export function isRuntimeOutcomeUnknown(error: unknown): boolean {
  return error instanceof RuntimeOutcomeUnknownError;
}

interface RuntimeClient {
  readonly baseUrl: string;
  authHeaders(init?: Record<string, string>): Record<string, string>;
}

interface WireFrame {
  type?: unknown;
  execution_id?: unknown;
  data_base64?: unknown;
  exit_code?: unknown;
  duration_ms?: unknown;
  error_code?: unknown;
  message?: unknown;
}

export async function exec(client: RuntimeClient, input: ExecRequest, options: ExecOptions = {}): Promise<ExecResult> {
  let response: Response;
  try {
    response = await fetch(`${client.baseUrl}/v1/runtime/exec`, {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json", Accept: "application/x-ndjson" }),
      body: JSON.stringify(input),
      signal: options.signal,
      redirect: "error",
    });
  } catch (error) {
    throw unknownOutcome("runtime request ended before a terminal frame", "", error);
  }
  if (!response.ok) {
    const failure = await readRuntimeError(response);
    throw new RuntimeStartError(failure.code || "http_error", failure.message || `HTTP ${response.status}`);
  }
  const contentType = response.headers.get("content-type")?.split(";", 1)[0].trim();
  if (contentType !== "application/x-ndjson" || !response.body) {
    throw unknownOutcome(`unexpected runtime response content type ${JSON.stringify(contentType || "")}`);
  }

  let executionId = "";
  try {
    for await (const line of runtimeLines(response.body, options.signal)) {
      if (line.trim() === "") continue;
      const wire = parseWireFrame(line);
      const kind = requiredString(wire.type, "frame type");
      validateFrame(wire, kind, executionId);
      if (kind === "started") {
        executionId = wire.execution_id as string;
        try {
          await options.onStarted?.({ executionId });
        } catch (error) {
          await cancelAfterLostObservation(client, executionId);
          throw unknownOutcome("runtime started callback failed", executionId, error);
        }
        continue;
      }
      if (kind === "stdout" || kind === "stderr") {
        const data = decodeBase64(wire.data_base64 as string);
        try {
          if (kind === "stdout") await options.onStdout?.(data);
          else await options.onStderr?.(data);
        } catch (error) {
          await cancelAfterLostObservation(client, executionId);
          throw unknownOutcome(`runtime ${kind} callback failed`, executionId, error);
        }
        continue;
      }
      if (kind === "exit") {
        return {
          executionId,
          exitCode: wire.exit_code as number,
          durationMs: wire.duration_ms as number,
        };
      }
      if (kind === "error") {
        throw terminalError(wire);
      }
    }
  } catch (error) {
    if (error instanceof RuntimeExecutionError || error instanceof RuntimeOutcomeUnknownError) throw error;
    if (options.signal?.aborted && executionId) {
      throw await runtimeAbortError(client, executionId, error);
    }
    throw unknownOutcome("runtime stream ended without a valid terminal frame", executionId, error);
  }
  if (options.signal?.aborted && executionId) {
    throw await runtimeAbortError(client, executionId, options.signal.reason);
  }
  throw unknownOutcome("runtime stream ended without a terminal frame", executionId);
}

export async function getRuntimeCapabilities(client: RuntimeClient, signal?: AbortSignal): Promise<RuntimeCapabilities> {
  const response = await fetch(`${client.baseUrl}/v1/runtime/capabilities`, {
    headers: client.authHeaders(),
    signal,
    redirect: "error",
  });
  if (!response.ok) {
    const failure = await readRuntimeError(response);
    throw new RuntimeExecutionError(failure.code || "http_error", failure.message || `HTTP ${response.status}`);
  }
  const value = await readRuntimeJSON(response);
  if (!isObject(value)) throw new Error("runtime capabilities must be an object");
  const allowed = new Set(["version", "streaming", "separate_stdout_stderr", "cancel", "cancel_scope", "detached", "replay", "providers"]);
  rejectUnknownFields(value, allowed, "runtime capabilities");
  const capabilities = value as unknown as RuntimeCapabilities;
  if (capabilities.version !== 1 || capabilities.streaming !== true || capabilities.separate_stdout_stderr !== true ||
      typeof capabilities.cancel !== "boolean" || typeof capabilities.cancel_scope !== "string" ||
      capabilities.detached !== false || capabilities.replay !== false || !Array.isArray(capabilities.providers)) {
    throw new Error("server does not support the Runtime exec protocol");
  }
  if (capabilities.cancel && capabilities.cancel_scope !== "active_execution") {
    throw new Error("server advertises an unsupported Runtime cancel scope");
  }
  for (const provider of capabilities.providers) {
    if (!isObject(provider) || Object.keys(provider).some((key) => key !== "provider" && key !== "profiles" && key !== "candidates") ||
        !nonemptyString(provider.provider) || !Array.isArray(provider.profiles) || provider.profiles.some((profile) => !nonemptyString(profile))) {
      throw new Error("server returned invalid Runtime provider capabilities");
    }
    if (provider.candidates !== undefined) {
      if (!Array.isArray(provider.candidates)) throw new Error("server returned invalid Runtime candidate capabilities");
      const candidateProfiles = new Set<string>();
      for (const candidate of provider.candidates) {
        if (!validRuntimeCandidateCapability(candidate) || !provider.profiles.includes(candidate.profile) || candidateProfiles.has(candidate.profile)) {
          throw new Error("server returned invalid Runtime candidate capabilities");
        }
        candidateProfiles.add(candidate.profile);
      }
    }
  }
  const providerNames = capabilities.providers.map((provider) => provider.provider);
  if (new Set(providerNames).size !== providerNames.length || capabilities.providers.some((provider) => new Set(provider.profiles).size !== provider.profiles.length)) {
    throw new Error("server returned duplicate Runtime provider capabilities");
  }
  return capabilities;
}

function validRuntimeCandidateCapability(value: unknown): value is RuntimeCandidateCapability {
  if (!isObject(value)) return false;
  const allowed = new Set(["profile", "execution_class", "persistence", "rootfs", "capabilities", "production_eligible", "bounded_selection_eligible"]);
  if (Object.keys(value).some((key) => !allowed.has(key)) || !nonemptyString(value.profile) || !nonemptyString(value.execution_class) ||
      typeof value.production_eligible !== "boolean" || typeof value.bounded_selection_eligible !== "boolean" ||
      !isObject(value.rootfs) || !isObject(value.capabilities)) return false;
  const rootfsAllowed = new Set(["driver", "capability_version", "config_hash", "lower_image_digest"]);
  if (Object.keys(value.rootfs).some((key) => !rootfsAllowed.has(key)) || value.execution_class !== "linux-full" ||
      typeof value.rootfs.config_hash !== "string" || !/^[0-9a-f]{64}$/.test(value.rootfs.config_hash) ||
      typeof value.rootfs.lower_image_digest !== "string" || !/^sha256:[0-9a-f]{64}$/.test(value.rootfs.lower_image_digest) ||
      Object.values(value.capabilities).some((capability) => typeof capability !== "boolean") ||
      value.bounded_selection_eligible && !value.production_eligible) {
    return false;
  }
  if (value.capabilities["workspace_persistence"] !== true) return false;
  if (value.persistence === "full_root") {
    return value.rootfs.driver === "user-union" && value.rootfs.capability_version === "drive9_rootfs.user_union.extent.v4" &&
      value.capabilities["full_root_persistence"] === true && value.capabilities["drive9.extent_xattr.v1"] === true;
  }
  if (value.persistence === "workspace") {
    return value.rootfs.driver === "workspace-mount" && value.rootfs.capability_version === "drive9_workspace.mount.v1" &&
      value.capabilities["full_root_persistence"] !== true && value.capabilities["drive9.extent_xattr.v1"] !== true;
  }
  return false;
}

export async function cancelRuntimeExecution(client: RuntimeClient, executionId: string, signal?: AbortSignal): Promise<void> {
  if (!validExecutionId(executionId)) throw new Error("invalid runtime execution id");
  let response: Response;
  try {
    response = await fetch(`${client.baseUrl}/v1/runtime/executions/${encodeURIComponent(executionId)}/cancel`, {
      method: "POST",
      headers: client.authHeaders(),
      signal,
      redirect: "error",
    });
  } catch (error) {
    throw unknownOutcome("runtime cancellation outcome is unknown", executionId, error);
  }
  if (!response.ok) {
    const failure = await readRuntimeError(response);
    if (failure.code === "outcome_unknown") {
      throw unknownOutcome("runtime cancellation outcome is unknown", executionId);
    }
    throw new RuntimeExecutionError(failure.code || "http_error", failure.message || `HTTP ${response.status}`, executionId);
  }
  const body = await readRuntimeJSON(response);
  if (!isObject(body) || body.canceled !== true || Object.keys(body).some((key) => key !== "canceled")) {
    throw unknownOutcome("server did not confirm runtime cancellation", executionId);
  }
}

function terminalError(wire: WireFrame): Error {
  const executionId = wire.execution_id === undefined ? "" : wire.execution_id as string;
  const code = wire.error_code as string;
  const message = wire.message as string;
  if (code === "invalid_request" || code === "unavailable" || code === "start_failed") {
    return new RuntimeStartError(code, message, executionId);
  }
  if (code === "timed_out") return new RuntimeTimeoutError(message, executionId);
  if (code === "canceled") return new RuntimeCanceledError(message, executionId);
  if (code === "outcome_unknown") {
    return unknownOutcome(message, executionId, new RuntimeExecutionError(code, message, executionId));
  }
  return new RuntimeExecutionError(code, message, executionId);
}

function validateFrame(wire: WireFrame, kind: string, executionId: string): void {
  if (kind === "started") {
    if (executionId || !validExecutionId(wire.execution_id) || hasOutputOrTerminalFields(wire)) {
      throw new Error("invalid runtime started frame");
    }
    return;
  }
  if (kind === "stdout" || kind === "stderr") {
    if (!executionId || wire.execution_id !== executionId || !nonemptyString(wire.data_base64) ||
        wire.exit_code !== undefined || wire.duration_ms !== undefined || wire.error_code !== undefined || wire.message !== undefined) {
      throw new Error("invalid runtime output frame");
    }
    return;
  }
  if (kind === "exit") {
    if (!executionId || wire.execution_id !== executionId || !Number.isSafeInteger(wire.exit_code) ||
        (wire.exit_code as number) < 0 || (wire.exit_code as number) > 255 || !Number.isSafeInteger(wire.duration_ms) ||
        (wire.duration_ms as number) < 0 || wire.data_base64 !== undefined ||
        wire.error_code !== undefined || wire.message !== undefined) {
      throw new Error("invalid runtime exit frame");
    }
    return;
  }
  if (kind === "error") {
    const id = wire.execution_id === undefined ? "" : requiredString(wire.execution_id, "execution id");
    if ((executionId && id !== executionId) || (!executionId && id && !validExecutionId(id)) ||
        !nonemptyString(wire.error_code) || !nonemptyString(wire.message) || wire.data_base64 !== undefined ||
        wire.exit_code !== undefined || wire.duration_ms !== undefined) {
      throw new Error("invalid runtime error frame");
    }
    return;
  }
  throw new Error(`unknown runtime frame type ${JSON.stringify(kind)}`);
}

async function cancelAfterLostObservation(client: RuntimeClient, executionId: string): Promise<void> {
  if (!executionId) return;
  try {
    await cancelRuntimeExecution(client, executionId, AbortSignal.timeout(RUNTIME_CANCEL_TIMEOUT_MS));
  } catch {
    // Callback failure already makes output delivery incomplete. Cancellation
    // here is best-effort cleanup and cannot upgrade the result from unknown.
  }
}

async function runtimeAbortError(client: RuntimeClient, executionId: string, cause: unknown): Promise<Error> {
  try {
    await cancelRuntimeExecution(client, executionId, AbortSignal.timeout(RUNTIME_CANCEL_TIMEOUT_MS));
    return new RuntimeCanceledError("runtime execution was canceled and stopped", executionId);
  } catch (cancelError) {
    return unknownOutcome("runtime cancellation outcome is unknown", executionId, new AggregateError([cause, cancelError]));
  }
}

async function* runtimeLines(stream: ReadableStream<Uint8Array>, signal?: AbortSignal): AsyncGenerator<string> {
  const reader = stream.getReader();
  let pending: Uint8Array = new Uint8Array(0);
  let complete = false;
  try {
    while (true) {
      const { value, done } = await readRuntimeChunk(reader, signal);
      if (done) {
        complete = true;
        break;
      }
      if (!value) continue;
      pending = concatBytes(pending, value);
      while (true) {
        const newline = pending.indexOf(10);
        if (newline < 0) break;
        if (newline > MAX_RUNTIME_FRAME_BYTES) throw new Error(`runtime frame exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`);
        yield new TextDecoder("utf-8", { fatal: true }).decode(pending.subarray(0, newline));
        pending = pending.slice(newline + 1);
      }
      if (pending.byteLength > MAX_RUNTIME_FRAME_BYTES) throw new Error(`runtime frame exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`);
    }
    if (pending.byteLength > 0) {
      yield new TextDecoder("utf-8", { fatal: true }).decode(pending);
    }
  } finally {
    if (!complete) void reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

function readRuntimeChunk(reader: ReadableStreamDefaultReader<Uint8Array>, signal?: AbortSignal): Promise<ReadableStreamReadResult<Uint8Array>> {
  if (!signal) return reader.read();
  if (signal.aborted) return Promise.reject(signal.reason ?? new Error("runtime observation aborted"));
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason ?? new Error("runtime observation aborted"));
    signal.addEventListener("abort", onAbort, { once: true });
    reader.read().then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function concatBytes(left: Uint8Array, right: Uint8Array): Uint8Array {
  if (left.byteLength === 0) return right.slice();
  const joined = new Uint8Array(left.byteLength + right.byteLength);
  joined.set(left);
  joined.set(right, left.byteLength);
  return joined;
}

function parseWireFrame(line: string): WireFrame {
  const parsed = JSON.parse(line) as unknown;
  if (!isObject(parsed)) throw new Error("runtime frame must be an object");
  rejectUnknownFields(parsed, new Set(["type", "execution_id", "data_base64", "exit_code", "duration_ms", "error_code", "message"]), "runtime frame");
  return parsed as WireFrame;
}

function decodeBase64(value: string): Uint8Array {
  if (value === "" || value.length % 4 !== 0 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new Error("invalid runtime output encoding");
  }
  const decoded = Buffer.from(value, "base64");
  if (decoded.toString("base64") !== value) throw new Error("invalid runtime output encoding");
  return new Uint8Array(decoded);
}

function hasOutputOrTerminalFields(frame: WireFrame): boolean {
  return frame.data_base64 !== undefined || frame.exit_code !== undefined || frame.duration_ms !== undefined ||
    frame.error_code !== undefined || frame.message !== undefined;
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string" || value === "") throw new Error(`invalid ${name}`);
  return value;
}

function nonemptyString(value: unknown): value is string {
  return typeof value === "string" && value !== "";
}

function validExecutionId(value: unknown): value is string {
  return typeof value === "string" && value.length >= 8 && value.length <= 80 && /^[A-Za-z0-9_-]+$/.test(value);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function rejectUnknownFields(value: Record<string, unknown>, allowed: Set<string>, name: string): void {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new Error(`unknown ${name} field ${key}`);
  }
}

async function readRuntimeJSON(response: Response): Promise<unknown> {
  if (!response.body) throw new Error("runtime response body is missing");
  const reader = response.body.getReader();
  let bytes: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let complete = false;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        complete = true;
        break;
      }
      if (!value) continue;
      if (bytes.byteLength + value.byteLength > MAX_RUNTIME_FRAME_BYTES) {
        throw new Error(`runtime response exceeds ${MAX_RUNTIME_FRAME_BYTES} bytes`);
      }
      bytes = concatBytes(bytes, value);
    }
  } finally {
    if (!complete) void reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown;
}

async function readRuntimeError(response: Response): Promise<{ code: string; message: string }> {
  try {
    const value = await readRuntimeJSON(response);
    if (isObject(value)) {
      return {
        code: typeof value.code === "string" ? value.code : "",
        message: typeof value.error === "string" ? value.error : "",
      };
    }
  } catch {
    // Return the bounded generic status below.
  }
  return { code: "", message: `HTTP ${response.status}` };
}

function unknownOutcome(message: string, executionId = "", cause?: unknown): RuntimeOutcomeUnknownError {
  return new RuntimeOutcomeUnknownError(message, executionId, cause);
}
