export interface FileInfo {
  name: string;
  size: number;
  isDir: boolean;
  /** UTC epoch seconds from server; converted to Date by the client. */
  mtime?: Date;
  /** POSIX mode bits (e.g. 0o120777 for a symlink). Absent on old servers. */
  mode?: number;
  /** True when the server returned a mode header/value. */
  hasMode?: boolean;
}

export interface StatResult {
  size: number;
  isDir: boolean;
  revision: number;
  mtime?: Date;
  mode?: number;
  hasMode?: boolean;
  resource_id?: string;
  nlink?: number;
}

export interface StatMetadataResult {
  size: number;
  isdir: boolean;
  resource_id?: string;
  nlink?: number;
  revision: number;
  mtime?: number;
  content_type: string;
  semantic_text: string;
  tags: Record<string, string>;
  degraded?: boolean;
}

export interface SearchResult {
  path: string;
  name: string;
  /** Mirrors the snake_case field name used by the server API. */
  size_bytes: number;
  score?: number;
}

export const MaxBatchStatPaths = 256;
export const MaxBatchReadSmallPaths = 128;

export interface BatchStatResult {
  path: string;
  status: number;
  error?: string;
  size?: number;
  isDir: boolean;
  revision?: number;
  mtime?: number;
  mode?: number;
  hasMode?: boolean;
  resource_id?: string;
  nlink?: number;
}

export interface BatchReadSmallResult {
  path: string;
  status: number;
  error?: string;
  data?: Uint8Array;
  size?: number;
  revision?: number;
  mtime?: number;
}

export interface WriteOptions {
  expectedRevision?: number;
  tags?: Record<string, string>;
  description?: string;
}

export interface TenantStatus {
  status?: string;
  inline_threshold?: number;
  max_upload_bytes?: number;
  [key: string]: unknown;
}

export interface PartURL {
  number: number;
  url: string;
  size: number;
  checksum_sha256?: string;
  checksum_crc32c?: string;
  headers?: Record<string, unknown>;
  expires_at?: string;
}

export interface UploadPlan {
  upload_id: string;
  part_size: number;
  parts: PartURL[];
}

export interface PatchPartURL {
  number: number;
  url: string;
  size: number;
  headers?: Record<string, unknown>;
  expires_at?: string;
  read_url?: string;
  read_headers?: Record<string, unknown>;
}

export interface PatchPlan {
  upload_id: string;
  part_size: number;
  upload_parts: PatchPartURL[];
  copied_parts: number[];
}

export interface UploadMeta {
  upload_id: string;
  parts_total: number;
  status: string;
  expires_at: string;
}

export interface UploadSummary {
  type: "upload";
  mode: "direct_put" | "multipart_v1" | "multipart_v2";
  started_at: string;
  finished_at: string;
  elapsed_seconds: number;
  remote_path: string;
  total_bytes: number;
  part_size_bytes?: number;
  total_parts?: number;
  uploaded_parts?: number;
}

export interface FSScopeGrant {
  prefix: string;
  ops: string[];
}

export interface IssueScopedTokenRequest {
  subject?: string;
  ttl_seconds: number;
  scopes: FSScopeGrant[];
}

export interface IssueScopedTokenResponse {
  token: string;
  token_id?: string;
  subject?: string;
  scope_kind: string;
  expires_at?: string;
  scopes: FSScopeGrant[];
}

export interface RevokeScopedTokenByAPIKeyRequest {
  api_key: string;
}

export interface VaultSecret {
  name: string;
  secret_type: string;
  revision: number;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface VaultTokenIssueResponse {
  token: string;
  token_id: string;
  expires_at: string;
}

export interface VaultGrantIssueRequest {
  agent: string;
  scope: string[];
  perm: string;
  ttl_seconds: number;
  label_hint?: string;
}

export interface VaultGrantIssueResponse {
  token: string;
  grant_id: string;
  expires_at: string;
  scope: string[];
  perm: string;
}

export interface VaultGrantRevokeRequest {
  revoked_by?: string;
  reason?: string;
}

export interface VaultAuditEvent {
  event_id: string;
  event_type: string;
  timestamp: string;
  token_id?: string;
  agent_id?: string;
  task_id?: string;
  secret_name?: string;
  field_name?: string;
  adapter?: string;
  detail?: unknown;
}

export interface CompletePart {
  number: number;
  etag: string;
}

export interface PresignedPart {
  number: number;
  url: string;
  size: number;
  headers?: Record<string, unknown>;
}

export interface UploadPlanV2 {
  upload_id: string;
  key: string;
  part_size: number;
  total_parts: number;
  expires_at?: string;
  resumable?: boolean;
  checksum_contract?: {
    supported?: string[];
    required?: boolean;
  };
}

export interface ChangeEvent {
  seq: number;
  path: string;
  op: string;
  actor?: string;
  ts: number;
}

export interface ResetEvent {
  seq: number;
  reason: string;
  path?: string;
  op?: string;
  actor?: string;
}

export interface HeartbeatEvent {
  seq: number;
}

export type EventHandler = (change: ChangeEvent | undefined, reset: ResetEvent | undefined) => void | Promise<void>;

export interface EventLifecycle {
  onDisconnected?: (err: Error | undefined) => void | Promise<void>;
  onCurrent?: (seq: number) => void | Promise<void>;
}

export interface WatchEventsOptions {
  actor?: string;
  signal?: AbortSignal;
  initialSince?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
}

export interface FSLayerCreateRequest {
  layer_id?: string;
  base_root_path: string;
  name?: string;
  tags?: Record<string, string>;
  durability_mode?: string;
  actor_id?: string;
}

export interface FSLayer {
  layer_id: string;
  base_root_path: string;
  name: string;
  tags?: Record<string, string>;
  state: string;
  durability_mode: string;
  actor_id: string;
  durable_seq: number;
  created_at: string;
  updated_at: string;
  sealed_at?: string;
}

export type FSLayerEntryOp = "upsert" | "whiteout" | "mkdir" | "symlink" | "chmod" | "rename";
export type FSLayerEntryKind = "file" | "dir" | "symlink";

export interface FSLayerEntry {
  layer_id: string;
  path: string;
  parent_path: string;
  name: string;
  op: FSLayerEntryOp;
  kind: FSLayerEntryKind;
  base_inode_id: string;
  base_revision: number;
  storage_type: string;
  storage_ref: string;
  storage_ref_hash: string;
  storage_encryption_mode: string;
  storage_encryption_key_id: string;
  checksum_sha256: string;
  size_bytes: number;
  mode: number;
  content?: string;
  content_text?: string;
  entry_seq: number;
  created_at: string;
  updated_at: string;
}

export interface FSLayerEntryRequest {
  path: string;
  op?: FSLayerEntryOp;
  kind?: FSLayerEntryKind;
  base_inode_id?: string;
  base_revision?: number;
  storage_type?: string;
  storage_ref?: string;
  storage_ref_hash?: string;
  storage_encryption_mode?: string;
  storage_encryption_key_id?: string;
  content?: Uint8Array | string;
  content_type?: string;
  content_text?: string;
  checksum_sha256?: string;
  size_bytes?: number;
  mode?: number;
}

export interface FSLayerCommitConflict {
  path: string;
  reason: string;
  base_revision?: number;
  want_revision?: number;
}

export interface FSLayerCommit {
  status: string;
  layer_id: string;
  applied?: number;
  conflicts?: FSLayerCommitConflict[];
}

export interface FSLayerCheckpointRequest {
  checkpoint_id?: string;
  label?: string;
}

export interface FSLayerCheckpoint {
  checkpoint_id: string;
  layer_id: string;
  durable_seq: number;
  label: string;
  created_at: string;
}

export interface FSLayerEvent {
  event_id: string;
  layer_id: string;
  seq: number;
  actor_id: string;
  op: string;
  path: string;
  created_at: string;
}

export interface GitWorkspaceRequest {
  root_path: string;
  repo_url: string;
  remote_name?: string;
  branch_name?: string;
  base_commit?: string;
  head_commit?: string;
  mode?: string;
  workspace_kind?: string;
  common_workspace_id?: string;
  worktree_name?: string;
  gitdir_rel?: string;
}

export interface GitWorkspace {
  workspace_id: string;
  root_path: string;
  repo_url: string;
  remote_name: string;
  branch_name: string;
  base_commit: string;
  head_commit: string;
  mode: string;
  workspace_kind: string;
  common_workspace_id: string;
  worktree_name: string;
  gitdir_rel: string;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface GitTreeReplaceRequest {
  commit_sha: string;
  nodes: GitTreeNode[];
}

export interface GitTreeNode {
  workspace_id?: string;
  commit_sha?: string;
  path: string;
  parent_path: string;
  name: string;
  kind: string;
  mode: string;
  object_sha: string;
  size_bytes: number;
  created_at?: string;
}

export interface GitStateRequest {
  checkpoint_commit?: string;
  storage_type?: string;
  storage_ref?: string;
  storage_ref_hash?: string;
  checksum_sha256?: string;
  size_bytes?: number;
  content?: Uint8Array | string;
}

export interface GitState {
  workspace_id: string;
  checkpoint_commit: string;
  storage_type: string;
  storage_ref: string;
  storage_ref_hash: string;
  checksum_sha256: string;
  size_bytes: number;
  content?: string;
  created_at: string;
  updated_at: string;
}

export interface GitObjectPackRequest {
  content: Uint8Array | string;
}

export interface GitObjectPack {
  workspace_id: string;
  pack_id: string;
  checksum_sha256: string;
  size_bytes: number;
  content?: string;
  created_at: string;
}

export type GitOverlayOp = "upsert" | "whiteout" | "chmod" | "symlink";
export type GitOverlayKind = "file" | "directory" | "symlink" | "submodule";

export interface GitOverlayEntryRequest {
  path: string;
  op?: GitOverlayOp;
  kind?: GitOverlayKind;
  mode?: string;
  storage_type?: string;
  storage_ref?: string;
  storage_ref_hash?: string;
  checksum_sha256?: string;
  size_bytes?: number;
  base_object_sha?: string;
  content?: Uint8Array | string;
}

export interface GitOverlayEntry {
  workspace_id: string;
  path: string;
  op: GitOverlayOp;
  kind: GitOverlayKind;
  mode: string;
  storage_type: string;
  storage_ref: string;
  storage_ref_hash: string;
  checksum_sha256: string;
  size_bytes: number;
  base_object_sha: string;
  content?: string;
  created_at: string;
  updated_at: string;
}

export interface JournalActor {
  type?: string;
  id?: string;
}

export interface JournalLabel {
  key: string;
  value: string;
}

export interface JournalCreateRequest {
  journal_id?: string;
  kind?: string;
  title?: string;
  actor?: JournalActor;
  source?: string;
  meta?: Record<string, string>;
  labels?: JournalLabel[];
  retention?: unknown;
}

export interface Journal {
  tenant_id?: string;
  journal_id: string;
  kind: string;
  title?: string;
  actor?: JournalActor;
  source?: string;
  meta?: Record<string, string>;
  labels?: JournalLabel[];
  retention?: unknown;
  next_seq?: number;
  genesis_hash?: string;
  head_hash?: string;
  created_at: string;
  updated_at?: string;
  closed_at?: string;
}

export interface JournalArtifactRef {
  name: string;
  hash: string;
  content_type?: string;
  size_bytes: number;
}

export interface JournalEntryInput {
  type?: string;
  schema_version?: number;
  status?: string;
  occurred_at?: string;
  actor?: JournalActor;
  source?: string;
  parent_entry_id?: string;
  correlation_id?: string;
  subjects?: string[];
  summary?: unknown;
  artifacts?: JournalArtifactRef[];
  artifact_refs?: JournalArtifactRef[];
}

export interface JournalEntry {
  tenant_id?: string;
  journal_id: string;
  seq: number;
  entry_id: string;
  type: string;
  schema_version: number;
  status?: string;
  occurred_at: string;
  observed_at: string;
  actor?: JournalActor;
  source: string;
  parent_entry_id?: string;
  correlation_id?: string;
  subjects?: string[];
  summary?: unknown;
  artifact_refs?: JournalArtifactRef[];
  prev_hash: string;
  entry_hash: string;
}

export interface JournalAppendResponse {
  journal_id: string;
  append_id: string;
  first_seq: number;
  last_seq: number;
  count: number;
  head_hash: string;
  idempotent: boolean;
}

export interface JournalSearchRequest {
  type?: string;
  status?: string;
  kind?: string;
  actor_type?: string;
  actor_id?: string;
  subjects?: string[];
  labels?: JournalLabel[];
  since?: string;
  until?: string;
  limit?: number;
  entries?: boolean;
  cursor?: string;
}

export interface JournalSearchMatch {
  journal_id: string;
  seq?: number;
  type?: string;
  status?: string;
  kind?: string;
  title?: string;
  observed_at?: string;
  created_at?: string;
  matched_subjects?: string[];
  matched_labels?: JournalLabel[];
  cursor?: string;
  entry?: JournalEntry;
}

export interface JournalVerifyResult {
  ok: boolean;
  journal_id: string;
  entries: number;
  head_hash: string;
  hash_chain_ok: boolean;
  seal_ok?: boolean;
  projection_ok?: boolean;
  artifact_bytes_available?: boolean;
  head_sealed?: boolean;
  latest_seal_seq?: number;
}
