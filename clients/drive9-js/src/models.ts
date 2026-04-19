export interface FileInfo {
  name: string;
  size: number;
  isDir: boolean;
  /** UTC epoch seconds from server; converted to Date by the client. */
  mtime?: Date;
}

export interface StatResult {
  size: number;
  isDir: boolean;
  revision: number;
  mtime?: Date;
}

export interface SearchResult {
  path: string;
  name: string;
  /** Mirrors the snake_case field name used by the server API. */
  size_bytes: number;
  score?: number;
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

export interface VaultSecret {
  name: string;
  secret_type: string;
  revision: number;
  created_by: string;
  created_at: string;
  updated_at: string;
}

/**
 * Response for POST /v1/vault/tokens per spec 083aab8 line 133.
 * Wire shape: {token, grant_id, expires_at, scope[], perm, ttl}.
 */
export interface VaultTokenIssueResponse {
  token: string;
  grant_id: string;
  expires_at: string;
  scope: string[];
  perm: string;
  ttl: number;
}

/** Audit event returned by GET /v1/vault/audit (spec §16). */
export interface VaultAuditEvent {
  event_id: string;
  event_type: string;
  timestamp: string;
  grant_id?: string;
  agent?: string;
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
}
