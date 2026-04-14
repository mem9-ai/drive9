export interface FileInfo {
  name: string;
  size: number;
  isDir: boolean;
  mtime?: number;
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

export interface VaultTokenIssueResponse {
  token: string;
  token_id: string;
  expires_at: string;
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
}
