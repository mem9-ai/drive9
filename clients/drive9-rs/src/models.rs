use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub size: i64,
    #[serde(rename = "isDir")]
    pub is_dir: bool,
    pub mtime: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct StatResult {
    pub size: i64,
    pub is_dir: bool,
    pub revision: i64,
    pub mtime: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchResult {
    pub path: String,
    pub name: String,
    pub size_bytes: i64,
    pub score: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PartURL {
    pub number: i32,
    pub url: String,
    pub size: i64,
    pub checksum_sha256: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub headers: Option<serde_json::Map<String, serde_json::Value>>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UploadPlan {
    pub upload_id: String,
    pub part_size: i64,
    pub parts: Vec<PartURL>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PatchPartURL {
    pub number: i32,
    pub url: String,
    pub size: i64,
    pub headers: Option<serde_json::Map<String, serde_json::Value>>,
    pub expires_at: Option<String>,
    pub read_url: Option<String>,
    pub read_headers: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PatchPlan {
    pub upload_id: String,
    pub part_size: i64,
    pub upload_parts: Vec<PatchPartURL>,
    pub copied_parts: Vec<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UploadMeta {
    #[serde(rename = "upload_id")]
    pub upload_id: String,
    pub parts_total: i32,
    pub status: String,
    #[serde(rename = "expires_at")]
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VaultSecret {
    pub name: String,
    pub secret_type: String,
    pub revision: i64,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VaultTokenIssueResponse {
    pub token: String,
    pub token_id: String,
    #[serde(rename = "expires_at")]
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VaultAuditEvent {
    pub event_id: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "token_id")]
    pub token_id: Option<String>,
    pub agent_id: Option<String>,
    pub task_id: Option<String>,
    pub secret_name: Option<String>,
    pub field_name: Option<String>,
    pub adapter: Option<String>,
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CompletePart {
    pub number: i32,
    pub etag: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PresignedPart {
    pub number: i32,
    pub url: String,
    pub size: i64,
    pub headers: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct UploadPlanV2 {
    #[serde(rename = "upload_id")]
    pub upload_id: String,
    #[allow(dead_code)]
    pub key: String,
    #[serde(rename = "part_size")]
    pub part_size: i64,
    pub total_parts: i32,
}
