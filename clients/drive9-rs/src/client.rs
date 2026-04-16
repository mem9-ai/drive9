use crate::error::{check_error, Drive9Error};
use crate::models::{FileInfo, SearchResult, StatResult};
use crate::stream::StreamWriter;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::json;
use std::collections::HashMap;

const DEFAULT_SMALL_FILE_THRESHOLD: i64 = 50_000;

fn load_config_file() -> Option<(String, Option<String>)> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_default();
    let path = std::path::Path::new(&home).join(".drive9").join("config");
    let data = std::fs::read_to_string(path).ok()?;
    let cfg: serde_json::Value = serde_json::from_str(&data).ok()?;
    let server = cfg
        .get("server")
        .and_then(|v| v.as_str())
        .unwrap_or("https://api.drive9.ai")
        .to_string();
    let key = cfg
        .get("current_context")
        .and_then(|v| v.as_str())
        .and_then(|current| {
            cfg.get("contexts")
                .and_then(|v| v.get(current))
                .and_then(|v| v.get("api_key"))
                .and_then(|v| v.as_str())
        })
        .map(|s| s.to_string());
    Some((server, key))
}

fn load_config() -> (String, Option<String>) {
    let env_server = std::env::var("DRIVE9_SERVER").ok().filter(|s| !s.is_empty());
    let env_key = std::env::var("DRIVE9_API_KEY").ok().filter(|s| !s.is_empty());
    let (file_server, file_key) = load_config_file()
        .unwrap_or_else(|| ("https://api.drive9.ai".to_string(), None));
    (
        env_server.unwrap_or(file_server),
        env_key.or(file_key),
    )
}

#[derive(Clone, Debug)]
pub struct Client {
    pub(crate) base_url: String,
    pub(crate) api_key: Option<String>,
    pub(crate) http: reqwest::Client,
    pub(crate) small_file_threshold: i64,
}

impl Client {
    /// Create a new client.
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self::new_with_opts(base_url.into(), api_key.into())
    }

    /// Create a client using defaults from `~/.drive9/config`.
    pub fn default_client() -> Self {
        let (server, key) = load_config();
        Self::new_with_opts(server, key.unwrap_or_default())
    }

    fn new_with_opts(base_url: String, api_key: String) -> Self {
        let (cfg_base, cfg_api) = load_config();
        let mut base = base_url.trim_end_matches('/').to_string();
        let mut api = Some(api_key).filter(|s| !s.is_empty());
        if api.is_none() {
            api = cfg_api;
        }
        if base.is_empty() {
            base = cfg_base;
        }
        let http = reqwest::Client::builder()
            .user_agent(concat!("drive9-rs/", env!("CARGO_PKG_VERSION")))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            base_url: base,
            api_key: api,
            http,
            small_file_threshold: DEFAULT_SMALL_FILE_THRESHOLD,
        }
    }

    pub fn with_small_file_threshold(mut self, threshold: i64) -> Self {
        self.small_file_threshold = threshold;
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    pub(crate) fn fs_url(&self, path: &str) -> String {
        let p = if path.starts_with('/') {
            path
        } else {
            &format!("/{}", path)
        };
        format!("{}/v1/fs{}", self.base_url, p)
    }

    pub(crate) fn vault_url(&self, path: &str) -> String {
        let p = if path.starts_with('/') {
            path
        } else {
            &format!("/{}", path)
        };
        format!("{}/v1/vault{}", self.base_url, p)
    }

    pub(crate) fn auth_headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(ref key) = self.api_key {
            if let Ok(v) = HeaderValue::from_str(&format!("Bearer {}", key)) {
                h.insert("Authorization", v);
            }
        }
        h
    }

    pub async fn write(&self, path: &str, data: &[u8]) -> Result<(), Drive9Error> {
        self.write_with_revision(path, data, -1).await
    }

    pub async fn write_with_revision(
        &self,
        path: &str,
        data: &[u8],
        expected_revision: i64,
    ) -> Result<(), Drive9Error> {
        let mut headers = self.auth_headers();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        if expected_revision >= 0 {
            headers.insert(
                "X-Dat9-Expected-Revision",
                HeaderValue::from_str(&expected_revision.to_string()).unwrap(),
            );
        }
        let resp = self
            .http
            .put(self.fs_url(path))
            .headers(headers)
            .body(data.to_vec())
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn read(&self, path: &str) -> Result<Vec<u8>, Drive9Error> {
        let resp = self
            .http
            .get(self.fs_url(path))
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.bytes().await?.to_vec())
    }

    pub async fn list(&self, path: &str) -> Result<Vec<FileInfo>, Drive9Error> {
        let resp = self
            .http
            .get(format!("{}?list=1", self.fs_url(path)))
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        let result: serde_json::Value = resp.json().await?;
        let entries: Vec<FileInfo> =
            serde_json::from_value(result.get("entries").cloned().unwrap_or(json!([])))?;
        Ok(entries)
    }

    pub async fn stat(&self, path: &str) -> Result<StatResult, Drive9Error> {
        let resp = self
            .http
            .head(self.fs_url(path))
            .headers(self.auth_headers())
            .send()
            .await?;
        let status = resp.status();
        if status.as_u16() == 404 {
            return Err(Drive9Error::Other(format!("not found: {}", path)));
        }
        if !status.is_success() {
            return Err(Drive9Error::Status {
                status_code: status.as_u16(),
                message: format!("HTTP {}", status.as_u16()),
            });
        }
        let headers = resp.headers();
        let size = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0i64);
        let revision = headers
            .get("X-Dat9-Revision")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0i64);
        let mtime = headers
            .get("X-Dat9-Mtime")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0));
        Ok(StatResult {
            size,
            is_dir: headers
                .get("X-Dat9-IsDir")
                .map(|v| v == "true")
                .unwrap_or(false),
            revision,
            mtime,
        })
    }

    pub async fn delete(&self, path: &str) -> Result<(), Drive9Error> {
        let resp = self
            .http
            .delete(self.fs_url(path))
            .headers(self.auth_headers())
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn copy(&self, src_path: &str, dst_path: &str) -> Result<(), Drive9Error> {
        let mut headers = self.auth_headers();
        headers.insert(
            "X-Dat9-Copy-Source",
            HeaderValue::from_str(src_path).unwrap(),
        );
        let resp = self
            .http
            .post(format!("{}?copy", self.fs_url(dst_path)))
            .headers(headers)
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn rename(&self, old_path: &str, new_path: &str) -> Result<(), Drive9Error> {
        let mut headers = self.auth_headers();
        headers.insert(
            "X-Dat9-Rename-Source",
            HeaderValue::from_str(old_path).unwrap(),
        );
        let resp = self
            .http
            .post(format!("{}?rename", self.fs_url(new_path)))
            .headers(headers)
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn mkdir(&self, path: &str) -> Result<(), Drive9Error> {
        let resp = self
            .http
            .post(format!("{}?mkdir", self.fs_url(path)))
            .headers(self.auth_headers())
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn sql(&self, query: &str) -> Result<Vec<serde_json::Value>, Drive9Error> {
        let resp = self
            .http
            .post(format!("{}/v1/sql", self.base_url))
            .headers(self.auth_headers())
            .json(&json!({"query": query}))
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn grep(
        &self,
        query: &str,
        path_prefix: &str,
        limit: i32,
    ) -> Result<Vec<SearchResult>, Drive9Error> {
        let mut url = format!(
            "{}?grep={}",
            self.fs_url(path_prefix),
            urlencoding::encode(query)
        );
        if limit > 0 {
            url.push_str(&format!("&limit={}", limit));
        }
        let resp = self
            .http
            .get(&url)
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn find(
        &self,
        path_prefix: &str,
        params: &HashMap<String, String>,
    ) -> Result<Vec<SearchResult>, Drive9Error> {
        let mut p = params.clone();
        p.insert("find".to_string(), "".to_string());
        let qs: Vec<_> = p
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
            .collect();
        let url = format!("{}?{}", self.fs_url(path_prefix), qs.join("&"));
        let resp = self
            .http
            .get(&url)
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub fn new_stream_writer(&self, path: &str, total_size: i64) -> StreamWriter {
        StreamWriter::new(self.clone(), path.to_string(), total_size, -1)
    }

    pub fn new_stream_writer_conditional(
        &self,
        path: &str,
        total_size: i64,
        expected_revision: i64,
    ) -> StreamWriter {
        StreamWriter::new(
            self.clone(),
            path.to_string(),
            total_size,
            expected_revision,
        )
    }
}
