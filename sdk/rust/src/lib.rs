use reqwest::{Client, Method};
use serde::Deserialize;
use std::error::Error;

/// drive9 Rust SDK
/// 
/// A simple Rust client for drive9 API.

#[derive(Debug, Deserialize)]
pub struct Entry {
    pub name: String,
    pub size: i64,
    pub is_dir: bool,
}

#[derive(Debug)]
pub struct StatResult {
    pub size: i64,
    pub is_dir: bool,
    pub revision: i64,
}

pub struct Drive9Client {
    base_url: String,
    api_key: String,
    client: Client,
}

impl Drive9Client {
    /// Initialize drive9 client.
    /// 
    /// # Arguments
    /// * `base_url` - API endpoint (e.g., "https://api.drive9.ai")
    /// * `api_key` - Your API key from 'drive9 create' or console
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            client: Client::new(),
        }
    }

    /// Build full URL for a path.
    fn url(&self, path: &str) -> String {
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };
        format!("{}/v1/fs{}", self.base_url, path)
    }

    /// Upload data to a path.
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.client
            .put(self.url(path))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Download a file's content.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let resp = self.client
            .get(self.url(path))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?
            .error_for_status()?;
        
        Ok(resp.bytes().await?.to_vec())
    }

    /// Download a file as String.
    pub async fn read_text(&self, path: &str) -> Result<String, Box<dyn Error>> {
        let bytes = self.read(path).await?;
        Ok(String::from_utf8(bytes)?)
    }

    /// List directory entries.
    pub async fn list(&self, path: &str) -> Result<Vec<Entry>, Box<dyn Error>> {
        #[derive(Deserialize)]
        struct ListResponse {
            entries: Vec<Entry>,
        }

        let resp = self.client
            .get(self.url(path))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .query(&[("list", "")])
            .send()
            .await?
            .error_for_status()?;
        
        let data: ListResponse = resp.json().await?;
        Ok(data.entries)
    }

    /// Get file/directory metadata.
    pub async fn stat(&self, path: &str) -> Result<StatResult, Box<dyn Error>> {
        let resp = self.client
            .head(self.url(path))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?
            .error_for_status()?;

        let size = resp.headers()
            .get("X-Drive9-Size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let is_dir = resp.headers()
            .get("X-Drive9-IsDir")
            .map(|v| v == "true")
            .unwrap_or(false);

        let revision = resp.headers()
            .get("X-Drive9-Revision")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        Ok(StatResult { size, is_dir, revision })
    }

    /// Copy a file (zero-copy within drive9).
    pub async fn copy(&self, src: &str, dst: &str) -> Result<(), Box<dyn Error>> {
        self.client
            .post(self.url(dst))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("X-Drive9-Copy-Source", src)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Rename/move a file (metadata only).
    pub async fn rename(&self, src: &str, dst: &str) -> Result<(), Box<dyn Error>> {
        self.client
            .post(self.url(dst))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("X-Drive9-Rename-Source", src)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Delete a file or directory.
    pub async fn delete(&self, path: &str) -> Result<(), Box<dyn Error>> {
        self.client
            .delete(self.url(path))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Create a directory.
    pub async fn mkdir(&self, path: &str) -> Result<(), Box<dyn Error>> {
        self.client
            .post(self.url(path))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .query(&[("mkdir", "")])
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_write_read() {
        let client = Drive9Client::new(
            "https://api.drive9.ai",
            "your-api-key"
        );

        // This would need a real API key to test
        // client.write("/test.txt", b"hello".to_vec()).await.unwrap();
        // let data = client.read("/test.txt").await.unwrap();
        // assert_eq!(data, b"hello");
    }
}
