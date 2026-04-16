use crate::client::Client;
use crate::error::{check_error, Drive9Error};
use crate::models::PatchPartURL;
use reqwest::header::{HeaderMap, HeaderName, CONTENT_TYPE};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Semaphore;

impl Client {
    pub async fn patch_file<F>(
        &self,
        path: &str,
        new_size: i64,
        dirty_parts: &[i32],
        read_part: F,
    ) -> Result<(), Drive9Error>
    where
        F: Fn(i32, i64, Option<&[u8]>) -> Result<Vec<u8>, Drive9Error> + Send + Sync + 'static,
    {
        let body = json!({
            "new_size": new_size,
            "dirty_parts": dirty_parts,
        });
        let resp = self
            .http
            .patch(self.fs_url(path))
            .headers(self.auth_headers())
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        let plan: crate::models::PatchPlan = resp.json().await?;

        let _total_parts = plan.upload_parts.len() + plan.copied_parts.len();
        let sem = Arc::new(Semaphore::new(4));
        let mut tasks = vec![];
        let read_part = Arc::new(read_part);

        for part in plan.upload_parts {
            let permit = Arc::clone(&sem).acquire_owned().await.unwrap();
            let client = self.clone();
            let rf = Arc::clone(&read_part);
            let task = tokio::spawn(async move {
                let _permit = permit;
                client.upload_patch_part(&part, rf.as_ref()).await
            });
            tasks.push(task);
        }

        let mut first_err = None;
        for t in tasks {
            match t.await {
                Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                Err(e) if first_err.is_none() => {
                    first_err = Some(Drive9Error::Other(format!("task panicked: {}", e)))
                }
                _ => {}
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }

        self.complete_upload(&plan.upload_id).await
    }

    async fn upload_patch_part<F>(
        &self,
        part: &PatchPartURL,
        read_part: &F,
    ) -> Result<(), Drive9Error>
    where
        F: Fn(i32, i64, Option<&[u8]>) -> Result<Vec<u8>, Drive9Error> + Send + Sync + 'static,
    {
        let orig_data = if let Some(ref read_url) = part.read_url {
            let mut headers = HeaderMap::new();
            if let Some(ref rh) = part.read_headers {
                for (k, v) in rh {
                    if let Ok(hv) = reqwest::header::HeaderValue::from_str(v.as_str().unwrap_or(""))
                    {
                        headers.insert(
                            HeaderName::from_bytes(k.as_bytes()).unwrap_or(CONTENT_TYPE),
                            hv,
                        );
                    }
                }
            }
            let resp = self.http.get(read_url).headers(headers).send().await?;
            let resp = check_error(resp).await?;
            Some(resp.bytes().await?.to_vec())
        } else {
            None
        };

        let data = read_part(part.number, part.size, orig_data.as_deref())?;
        let checksum = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            Sha256::digest(&data),
        );

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-checksum-sha256",
            reqwest::header::HeaderValue::from_str(&checksum).unwrap(),
        );
        if let Some(ref ph) = part.headers {
            for (k, v) in ph {
                if let Ok(hv) = reqwest::header::HeaderValue::from_str(v.as_str().unwrap_or("")) {
                    if k.eq_ignore_ascii_case("host") {
                        continue;
                    }
                    headers.insert(
                        HeaderName::from_bytes(k.as_bytes()).unwrap_or(CONTENT_TYPE),
                        hv,
                    );
                }
            }
        }
        let resp = self
            .http
            .put(&part.url)
            .headers(headers)
            .body(data)
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }
}
