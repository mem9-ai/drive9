use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Drive9Error {
    #[error("HTTP {status_code}: {message}")]
    Status { status_code: u16, message: String },

    #[error("Conflict: {message}")]
    Conflict {
        status_code: u16,
        message: String,
        server_revision: Option<i64>,
    },

    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub(crate) async fn check_error(resp: reqwest::Response) -> Result<reqwest::Response, Drive9Error> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let bytes = resp.bytes().await.unwrap_or_default();
    let parsed = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
    let message = parsed
        .as_ref()
        .and_then(|v| {
            v.get("error")
                .or_else(|| v.get("message"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        })
        .or_else(|| {
            let text = String::from_utf8_lossy(&bytes).trim().to_string();
            if text.is_empty() { None } else { Some(text) }
        })
        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
    if status == StatusCode::CONFLICT {
        let server_revision = parsed
            .as_ref()
            .and_then(|v| v.get("server_revision").and_then(|n| n.as_i64()));
        Err(Drive9Error::Conflict {
            status_code: status.as_u16(),
            message,
            server_revision,
        })
    } else {
        Err(Drive9Error::Status {
            status_code: status.as_u16(),
            message,
        })
    }
}
