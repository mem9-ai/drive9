use crate::client::Client;
use crate::error::{check_error, Drive9Error};
use crate::models::{VaultAuditEvent, VaultSecret, VaultTokenIssueResponse};
use serde_json::json;

impl Client {
    pub async fn create_vault_secret(
        &self,
        name: &str,
        fields: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<VaultSecret, Drive9Error> {
        let resp = self
            .http
            .post(self.vault_url("/secrets"))
            .headers(self.auth_headers())
            .json(&json!({"name": name, "fields": fields, "created_by": "drive9-rs"}))
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn update_vault_secret(
        &self,
        name: &str,
        fields: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<VaultSecret, Drive9Error> {
        let resp = self
            .http
            .put(self.vault_url(&format!("/secrets/{}", urlencoding::encode(name))))
            .headers(self.auth_headers())
            .json(&json!({"fields": fields, "updated_by": "drive9-rs"}))
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn delete_vault_secret(&self, name: &str) -> Result<(), Drive9Error> {
        let resp = self
            .http
            .delete(self.vault_url(&format!("/secrets/{}", urlencoding::encode(name))))
            .headers(self.auth_headers())
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn list_vault_secrets(&self) -> Result<Vec<VaultSecret>, Drive9Error> {
        let resp = self
            .http
            .get(self.vault_url("/secrets"))
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        let result: serde_json::Value = resp.json().await?;
        Ok(serde_json::from_value(
            result.get("secrets").cloned().unwrap_or(json!([])),
        )?)
    }

    /// Issue a scoped capability grant (spec §6).
    /// Request body: {agent, scope[], perm, ttl_seconds, label_hint?}.
    /// Response: {token, grant_id, expires_at, scope[], perm, ttl}.
    pub async fn issue_vault_token(
        &self,
        agent: &str,
        scope: &[String],
        perm: &str,
        ttl_seconds: i64,
        label_hint: Option<&str>,
    ) -> Result<VaultTokenIssueResponse, Drive9Error> {
        let mut body = json!({
            "agent": agent,
            "scope": scope,
            "perm": perm,
            "ttl_seconds": ttl_seconds,
        });
        if let Some(hint) = label_hint {
            body["label_hint"] = json!(hint);
        }
        let resp = self
            .http
            .post(self.vault_url("/tokens"))
            .headers(self.auth_headers())
            .json(&body)
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn revoke_vault_token(&self, grant_id: &str) -> Result<(), Drive9Error> {
        let resp = self
            .http
            .delete(self.vault_url(&format!("/tokens/{}", urlencoding::encode(grant_id))))
            .headers(self.auth_headers())
            .send()
            .await?;
        check_error(resp).await?;
        Ok(())
    }

    pub async fn query_vault_audit(
        &self,
        secret_name: Option<&str>,
        limit: i32,
    ) -> Result<Vec<VaultAuditEvent>, Drive9Error> {
        let mut url = self.vault_url("/audit").to_string();
        let mut params = vec![];
        if let Some(s) = secret_name {
            params.push(format!("secret={}", urlencoding::encode(s)));
        }
        if limit > 0 {
            params.push(format!("limit={}", limit));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        let resp = self
            .http
            .get(&url)
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        let result: serde_json::Value = resp.json().await?;
        Ok(serde_json::from_value(
            result.get("events").cloned().unwrap_or(json!([])),
        )?)
    }

    pub async fn list_readable_vault_secrets(&self) -> Result<Vec<String>, Drive9Error> {
        let resp = self
            .http
            .get(self.vault_url("/read"))
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        let result: serde_json::Value = resp.json().await?;
        Ok(serde_json::from_value(
            result.get("secrets").cloned().unwrap_or(json!([])),
        )?)
    }

    pub async fn read_vault_secret(
        &self,
        name: &str,
    ) -> Result<serde_json::Map<String, serde_json::Value>, Drive9Error> {
        let resp = self
            .http
            .get(self.vault_url(&format!("/read/{}", urlencoding::encode(name))))
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.json().await?)
    }

    pub async fn read_vault_secret_field(
        &self,
        name: &str,
        field: &str,
    ) -> Result<String, Drive9Error> {
        let resp = self
            .http
            .get(self.vault_url(&format!(
                "/read/{}/{}",
                urlencoding::encode(name),
                urlencoding::encode(field)
            )))
            .headers(self.auth_headers())
            .send()
            .await?;
        let resp = check_error(resp).await?;
        Ok(resp.text().await?)
    }
}
