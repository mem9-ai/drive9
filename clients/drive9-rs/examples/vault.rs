use drive9::Client;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), drive9::Drive9Error> {
    let client = Client::default_client();
    // Or: let client = Client::new("http://127.0.0.1:9009", "your-api-key");

    let secret_name = "example-secret";

    // Create or update a secret
    let mut fields = serde_json::Map::new();
    fields.insert("password".to_string(), json!("hunter2"));
    fields.insert("api_key".to_string(), json!("d9_xxxxxxxx"));

    client.create_vault_secret(secret_name, &fields).await?;
    println!("Created vault secret: {}", secret_name);

    // Read the secret
    let secret = client.read_vault_secret(secret_name).await?;
    println!("Read secret: {:?}", secret);

    // Read a single field
    if let Ok(field) = client.read_vault_secret_field(secret_name, "password").await {
        println!("Password field: {}", field);
    }

    // List secrets
    let secrets = client.list_vault_secrets().await?;
    println!("Vault secrets:");
    for s in secrets {
        println!("  {}", s.name);
    }

    // Issue a token
    let token = client
        .issue_vault_token("agent-1", "task-1", &["read".to_string()], 3600)
        .await?;
    println!("Issued token: {}", token.token_id);

    // Revoke the token
    client.revoke_vault_token(&token.token_id).await?;
    println!("Revoked token");

    // Clean up
    client.delete_vault_secret(secret_name).await?;
    println!("Deleted secret");

    Ok(())
}
