import { Client } from "../src/index.js";

async function main() {
  const client = Client.defaultClient();
  // Or: const client = new Client("http://127.0.0.1:9009", "your-api-key");

  const secretName = "example-secret";

  // Create or update
  await client.createVaultSecret(secretName, {
    password: "hunter2",
    api_key: "d9_xxxxxxxx",
  });
  console.log("Created vault secret:", secretName);

  // Read
  const secret = await client.readVaultSecret(secretName);
  console.log("Read secret:", secret);

  // Read single field
  const field = await client.readVaultSecretField(secretName, "password");
  console.log("Password field:", field);

  // List
  const secrets = await client.listVaultSecrets();
  console.log("Vault secrets:");
  for (const s of secrets) {
    console.log(`  ${s.name}`);
  }

  // Issue grant (spec §6)
  const grant = await client.issueVaultToken("agent-1", [secretName], "read", 3600);
  console.log("Issued grant:", grant.grant_id, "perm:", grant.perm, "ttl:", grant.ttl);

  // Revoke
  await client.revokeVaultToken(grant.grant_id);
  console.log("Revoked grant");

  // Clean up
  await client.deleteVaultSecret(secretName);
  console.log("Deleted secret");
}

main().catch(console.error);
