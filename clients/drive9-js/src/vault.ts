import { Client } from "./client.js";
import { checkError, Drive9Error } from "./error.js";
import type { VaultAuditEvent, VaultSecret, VaultTokenIssueResponse } from "./models.js";

export async function createVaultSecret(
  client: Client,
  name: string,
  fields: Record<string, unknown>,
  createdBy = "drive9-js"
): Promise<VaultSecret> {
  const resp = await fetch(client.vaultUrl("/secrets"), {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ name, fields, created_by: createdBy }),
  });
  await checkError(resp);
  return (await resp.json()) as VaultSecret;
}

export async function updateVaultSecret(
  client: Client,
  name: string,
  fields: Record<string, unknown>,
  updatedBy = "drive9-js"
): Promise<VaultSecret> {
  const resp = await fetch(client.vaultUrl(`/secrets/${encodeURIComponent(name)}`), {
    method: "PUT",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ fields, updated_by: updatedBy }),
  });
  await checkError(resp);
  return (await resp.json()) as VaultSecret;
}

export async function deleteVaultSecret(client: Client, name: string): Promise<void> {
  const resp = await fetch(client.vaultUrl(`/secrets/${encodeURIComponent(name)}`), {
    method: "DELETE",
    headers: client["authHeaders"](),
  });
  await checkError(resp);
}

export async function listVaultSecrets(client: Client): Promise<VaultSecret[]> {
  const resp = await fetch(client.vaultUrl("/secrets"), {
    headers: client["authHeaders"](),
  });
  await checkError(resp);
  const body = (await resp.json()) as { secrets?: VaultSecret[] };
  return body.secrets || [];
}

/**
 * Issue a scoped capability grant (spec §6).
 * Request body: {agent, scope[], perm, ttl_seconds, label_hint?}.
 * Response: {token, grant_id, expires_at, scope[], perm, ttl}.
 */
export async function issueVaultToken(
  client: Client,
  agent: string,
  scope: string[],
  perm: string,
  ttlSeconds: number,
  labelHint?: string
): Promise<VaultTokenIssueResponse> {
  const body: Record<string, unknown> = {
    agent,
    scope,
    perm,
    ttl_seconds: ttlSeconds,
  };
  if (labelHint) body.label_hint = labelHint;
  const resp = await fetch(client.vaultUrl("/tokens"), {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify(body),
  });
  await checkError(resp);
  return (await resp.json()) as VaultTokenIssueResponse;
}

export async function revokeVaultToken(client: Client, grantId: string): Promise<void> {
  const resp = await fetch(client.vaultUrl(`/tokens/${encodeURIComponent(grantId)}`), {
    method: "DELETE",
    headers: client["authHeaders"](),
  });
  await checkError(resp);
}

export async function queryVaultAudit(
  client: Client,
  secretName?: string,
  limit = 0
): Promise<VaultAuditEvent[]> {
  const params = new URLSearchParams();
  if (secretName) params.set("secret", secretName);
  if (limit > 0) params.set("limit", String(limit));
  const qs = params.toString();
  const url = `${client.vaultUrl("/audit")}${qs ? "?" + qs : ""}`;
  const resp = await fetch(url, { headers: client["authHeaders"]() });
  await checkError(resp);
  const body = (await resp.json()) as { events?: VaultAuditEvent[] };
  return body.events || [];
}

export async function listReadableVaultSecrets(client: Client): Promise<string[]> {
  const resp = await fetch(client.vaultUrl("/read"), { headers: client["authHeaders"]() });
  await checkError(resp);
  const body = (await resp.json()) as { secrets?: string[] };
  return body.secrets || [];
}

export async function readVaultSecret(
  client: Client,
  name: string
): Promise<Record<string, unknown>> {
  const resp = await fetch(client.vaultUrl(`/read/${encodeURIComponent(name)}`), {
    headers: client["authHeaders"](),
  });
  await checkError(resp);
  return (await resp.json()) as Record<string, unknown>;
}

export async function readVaultSecretField(client: Client, name: string, field: string): Promise<string> {
  const resp = await fetch(
    client.vaultUrl(`/read/${encodeURIComponent(name)}/${encodeURIComponent(field)}`),
    { headers: client["authHeaders"]() }
  );
  await checkError(resp);
  return await resp.text();
}
