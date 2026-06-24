import type { Client } from "./client.js";
import { checkError } from "./error.js";
import type { IssueScopedTokenRequest, IssueScopedTokenResponse } from "./models.js";

export async function issueScopedToken(
  client: Client,
  req: IssueScopedTokenRequest
): Promise<IssueScopedTokenResponse> {
  const resp = await fetch(`${client.baseUrl}/v1/tokens`, {
    method: "POST",
    headers: client.authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(req),
  });
  await checkError(resp);
  return (await resp.json()) as IssueScopedTokenResponse;
}

export async function revokeScopedToken(client: Client, tokenId: string): Promise<void> {
  const resp = await fetch(`${client.baseUrl}/v1/tokens/${encodeURIComponent(tokenId)}`, {
    method: "DELETE",
    headers: client.authHeaders(),
  });
  await checkError(resp);
}

export async function revokeScopedTokenByAPIKey(client: Client, apiKey: string): Promise<void> {
  const resp = await fetch(`${client.baseUrl}/v1/tokens/revoke`, {
    method: "POST",
    headers: client.authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({ api_key: apiKey }),
  });
  await checkError(resp);
}
