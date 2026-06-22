import type { Client } from "./client.js";
import { checkError } from "./error.js";
import type {
  GitObjectPack,
  GitObjectPackRequest,
  GitOverlayEntry,
  GitOverlayEntryRequest,
  GitState,
  GitStateRequest,
  GitTreeNode,
  GitTreeReplaceRequest,
  GitWorkspace,
  GitWorkspaceRequest,
} from "./models.js";

function wsURL(client: Client, workspaceId?: string, suffix = ""): string {
  return workspaceId
    ? `${client.baseUrl}/v1/git-workspaces/${encodeURIComponent(workspaceId)}${suffix}`
    : `${client.baseUrl}/v1/git-workspaces${suffix}`;
}

function encodeContent<T extends { content?: Uint8Array | string }>(req: T): T {
  if (req.content instanceof Uint8Array) {
    return { ...req, content: Buffer.from(req.content).toString("base64") };
  }
  return req;
}

async function json<T>(resp: Response): Promise<T> {
  await checkError(resp);
  return (await resp.json()) as T;
}

export async function upsertGitWorkspace(client: Client, req: GitWorkspaceRequest): Promise<GitWorkspace> {
  return json<GitWorkspace>(
    await fetch(wsURL(client), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function getGitWorkspaceByRoot(client: Client, rootPath: string): Promise<GitWorkspace> {
  return json<GitWorkspace>(
    await fetch(wsURL(client, undefined, `?root_path=${encodeURIComponent(rootPath)}`), {
      headers: client.authHeaders(),
    })
  );
}

export async function getGitWorkspace(client: Client, workspaceId: string): Promise<GitWorkspace> {
  return json<GitWorkspace>(await fetch(wsURL(client, workspaceId), { headers: client.authHeaders() }));
}

export async function deleteGitWorkspace(client: Client, workspaceId: string): Promise<void> {
  await checkError(await fetch(wsURL(client, workspaceId), { method: "DELETE", headers: client.authHeaders() }));
}

export async function listGitWorkspaces(client: Client): Promise<GitWorkspace[]> {
  const body = await json<{ workspaces?: GitWorkspace[] }>(await fetch(wsURL(client), { headers: client.authHeaders() }));
  return body.workspaces || [];
}

export async function replaceGitTree(client: Client, workspaceId: string, req: GitTreeReplaceRequest): Promise<void> {
  await checkError(
    await fetch(wsURL(client, workspaceId, "/tree"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(req),
    })
  );
}

export async function listGitTree(client: Client, workspaceId: string, commitSHA: string): Promise<GitTreeNode[]> {
  const qs = new URLSearchParams({ commit_sha: commitSHA });
  const body = await json<{ nodes?: GitTreeNode[] }>(
    await fetch(wsURL(client, workspaceId, `/tree?${qs.toString()}`), { headers: client.authHeaders() })
  );
  return body.nodes || [];
}

export async function upsertGitState(client: Client, workspaceId: string, req: GitStateRequest): Promise<GitState> {
  return json<GitState>(
    await fetch(wsURL(client, workspaceId, "/git-state"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(encodeContent(req)),
    })
  );
}

export async function getGitState(client: Client, workspaceId: string): Promise<GitState> {
  return json<GitState>(await fetch(wsURL(client, workspaceId, "/git-state"), { headers: client.authHeaders() }));
}

export async function putGitObjectPack(client: Client, workspaceId: string, req: GitObjectPackRequest): Promise<GitObjectPack> {
  return json<GitObjectPack>(
    await fetch(wsURL(client, workspaceId, "/object-packs"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(encodeContent(req)),
    })
  );
}

export async function listGitObjectPacks(client: Client, workspaceId: string): Promise<GitObjectPack[]> {
  const body = await json<{ packs?: GitObjectPack[] }>(
    await fetch(wsURL(client, workspaceId, "/object-packs"), { headers: client.authHeaders() })
  );
  return body.packs || [];
}

export async function getGitObjectPack(client: Client, workspaceId: string, packId: string): Promise<GitObjectPack> {
  return json<GitObjectPack>(
    await fetch(wsURL(client, workspaceId, `/object-packs/${encodeURIComponent(packId)}`), {
      headers: client.authHeaders(),
    })
  );
}

export async function putGitOverlayEntry(
  client: Client,
  workspaceId: string,
  req: GitOverlayEntryRequest
): Promise<GitOverlayEntry> {
  return json<GitOverlayEntry>(
    await fetch(wsURL(client, workspaceId, "/overlay"), {
      method: "POST",
      headers: client.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(encodeContent(req)),
    })
  );
}

export async function getGitOverlayEntry(client: Client, workspaceId: string, relPath: string): Promise<GitOverlayEntry> {
  const qs = new URLSearchParams({ path: relPath });
  return json<GitOverlayEntry>(
    await fetch(wsURL(client, workspaceId, `/overlay?${qs.toString()}`), { headers: client.authHeaders() })
  );
}

export async function listGitOverlayEntries(client: Client, workspaceId: string): Promise<GitOverlayEntry[]> {
  const body = await json<{ entries?: GitOverlayEntry[] }>(
    await fetch(wsURL(client, workspaceId, "/overlay"), { headers: client.authHeaders() })
  );
  return body.entries || [];
}
