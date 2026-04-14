import { Vault, TFile, Modal, App, Setting, Notice } from "obsidian";
import { Drive9Client, Drive9Error } from "./client";
import { t } from "./i18n";
import type { ShadowStore } from "./shadow-store";
import type { SyncState } from "./types";
import { IgnoreMatcher } from "./ignore";

export type FirstRunResult =
  | { action: "push_all" }
  | { action: "pull_all" }
  | { action: "reconciled"; states: Record<string, SyncState> }
  | { action: "cancelled" };

/**
 * Detect remote state and reconcile with local vault on first run.
 * Safety invariant: never silent overwrite.
 */
export async function runFirstRunReconciliation(
  app: App,
  vault: Vault,
  client: Drive9Client,
  ignorePaths: string[],
): Promise<FirstRunResult> {
  const ignore = new IgnoreMatcher(ignorePaths);

  // Gather local files.
  const localFiles = new Map<string, TFile>();
  for (const file of vault.getFiles()) {
    if (!ignore.isIgnored(file.path)) {
      localFiles.set(file.path, file);
    }
  }

  // Gather remote files.
  let remoteFiles: Map<string, { size: number; mtime: number }>;
  try {
    const entries = await client.listRecursive("/");
    remoteFiles = new Map(
      entries
        .filter((e) => !ignore.isIgnored(e.name))
        .map((e) => [e.name, { size: e.size, mtime: e.mtime }]),
    );
  } catch (e) {
    if (e instanceof Drive9Error && e.status === 404) {
      remoteFiles = new Map();
    } else {
      throw e;
    }
  }

  const localEmpty = localFiles.size === 0;
  const remoteEmpty = remoteFiles.size === 0;

  // Scenario 1: Remote empty, local has content → push all.
  if (remoteEmpty && !localEmpty) {
    return { action: "push_all" };
  }

  // Scenario 2: Remote has content, local empty → ask to pull.
  if (!remoteEmpty && localEmpty) {
    const confirmed = await askUser(
      app,
      t("firstRun.downloadTitle"),
      t("firstRun.downloadMsg", { count: remoteFiles.size }),
    );
    return confirmed ? { action: "pull_all" } : { action: "cancelled" };
  }

  // Scenario 3: Both empty → nothing to do.
  if (remoteEmpty && localEmpty) {
    return { action: "reconciled", states: {} };
  }

  // Scenario 4: Both have content → reconcile.
  const onlyLocal: string[] = [];
  const onlyRemote: string[] = [];
  const bothPresent: string[] = []; // exists on both sides — always conflict without checksum
  const states: Record<string, SyncState> = {};

  for (const [path] of localFiles) {
    const remote = remoteFiles.get(path);
    if (!remote) {
      onlyLocal.push(path);
    } else {
      // Without checksum, we cannot prove content is identical even if
      // size matches. Mark as conflict per stop-and-ask safety invariant.
      bothPresent.push(path);
    }
  }

  for (const path of remoteFiles.keys()) {
    if (!localFiles.has(path)) {
      onlyRemote.push(path);
    }
  }

  if (bothPresent.length > 0) {
    const fileList = bothPresent.slice(0, 10).join("\n");
    const more = bothPresent.length > 10 ? t("firstRun.andMore", { count: bothPresent.length - 10 }) : "";
    const files = more ? `${fileList}\n${more}` : fileList;
    const msg = t("firstRun.bothSidesNotice", { count: bothPresent.length, files });

    new Notice(msg, 15000);
  }

  // Only-local files: push (no expectedRevision needed for new files).
  for (const path of onlyLocal) {
    const file = localFiles.get(path)!;
    states[path] = {
      path,
      localMtime: file.stat.mtime,
      localSize: file.stat.size,
      remoteRevision: 0,
      syncedAt: 0,
      status: "local_dirty",
    };
  }

  // Only-remote files: need real revision for future CAS.
  for (const path of onlyRemote) {
    let revision: number | null = null;
    try {
      const st = await client.stat(path);
      revision = st.revision;
    } catch { /* stat failed — revision stays null (unknown) */ }
    states[path] = {
      path,
      localMtime: 0,
      localSize: 0,
      remoteRevision: revision,
      syncedAt: 0,
      status: "remote_dirty",
    };
  }

  // Both-present files: conflict. Seed revision for future resolution.
  for (const path of bothPresent) {
    const file = localFiles.get(path)!;
    let revision: number | null = null;
    try {
      const st = await client.stat(path);
      revision = st.revision;
    } catch { /* stat failed — revision stays null (unknown) */ }
    states[path] = {
      path,
      localMtime: file.stat.mtime,
      localSize: file.stat.size,
      remoteRevision: revision,
      syncedAt: 0,
      status: "conflict",
    };
  }

  return { action: "reconciled", states };
}

/**
 * Pull all remote files into the local vault.
 */
export async function pullAllRemote(
  vault: Vault,
  client: Drive9Client,
  syncStates: Record<string, SyncState>,
  ignorePaths: string[],
  shadowStore?: ShadowStore,
  onProgress?: (current: number, total: number) => void,
  persistData?: () => Promise<void>,
): Promise<void> {
  const ignore = new IgnoreMatcher(ignorePaths);
  const entries = await client.listRecursive("/");
  const filtered = entries.filter((e) => !ignore.isIgnored(e.name));
  const total = filtered.length;
  let count = 0;

  for (const entry of filtered) {
    // Ensure parent directories exist.
    const dir = entry.name.contains("/")
      ? entry.name.substring(0, entry.name.lastIndexOf("/"))
      : "";
    if (dir && !vault.getAbstractFileByPath(dir)) {
      await vault.createFolder(dir);
    }

    const data = await client.read(entry.name);
    const existing = vault.getAbstractFileByPath(entry.name);
    if (existing instanceof TFile) {
      await vault.modifyBinary(existing, data);
    } else {
      await vault.createBinary(entry.name, data);
    }

    const file = vault.getAbstractFileByPath(entry.name);
    if (file instanceof TFile) {
      let revision: number | null = null;
      try {
        const st = await client.stat(entry.name);
        revision = st.revision;
      } catch { /* revision stays null */ }
      let contentHash: string | undefined;
      if (shadowStore) {
        try { contentHash = await shadowStore.save(data); } catch { /* shadow save best-effort */ }
      }
      syncStates[entry.name] = {
        path: entry.name,
        localMtime: file.stat.mtime,
        localSize: file.stat.size,
        remoteRevision: revision,
        syncedAt: Date.now(),
        status: revision !== null ? "synced" : "needs_refresh",
        lastSyncedContentHash: contentHash,
      };
    }
    count++;
    onProgress?.(count, total);

    // Persist every 10 files so interrupted downloads retain progress
    if (persistData && count % 10 === 0) {
      await persistData();
    }
  }

  // Final persist to capture any remaining state
  if (persistData) {
    await persistData();
  }

  new Notice(t("notice.downloaded", { count }));
}

// ---------------------------------------------------------------------------
// Simple confirmation dialog
// ---------------------------------------------------------------------------

function askUser(app: App, title: string, message: string): Promise<boolean> {
  return new Promise((resolve) => {
    const modal = new ConfirmModal(app, title, message, resolve);
    modal.open();
  });
}

class ConfirmModal extends Modal {
  constructor(
    app: App,
    private title: string,
    private message: string,
    private resolve: (confirmed: boolean) => void,
  ) {
    super(app);
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.createEl("h3", { text: this.title });
    contentEl.createEl("p", { text: this.message });

    new Setting(contentEl)
      .addButton((btn) =>
        btn.setButtonText(t("firstRun.yes")).setCta().onClick(() => {
          this.resolve(true);
          this.close();
        }),
      )
      .addButton((btn) =>
        btn.setButtonText(t("firstRun.cancel")).onClick(() => {
          this.resolve(false);
          this.close();
        }),
      );
  }

  onClose(): void {
    this.contentEl.empty();
  }
}
