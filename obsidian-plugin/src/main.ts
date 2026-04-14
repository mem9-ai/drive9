import { Plugin, Notice, TFile, addIcon } from "obsidian";
import { Drive9Client, sanitizeError } from "./client";
import { RemoteWatcher } from "./remote-watcher";
import { SyncEngine } from "./sync-engine";
import { ShadowStore } from "./shadow-store";
import { ConflictResolver } from "./conflict-resolver";
import { Drive9SettingTab } from "./settings";
import { Drive9SearchModal } from "./search-modal";
import { SyncPanelModal } from "./sync-panel-modal";
import { runFirstRunReconciliation, pullAllRemote } from "./first-run";
import { initLocale, t } from "./i18n";
import type { PluginData, Drive9Settings, SyncState } from "./types";
import { DEFAULT_PLUGIN_DATA, DEFAULT_SETTINGS } from "./types";

export default class Drive9Plugin extends Plugin {
  settings: Drive9Settings = DEFAULT_SETTINGS;
  private client!: Drive9Client;
  private remoteWatcher: RemoteWatcher | null = null;
  private syncEngine!: SyncEngine;
  private conflictResolver!: ConflictResolver;
  private shadowStore!: ShadowStore;
  private resolutionTimer: ReturnType<typeof setInterval> | null = null;
  private shadowGCTimer: ReturnType<typeof setInterval> | null = null;
  private syncStates: Record<string, SyncState> = {};
  private firstRunComplete = false;
  private syncStarted = false;
  private statusBarEl: HTMLElement | null = null;
  private actorId = "";

  async onload(): Promise<void> {
    initLocale();
    await this.loadPluginData();

    const needsActorId = !this.actorId;
    if (needsActorId) {
      this.actorId = generateActorId();
    }

    this.client = new Drive9Client(
      this.settings.serverUrl,
      this.settings.apiKey,
    );
    this.client.setActorId(this.actorId);

    this.syncEngine = new SyncEngine(
      this.app.vault,
      this.client,
      this.syncStates,
      this.settings,
      () => this.savePluginData(),
    );

    this.shadowStore = new ShadowStore(this.app.vault.adapter);
    this.syncEngine.setShadowStore(this.shadowStore);

    this.conflictResolver = new ConflictResolver(
      this.app,
      this.app.vault,
      this.client,
      this.syncStates,
      () => this.savePluginData(),
    );
    this.conflictResolver.setSuppressLocalEvent(
      (path, fn) => this.syncEngine.withSuppressedLocalEvents(path, fn),
    );

    this.remoteWatcher = new RemoteWatcher(this.client, {
      onChange: (event) => this.syncEngine.onRemoteChange(event.path, event.op),
      onReset: () => this.syncEngine.fullSync(),
      onPoll: () => this.syncEngine.fullSync(),
    });

    if (needsActorId) {
      await this.savePluginData();
    }

    this.statusBarEl = this.addStatusBarItem();
    this.statusBarEl.addClass("mod-clickable");
    this.statusBarEl.addEventListener("click", () => this.showSyncPanel());
    this.updateStatusBar();
    this.syncEngine.onStatusChange(() => this.updateStatusBar());

    this.addSettingTab(new Drive9SettingTab(this.app, this));

    this.addCommand({
      id: "drive9-search",
      name: t("cmd.search"),
      callback: () => {
        if (!this.settings.serverUrl || !this.settings.apiKey) {
          new Notice(t("notice.configureFirst"));
          return;
        }
        new Drive9SearchModal(this.app, this.client).open();
      },
    });

    this.addCommand({
      id: "drive9-retry-sync",
      name: t("cmd.retrySync"),
      callback: () => {
        this.syncEngine.retrySync();
        new Notice(t("notice.retrying"));
      },
    });

    addIcon("drive9", DRIVE9_ICON_SVG);
    this.addRibbonIcon("drive9", t("cmd.searchRibbon"), () => {
      if (!this.settings.serverUrl || !this.settings.apiKey) {
        new Notice(t("notice.configureFirst"));
        return;
      }
      new Drive9SearchModal(this.app, this.client).open();
    });

    this.app.workspace.onLayoutReady(() => {
      void this.onLayoutReady();
    });
  }

  private async onLayoutReady(): Promise<void> {
    if (!this.settings.serverUrl || !this.settings.apiKey) {
      this.setStatusBar(t("status.configure"));
      return;
    }

    await this.startSyncIfReady();
  }

  /**
   * Run first-run reconciliation (if needed) then register vault events
   * and start the remote watcher. Safe to call multiple times — subsequent
   * calls are no-ops once sync is running.
   *
   * Called from onLayoutReady() and from the settings page after provision.
   */
  async startSyncIfReady(): Promise<void> {
    if (!this.settings.serverUrl || !this.settings.apiKey) {
      return;
    }

    if (!this.firstRunComplete) {
      try {
        await this.doFirstRun();
      } catch (e) {
        console.error("[drive9] first-run failed", e instanceof Error ? e.message : sanitizeError(String(e)));
        new Notice(t("notice.firstRunFailed", { error: e instanceof Error ? e.message : sanitizeError(String(e)) }));
        this.setStatusBar(t("status.firstRunFailed"));
        return;
      }
    }

    // Guard against duplicate event registration
    if (this.syncStarted) return;
    this.syncStarted = true;

    this.registerEvent(
      this.app.vault.on("create", (file) => this.syncEngine.onLocalCreate(file)),
    );
    this.registerEvent(
      this.app.vault.on("modify", (file) => this.syncEngine.onLocalModify(file)),
    );
    this.registerEvent(
      this.app.vault.on("delete", (file) => this.syncEngine.onLocalDelete(file)),
    );
    this.registerEvent(
      this.app.vault.on("rename", (file, oldPath) =>
        this.syncEngine.onLocalRename(file, oldPath),
      ),
    );

    this.remoteWatcher?.start();

    // Resolution loop: scan for conflicts and remote_deleted every 10s
    this.resolutionTimer = setInterval(() => {
      void this.conflictResolver.resolveAll();
    }, 10_000);

    // Shadow GC: clean up unreferenced shadow files every 5 minutes
    this.shadowGCTimer = setInterval(() => {
      void this.conflictResolver.gcShadows();
    }, 5 * 60_000);

    this.setStatusBar(t("status.synced"));
  }

  private async doFirstRun(): Promise<void> {
    this.setStatusBar(t("status.reconciling"));

    const result = await runFirstRunReconciliation(
      this.app,
      this.app.vault,
      this.client,
      this.settings.ignorePaths,
    );

    switch (result.action) {
      case "push_all": {
        const files = this.app.vault.getFiles();
        const total = files.length;
        new Notice(t("notice.uploading", { count: total }));
        this.setStatusBar(t("status.queuing", { current: 0, total }));
        for (let i = 0; i < files.length; i++) {
          this.syncEngine.onLocalCreate(files[i]);
          if ((i + 1) % 50 === 0 || i === files.length - 1) {
            this.setStatusBar(t("status.queuing", { current: i + 1, total }));
          }
        }
        break;
      }

      case "pull_all":
        new Notice(t("notice.downloading"));
        await pullAllRemote(
          this.app.vault,
          this.client,
          this.syncStates,
          this.settings.ignorePaths,
          this.shadowStore,
        );
        break;

      case "reconciled":
        Object.assign(this.syncStates, result.states);
        for (const [path, state] of Object.entries(result.states)) {
          if (state.status === "local_dirty") {
            this.syncEngine.onLocalCreate(
              this.app.vault.getAbstractFileByPath(path)!,
            );
          }
        }
        for (const [path, state] of Object.entries(result.states)) {
          if (state.status === "remote_dirty") {
            try {
              const data = await this.client.read(path);
              const dir = path.contains("/")
                ? path.substring(0, path.lastIndexOf("/"))
                : "";
              if (dir && !this.app.vault.getAbstractFileByPath(dir)) {
                await this.app.vault.createFolder(dir);
              }
              await this.app.vault.createBinary(path, data);
              if (state.remoteRevision === null) {
                try {
                  const st = await this.client.stat(path);
                  state.remoteRevision = st.revision;
                } catch {
                  // Leave revision unknown; push path will refresh before write.
                }
              }
              try {
                state.lastSyncedContentHash = await this.shadowStore.save(data);
              } catch { /* shadow save is best-effort */ }
              const pulled = this.app.vault.getAbstractFileByPath(path);
              if (pulled instanceof TFile) {
                state.localMtime = pulled.stat.mtime;
                state.localSize = pulled.stat.size;
              }
              state.status = state.remoteRevision !== null ? "synced" : "needs_refresh";
              state.syncedAt = Date.now();
            } catch (e) {
              console.error(`[drive9] pull failed: ${path}`, e instanceof Error ? e.message : sanitizeError(String(e)));
            }
          }
        }
        break;

      case "cancelled":
        new Notice(t("notice.firstRunCancelled"));
        return;
    }

    this.firstRunComplete = true;
    await this.savePluginData();
  }

  async loadPluginData(): Promise<void> {
    const raw = await this.loadData();
    const data: PluginData = Object.assign({}, DEFAULT_PLUGIN_DATA, raw ?? {});
    this.settings = Object.assign({}, DEFAULT_SETTINGS, data.settings);
    // Migration: existing installs may have empty serverUrl from before it was hardcoded
    if (!this.settings.serverUrl) {
      this.settings.serverUrl = DEFAULT_SETTINGS.serverUrl;
    }
    this.syncStates = data.syncStates ?? {};
    this.firstRunComplete = data.firstRunComplete ?? false;
    this.actorId = data.actorId ?? "";
  }

  async savePluginData(): Promise<void> {
    const data: PluginData = {
      settings: this.settings,
      syncStates: this.syncStates,
      firstRunComplete: this.firstRunComplete,
      actorId: this.actorId,
    };
    await this.saveData(data);

    if (!this.client) {
      return;
    }

    const urlChanged =
      this.client.getServerUrl() !== this.settings.serverUrl ||
      this.client.getAPIKey() !== this.settings.apiKey;

    this.client.updateConfig(this.settings.serverUrl, this.settings.apiKey);
    this.client.setActorId(this.actorId);
    this.syncEngine?.updateSettings(this.settings);

    if (this.remoteWatcher && urlChanged) {
      if (this.firstRunComplete && this.settings.serverUrl) {
        this.remoteWatcher.restart();
      } else {
        this.remoteWatcher.stop();
      }
    }
  }

  private updateStatusBar(): void {
    const engine = this.syncEngine;
    if (!engine) return;

    const skipped = engine.skippedLargeFiles.length;
    const conflicts = this.countConflicts();
    switch (engine.status) {
      case "syncing": {
        const progress = engine.uploadProgressText;
        if (progress) {
          this.setStatusBar(t("status.syncingProgress", { progress }));
        } else {
          this.setStatusBar(t("status.syncing", { count: engine.pendingCount }));
        }
        break;
      }
      case "offline":
        this.setStatusBar(t("status.offline"));
        break;
      case "error": {
        const detail = engine.lastErrorDetail;
        this.setStatusBar(detail ? t("status.errorDetail", { detail }) : t("status.error"));
        break;
      }
      case "idle":
        if (engine.pendingCount > 0) {
          this.setStatusBar(t("status.queued", { count: engine.pendingCount }));
        } else if (conflicts > 0) {
          this.setStatusBar(t(conflicts > 1 ? "status.conflictsPlural" : "status.conflicts", { count: conflicts }));
        } else if (skipped > 0) {
          this.setStatusBar(t("status.syncedSkipped", { count: skipped }));
        } else {
          this.setStatusBar(t("status.synced"));
        }
        break;
    }
  }

  private countConflicts(): number {
    let count = 0;
    for (const state of Object.values(this.syncStates)) {
      if (state.status === "conflict") count++;
    }
    return count;
  }

  private showSyncPanel(): void {
    const engine = this.syncEngine;

    const conflicts: Array<{ path: string; state: SyncState }> = [];
    for (const [path, state] of Object.entries(this.syncStates)) {
      if (state.status === "conflict") {
        conflicts.push({ path, state });
      }
    }

    new SyncPanelModal(this.app, {
      status: engine.status,
      pendingCount: engine.pendingCount,
      lastErrorDetail: engine.lastErrorDetail,
      skippedLargeFiles: engine.skippedLargeFiles,
      conflicts,
      onRetry: () => {
        this.syncEngine.retrySync();
        new Notice(t("notice.retrying"));
      },
      onOpenFile: (path) => {
        const file = this.app.vault.getAbstractFileByPath(path);
        if (file instanceof TFile) {
          void this.app.workspace.getLeaf().openFile(file);
        }
      },
    }).open();
  }

  private setStatusBar(text: string): void {
    if (this.statusBarEl) {
      this.statusBarEl.setText(text);
    }
  }

  onunload(): void {
    this.remoteWatcher?.stop();
    if (this.resolutionTimer) {
      clearInterval(this.resolutionTimer);
      this.resolutionTimer = null;
    }
    if (this.shadowGCTimer) {
      clearInterval(this.shadowGCTimer);
      this.shadowGCTimer = null;
    }
  }
}

/** drive9 logo as SVG for Obsidian's addIcon (100x100 viewBox, no fill — inherits currentColor). */
const DRIVE9_ICON_SVG = `<text x="50" y="68" text-anchor="middle" font-family="Arial,Helvetica,sans-serif" font-weight="bold" font-size="48" fill="currentColor">D9</text>`;

function generateActorId(): string {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return `obsidian-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
