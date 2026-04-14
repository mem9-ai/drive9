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
        // Mark first-run decision made before starting — if interrupted,
        // normal sync (fullSync) will pick up remaining files on restart
        // instead of re-entering first-run and causing conflicts.
        this.firstRunComplete = true;
        await this.savePluginData();

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

      case "pull_all": {
        // Mark first-run decision made before starting — if interrupted,
        // normal sync (fullSync) will pick up remaining files on restart
        // instead of re-entering first-run and causing conflicts.
        this.firstRunComplete = true;
        await this.savePluginData();

        new Notice(t("notice.downloading"));
        await pullAllRemote(
          this.app.vault,
          this.client,
          this.syncStates,
          this.settings.ignorePaths,
          this.shadowStore,
          (current, total) => {
            this.setStatusBar(t("status.downloading", { current, total }));
          },
          () => this.savePluginData(),
        );
        break;
      }

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

/** drive9 "9" glyph as SVG for Obsidian's addIcon (100×100 viewBox, currentColor for theme compat). */
const DRIVE9_ICON_SVG = `<g transform="translate(13.4,3) scale(1.136)"><path d="M366.829 88L393.478 56.8333C392.954 57.3571 392.037 57.8809 390.728 58.4048C389.418 58.8849 387.694 59.125 385.555 59.125C381.059 59.125 376.716 57.9683 372.525 55.6548C368.378 53.3413 364.952 50.0456 362.246 45.7679C359.583 41.4464 358.251 36.2956 358.251 30.3155C358.251 24.4226 359.692 19.2064 362.573 14.6667C365.497 10.127 369.382 6.54762 374.228 3.92858C379.073 1.30953 384.442 0 390.335 0C396.359 0 401.793 1.24405 406.638 3.73214C411.484 6.17659 415.347 9.73413 418.228 14.4048C421.152 19.0318 422.615 24.5754 422.615 31.0357C422.615 38.1508 421.458 44.1528 419.144 49.0417C416.831 53.8869 414.19 58.0992 411.222 61.6786L389.746 88H366.829ZM390.4 43.4762C392.976 43.4762 395.289 42.8651 397.341 41.6429C399.436 40.4206 401.095 38.7619 402.317 36.6667C403.583 34.5714 404.216 32.2579 404.216 29.7262C404.216 27.1944 403.583 24.881 402.317 22.7857C401.095 20.6905 399.436 19.0317 397.341 17.8095C395.246 16.5873 392.932 15.9762 390.4 15.9762C387.869 15.9762 385.555 16.6091 383.46 17.875C381.408 19.0972 379.749 20.7559 378.484 22.8512C377.261 24.9028 376.65 27.1944 376.65 29.7262C376.65 32.2579 377.261 34.5714 378.484 36.6667C379.749 38.7619 381.43 40.4206 383.525 41.6429C385.621 42.8651 387.912 43.4762 390.4 43.4762Z" fill="currentColor" transform="translate(-358.251,0)"/></g>`;

function generateActorId(): string {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return `obsidian-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
