import { App, PluginSettingTab, Setting, Notice } from "obsidian";
import type Drive9Plugin from "./main";
import { Drive9Client, Drive9Error, sanitizeError } from "./client";
import { t } from "./i18n";

const PROVISION_POLL_INTERVAL = 2000;
const PROVISION_POLL_TIMEOUT = 600_000; // 10 minutes — matches server retry window

export class Drive9SettingTab extends PluginSettingTab {
  private validateTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    app: App,
    private plugin: Drive9Plugin,
  ) {
    super(app, plugin);
  }

  display(): void {
    const { containerEl } = this;
    containerEl.empty();

    containerEl.createEl("h2", { text: t("settings.title") });

    // --- Quick Setup ---
    this.renderQuickSetup(containerEl);

    // --- Main settings: API Key + Test Connection ---

    new Setting(containerEl)
      .setName(t("settings.apiKey"))
      .setDesc(t("settings.apiKey.desc"))
      .addText((text) => {
        text.inputEl.type = "password";
        text.inputEl.autocomplete = "off";
        text
          .setPlaceholder("your-api-key")
          .setValue(this.plugin.settings.apiKey)
          .onChange(async (value) => {
            this.plugin.settings.apiKey = value.trim();
            await this.plugin.savePluginData();
            this.scheduleValidation();
          });
      })
      .addExtraButton((btn) =>
        btn.setIcon("eye").setTooltip(t("settings.apiKey.toggle")).onClick(() => {
          const input = btn.extraSettingsEl.parentElement?.querySelector("input");
          if (!input) return;
          const hidden = input.type === "password";
          input.type = hidden ? "text" : "password";
          btn.setIcon(hidden ? "eye-off" : "eye");
        }),
      )
      .addExtraButton((btn) =>
        btn.setIcon("copy").setTooltip(t("settings.apiKey.copy")).onClick(() => {
          navigator.clipboard.writeText(this.plugin.settings.apiKey);
          new Notice(t("settings.apiKey.copied"));
        }),
      );

    new Setting(containerEl)
      .setName(t("settings.testConnection"))
      .setDesc(t("settings.testConnection.desc"))
      .addButton((btn) =>
        btn.setButtonText(t("settings.testConnection.btn")).onClick(async () => {
          await this.testConnection();
        }),
      );

    // --- Advanced settings (collapsed) ---

    const details = containerEl.createEl("details");
    details.createEl("summary", { text: t("settings.advanced"), cls: "drive9-advanced-summary" });

    new Setting(details)
      .setName(t("settings.serverUrl"))
      .setDesc(t("settings.serverUrl.desc"))
      .addText((text) =>
        text
          .setPlaceholder("https://api.drive9.ai")
          .setValue(this.plugin.settings.serverUrl)
          .onChange(async (value) => {
            this.plugin.settings.serverUrl = value.trim();
            await this.plugin.savePluginData();
            this.scheduleValidation();
          }),
      );

    new Setting(details)
      .setName(t("settings.pushDebounce"))
      .setDesc(t("settings.pushDebounce.desc"))
      .addText((text) =>
        text
          .setPlaceholder("2000")
          .setValue(String(this.plugin.settings.pushDebounce))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n >= 500) {
              this.plugin.settings.pushDebounce = n;
              await this.plugin.savePluginData();
            }
          }),
      );

    new Setting(details)
      .setName(t("settings.ignorePaths"))
      .setDesc(t("settings.ignorePaths.desc"))
      .addTextArea((text) =>
        text
          .setPlaceholder(".obsidian/**\n.trash/**")
          .setValue(this.plugin.settings.ignorePaths.join("\n"))
          .onChange(async (value) => {
            this.plugin.settings.ignorePaths = value
              .split("\n")
              .map((s) => s.trim())
              .filter((s) => s.length > 0);
            await this.plugin.savePluginData();
          }),
      );

    new Setting(details)
      .setName(t("settings.maxFileSize"))
      .setDesc(t("settings.maxFileSize.desc"))
      .addText((text) =>
        text
          .setPlaceholder("100")
          .setValue(String(Math.round(this.plugin.settings.maxFileSize / (1024 * 1024))))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n >= 1) {
              this.plugin.settings.maxFileSize = n * 1024 * 1024;
              await this.plugin.savePluginData();
            }
          }),
      );

    new Setting(details)
      .setName(t("settings.mobileMaxFileSize"))
      .setDesc(t("settings.mobileMaxFileSize.desc"))
      .addText((text) =>
        text
          .setPlaceholder("20")
          .setValue(String(Math.round(this.plugin.settings.mobileMaxFileSize / (1024 * 1024))))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n >= 1) {
              this.plugin.settings.mobileMaxFileSize = n * 1024 * 1024;
              await this.plugin.savePluginData();
            }
          }),
      );

    // .gitignore warning
    void this.checkGitignore(containerEl);
  }

  private scheduleValidation(): void {
    if (this.validateTimer) clearTimeout(this.validateTimer);
    this.validateTimer = setTimeout(() => {
      this.validateTimer = null;
      void this.testConnection();
    }, 1500);
  }

  private async testConnection(): Promise<void> {
    if (!this.plugin.settings.apiKey) {
      new Notice(t("settings.enterApiKey"));
      return;
    }
    const testClient = new Drive9Client(
      this.plugin.settings.serverUrl,
      this.plugin.settings.apiKey,
    );

    try {
      const statusResp = await testClient.getStatus();
      if (statusResp.status === "provisioning") {
        new Notice(t("settings.provisioningInProgress"));
        return;
      }
      if (statusResp.status === "failed") {
        new Notice(t("settings.provisioningFailed"));
        return;
      }
    } catch {
      // Status check failed — fall through to ping
    }

    try {
      await testClient.ping();
      new Notice(t("settings.connectionSuccess"));
      // If sync hasn't started yet (e.g. provision timed out earlier), kick it off
      // in the background so the Test button returns immediately.
      void this.plugin.startSyncIfReady();
    } catch (e) {
      if (e instanceof Drive9Error && e.status === 503) {
        new Notice(t("settings.provisioningInProgress"));
        return;
      }
      const msg = e instanceof Error ? e.message : String(e);
      new Notice(t("settings.connectionFailed", { error: sanitizeError(msg) }));
    }
  }

  private async checkGitignore(containerEl: HTMLElement): Promise<void> {
    const adapter = this.app.vault.adapter;
    const vaultRoot = (adapter as { getBasePath?: () => string }).getBasePath?.();
    if (!vaultRoot) return;

    try {
      const gitignorePath = `${vaultRoot}/.gitignore`;
      const fs = (globalThis as { require?: (name: string) => { existsSync: (p: string) => boolean; readFileSync: (p: string, e: string) => string } }).require?.("fs");
      if (!fs) return;

      if (!fs.existsSync(`${vaultRoot}/.git`)) return;

      if (!fs.existsSync(gitignorePath)) {
        this.addGitignoreWarning(containerEl, t("settings.gitignoreNoFile"));
        return;
      }

      const content = fs.readFileSync(gitignorePath, "utf-8");
      const lines = content.split("\n").map((l: string) => l.trim());
      const coversObsidian = lines.some((l: string) => {
        // Strip comments and empty lines
        if (!l || l.startsWith("#")) return false;
        // Match common patterns that cover .obsidian/ or the plugin data dir
        return /^\/?\.obsidian(\/.*)?$/.test(l)
          || l === ".obsidian"
          || l === ".obsidian/"
          || l === ".obsidian/**"
          || l === ".obsidian/*";
      });

      if (!coversObsidian) {
        this.addGitignoreWarning(containerEl, t("settings.gitignoreNoCoverage"));
      }
    } catch {
      // Not on desktop or fs access failed — skip warning
    }
  }

  private addGitignoreWarning(containerEl: HTMLElement, message: string): void {
    const warning = containerEl.createEl("div", { cls: "drive9-gitignore-warning" });
    warning.style.padding = "8px 12px";
    warning.style.marginTop = "12px";
    warning.style.borderRadius = "4px";
    warning.style.backgroundColor = "var(--background-modifier-error)";
    warning.style.color = "var(--text-on-accent)";
    warning.createEl("strong", { text: t("settings.securityWarning") });
    warning.createSpan({ text: message });
  }

  private renderQuickSetup(containerEl: HTMLElement): void {
    const alreadyConfigured = !!(this.plugin.settings.serverUrl && this.plugin.settings.apiKey);

    const wrapper = containerEl.createEl("div", { cls: "drive9-quick-setup" });
    wrapper.style.padding = "12px 16px";
    wrapper.style.marginBottom = "16px";
    wrapper.style.borderRadius = "8px";
    wrapper.style.border = "1px solid var(--background-modifier-border)";
    wrapper.style.backgroundColor = "var(--background-secondary)";

    wrapper.createEl("div", {
      text: t("settings.quickSetup"),
      cls: "drive9-quick-setup-title",
    }).style.fontWeight = "bold";

    if (alreadyConfigured) {
      wrapper.createEl("div", {
        text: t("settings.quickSetupConnected"),
        cls: "drive9-quick-setup-desc",
      }).style.cssText = "margin-top: 4px; color: var(--text-muted);";
      return;
    }

    // --- Path 1: Create new account ---
    wrapper.createEl("div", {
      text: t("settings.quickSetupNewUser"),
      cls: "drive9-quick-setup-desc",
    }).style.cssText = "margin-top: 8px; font-size: 0.9em;";

    const createBtnRow = wrapper.createEl("div");
    createBtnRow.style.marginTop = "6px";

    const createBtn = createBtnRow.createEl("button", { text: t("settings.createAccount") });
    createBtn.classList.add("mod-cta");

    const statusEl = wrapper.createEl("div", { cls: "drive9-quick-setup-status" });
    statusEl.style.cssText = "margin-top: 8px; font-size: 0.85em; color: var(--text-muted); display: none;";

    // --- Divider ---
    const divider = wrapper.createEl("div", { cls: "drive9-quick-setup-divider" });
    divider.style.cssText = "display: flex; align-items: center; margin: 12px 0; gap: 8px;";
    const line = () => {
      const el = divider.createEl("div");
      el.style.cssText = "flex: 1; height: 1px; background: var(--background-modifier-border);";
    };
    line();
    divider.createEl("span", { text: t("settings.quickSetupOr") }).style.cssText =
      "font-size: 0.8em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em;";
    line();

    // --- Path 2: Existing API key ---
    wrapper.createEl("div", {
      text: t("settings.quickSetupExistingUser"),
      cls: "drive9-quick-setup-desc",
    }).style.cssText = "font-size: 0.9em;";

    wrapper.createEl("div", {
      text: t("settings.quickSetupExistingHint"),
      cls: "drive9-quick-setup-hint",
    }).style.cssText = "font-size: 0.8em; color: var(--text-muted); margin-top: 2px;";

    const keyRow = wrapper.createEl("div", { cls: "drive9-quick-setup-key-row" });
    keyRow.style.cssText = "display: flex; gap: 8px; margin-top: 6px; align-items: center;";

    const keyInput = keyRow.createEl("input", { type: "password", placeholder: t("settings.quickSetupKeyPlaceholder") });
    keyInput.style.cssText = "flex: 1; padding: 6px 8px; border-radius: 4px; border: 1px solid var(--background-modifier-border); background: var(--background-primary); color: var(--text-normal); font-size: 0.9em;";
    keyInput.autocomplete = "off";

    const connectBtn = keyRow.createEl("button", { text: t("settings.quickSetupConnect") });

    const keyStatusEl = wrapper.createEl("div", { cls: "drive9-quick-setup-key-status" });
    keyStatusEl.style.cssText = "margin-top: 6px; font-size: 0.85em; display: none;";

    connectBtn.addEventListener("click", () => {
      void this.connectExistingKey(keyInput, connectBtn, keyStatusEl);
    });

    // Also connect on Enter key
    keyInput.addEventListener("keydown", (e: KeyboardEvent) => {
      if (e.key === "Enter") {
        e.preventDefault();
        void this.connectExistingKey(keyInput, connectBtn, keyStatusEl);
      }
    });

    createBtn.addEventListener("click", () => {
      void this.doProvision(createBtn, statusEl);
    });
  }

  private async connectExistingKey(
    input: HTMLInputElement,
    btn: HTMLButtonElement,
    statusEl: HTMLElement,
  ): Promise<void> {
    if (btn.disabled) return;
    const key = input.value.trim();
    if (!key) {
      statusEl.style.display = "block";
      statusEl.style.color = "var(--text-error)";
      statusEl.setText(t("settings.enterApiKey"));
      return;
    }

    btn.disabled = true;
    btn.setText(t("settings.quickSetupConnecting"));
    statusEl.style.display = "block";
    statusEl.style.color = "var(--text-muted)";
    statusEl.setText(t("settings.quickSetupVerifying"));

    const testClient = new Drive9Client(
      this.plugin.settings.serverUrl,
      key,
    );

    try {
      await testClient.ping();

      // Key is valid — save and start sync
      this.plugin.settings.apiKey = key;
      await this.plugin.savePluginData();

      statusEl.style.color = "var(--text-success)";
      statusEl.setText(t("settings.quickSetupKeySuccess"));
      new Notice(t("settings.connectionSuccess"));

      void this.plugin.startSyncIfReady();

      // Re-render to show "connected" state
      this.display();
    } catch (e) {
      btn.disabled = false;
      btn.setText(t("settings.quickSetupConnect"));
      const msg = e instanceof Error ? e.message : String(e);
      statusEl.style.color = "var(--text-error)";
      statusEl.setText(t("settings.connectionFailed", { error: sanitizeError(msg) }));
    }
  }

  private async doProvision(btn: HTMLButtonElement, statusEl: HTMLElement): Promise<void> {
    btn.disabled = true;
    btn.setText(t("settings.creatingAccount"));
    statusEl.style.display = "block";
    statusEl.setText(t("settings.provisionConnecting"));

    try {
      const client = new Drive9Client(this.plugin.settings.serverUrl, "");
      statusEl.setText(t("settings.provisionInitializing"));
      const result = await client.provision();

      this.plugin.settings.apiKey = result.api_key;
      await this.plugin.savePluginData();

      new Notice(t("settings.provisionCreated"));
      statusEl.setText(t("settings.provisionSchema"));

      const ready = await this.pollUntilActive(statusEl);

      if (ready) {
        new Notice(t("settings.provisionReady"));
        statusEl.setText(t("settings.provisionStarting"));
        await this.plugin.startSyncIfReady();
      } else {
        new Notice(t("settings.provisionStillSetup"));
        statusEl.setText(t("settings.provisionStillSetup"));
      }

      this.display();
    } catch (e) {
      btn.disabled = false;
      btn.setText(t("settings.createAccount"));
      const msg = e instanceof Error ? sanitizeError(e.message) : String(e);
      statusEl.setText(t("settings.provisionFailed", { error: msg }));
      new Notice(t("settings.provisionFailed", { error: msg }));
    }
  }

  private async pollUntilActive(statusEl: HTMLElement): Promise<boolean> {
    const client = new Drive9Client(
      this.plugin.settings.serverUrl,
      this.plugin.settings.apiKey,
    );

    const deadline = Date.now() + PROVISION_POLL_TIMEOUT;
    let elapsed = 0;
    while (Date.now() < deadline) {
      await sleep(PROVISION_POLL_INTERVAL);
      elapsed += PROVISION_POLL_INTERVAL;
      const secs = Math.round(elapsed / 1000);
      statusEl.setText(t("settings.provisionPolling", { seconds: secs }));
      try {
        const resp = await client.getStatus();
        if (resp.status === "active") {
          return true;
        }
        if (resp.status === "failed") {
          statusEl.setText(t("settings.provisioningFailed"));
          new Notice(t("settings.provisioningFailed"));
          return false;
        }
      } catch {
        // Status endpoint may not be ready yet — keep polling
      }
    }
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
