import { App, PluginSettingTab, Setting, Notice } from "obsidian";
import type Drive9Plugin from "./main";
import { Drive9Client, Drive9Error, sanitizeError } from "./client";

const DEFAULT_SERVER_URL = "https://api.drive9.ai";
const PROVISION_POLL_INTERVAL = 2000;
const PROVISION_POLL_TIMEOUT = 120_000;

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

    containerEl.createEl("h2", { text: "drive9 Settings" });

    this.renderQuickSetup(containerEl);

    new Setting(containerEl)
      .setName("Server URL")
      .setDesc("drive9 server address (e.g. https://api.drive9.ai)")
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

    new Setting(containerEl)
      .setName("API Key")
      .setDesc("drive9 API key for authentication")
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
      });

    new Setting(containerEl)
      .setName("Test Connection")
      .setDesc("Verify server URL and API key")
      .addButton((btn) =>
        btn.setButtonText("Test").onClick(async () => {
          await this.testConnection();
        }),
      );

    new Setting(containerEl)
      .setName("Push Debounce (ms)")
      .setDesc("Delay before syncing after a file change (default: 2000)")
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

    new Setting(containerEl)
      .setName("Ignore Paths")
      .setDesc("Glob patterns to exclude from sync (one per line)")
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

    new Setting(containerEl)
      .setName("Max File Size (MB)")
      .setDesc("Skip files larger than this (default: 100)")
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

    new Setting(containerEl)
      .setName("Mobile Max File Size (MB)")
      .setDesc("Lower file size limit on mobile to avoid OOM (default: 20)")
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
    if (!this.plugin.settings.serverUrl) {
      new Notice("Please enter a server URL first");
      return;
    }
    if (!this.plugin.settings.apiKey) {
      new Notice("Please enter an API key first");
      return;
    }
    const testClient = new Drive9Client(
      this.plugin.settings.serverUrl,
      this.plugin.settings.apiKey,
    );

    // First check tenant status — if still provisioning, ping will fail with 503
    try {
      const statusResp = await testClient.getStatus();
      if (statusResp.status === "provisioning") {
        new Notice("drive9: account is still being set up. Please wait a moment and try again.");
        return;
      }
      if (statusResp.status === "failed") {
        new Notice("drive9: account provisioning failed. Please create a new account.");
        return;
      }
    } catch {
      // Status check failed — fall through to ping which will give a more specific error
    }

    try {
      await testClient.ping();
      new Notice("drive9: connection successful!");
    } catch (e) {
      if (e instanceof Drive9Error && e.status === 503) {
        new Notice("drive9: account is still being set up. Please wait a moment and try again.");
        return;
      }
      const msg = e instanceof Error ? e.message : String(e);
      new Notice(`drive9: connection failed — ${sanitizeError(msg)}`);
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
        this.addGitignoreWarning(containerEl, "No .gitignore found. Your API key in .obsidian/ could be committed to git.");
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
        this.addGitignoreWarning(containerEl, ".gitignore does not cover .obsidian/ — your API key could be committed to git.");
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
    warning.createEl("strong", { text: "⚠ Security Warning: " });
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
      text: "⚡ Quick Setup",
      cls: "drive9-quick-setup-title",
    }).style.fontWeight = "bold";

    if (alreadyConfigured) {
      wrapper.createEl("div", {
        text: "Already configured — your account is connected.",
        cls: "drive9-quick-setup-desc",
      }).style.cssText = "margin-top: 4px; color: var(--text-muted);";
      return;
    }

    wrapper.createEl("div", {
      text: "Create a free drive9 account in one click.",
      cls: "drive9-quick-setup-desc",
    }).style.marginTop = "4px";

    const btnRow = wrapper.createEl("div");
    btnRow.style.marginTop = "8px";

    const btn = btnRow.createEl("button", { text: "Create Account" });
    btn.classList.add("mod-cta");

    const statusEl = wrapper.createEl("div", { cls: "drive9-quick-setup-status" });
    statusEl.style.cssText = "margin-top: 8px; font-size: 0.85em; color: var(--text-muted); display: none;";

    wrapper.createEl("div", {
      text: "Or enter an existing server URL and API key below.",
      cls: "drive9-quick-setup-alt",
    }).style.cssText = "margin-top: 8px; font-size: 0.85em; color: var(--text-muted);";

    btn.addEventListener("click", () => {
      void this.doProvision(btn, statusEl);
    });
  }

  private async doProvision(btn: HTMLButtonElement, statusEl: HTMLElement): Promise<void> {
    btn.disabled = true;
    btn.setText("Creating account...");
    statusEl.style.display = "block";
    statusEl.setText("Connecting to drive9 servers...");

    try {
      // Set server URL if empty
      if (!this.plugin.settings.serverUrl) {
        this.plugin.settings.serverUrl = DEFAULT_SERVER_URL;
      }

      const client = new Drive9Client(this.plugin.settings.serverUrl, "");
      statusEl.setText("Provisioning database (this may take 10-30 seconds)...");
      const result = await client.provision();

      // Save the API key
      this.plugin.settings.apiKey = result.api_key;
      await this.plugin.savePluginData();

      new Notice("drive9: account created! Setting up...");
      statusEl.setText("Account created. Initializing database schema...");

      // Poll for status to become active
      const ready = await this.pollUntilActive(statusEl);

      if (ready) {
        new Notice("drive9: ready! Sync will start automatically.");
        statusEl.setText("Ready! Starting sync...");
        await this.plugin.startSyncIfReady();
      } else {
        new Notice("drive9: workspace is still being set up. Sync will start when ready.");
        statusEl.setText("Workspace is still being set up. Sync will start when ready.");
      }

      // Re-render to show "Already configured" state
      this.display();
    } catch (e) {
      btn.disabled = false;
      btn.setText("Create Account");
      const msg = e instanceof Error ? sanitizeError(e.message) : String(e);
      statusEl.setText(`Failed: ${msg}`);
      new Notice(`drive9: provision failed — ${msg}`);
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
      statusEl.setText(`Initializing database schema (${secs}s)...`);
      try {
        const resp = await client.getStatus();
        if (resp.status === "active") {
          return true;
        }
        if (resp.status === "failed") {
          statusEl.setText("Provisioning failed. Please try again.");
          new Notice("drive9: provisioning failed on server side.");
          return false;
        }
      } catch {
        // Status endpoint may not be ready yet — keep polling.
      }
    }
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

