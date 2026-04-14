import { App, PluginSettingTab, Setting, Notice } from "obsidian";
import type Drive9Plugin from "./main";
import { Drive9Client, sanitizeError } from "./client";
import { t } from "./i18n";

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

    new Setting(containerEl)
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
      });

    new Setting(containerEl)
      .setName(t("settings.testConnection"))
      .setDesc(t("settings.testConnection.desc"))
      .addButton((btn) =>
        btn.setButtonText(t("settings.testConnection.btn")).onClick(async () => {
          await this.testConnection();
        }),
      );

    new Setting(containerEl)
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

    new Setting(containerEl)
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

    new Setting(containerEl)
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

    new Setting(containerEl)
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
    if (!this.plugin.settings.serverUrl) {
      new Notice(t("settings.enterServerUrl"));
      return;
    }
    if (!this.plugin.settings.apiKey) {
      new Notice(t("settings.enterApiKey"));
      return;
    }
    try {
      const testClient = new Drive9Client(
        this.plugin.settings.serverUrl,
        this.plugin.settings.apiKey,
      );
      await testClient.ping();
      new Notice(t("settings.connectionSuccess"));
    } catch (e) {
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
}

