import { App, Modal } from "obsidian";

export type ConflictChoice = "keep_local" | "keep_remote" | "keep_both";

interface ConflictInfo {
  path: string;
  localSize: number;
  localMtime: number;
  remoteSize: number;
  remoteMtime: number;
  isText: boolean;
  diffPreview?: string;
}

/**
 * Modal shown when automatic 3-way merge fails or for binary files.
 * Resolves with the user's choice.
 */
export class ConflictModal extends Modal {
  private resolve: ((choice: ConflictChoice | null) => void) | null = null;

  constructor(
    app: App,
    private info: ConflictInfo,
  ) {
    super(app);
  }

  open(): Promise<ConflictChoice | null> {
    return new Promise((resolve) => {
      this.resolve = resolve;
      super.open();
    });
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.empty();
    contentEl.addClass("drive9-conflict-modal");

    contentEl.createEl("h2", { text: "Sync Conflict" });
    contentEl.createEl("p", {
      text: this.info.path,
      cls: "drive9-conflict-path",
    });

    const table = contentEl.createEl("table", { cls: "drive9-conflict-table" });
    const header = table.createEl("tr");
    header.createEl("th", { text: "" });
    header.createEl("th", { text: "Local" });
    header.createEl("th", { text: "Remote" });

    const sizeRow = table.createEl("tr");
    sizeRow.createEl("td", { text: "Size" });
    sizeRow.createEl("td", { text: formatSize(this.info.localSize) });
    sizeRow.createEl("td", { text: formatSize(this.info.remoteSize) });

    const mtimeRow = table.createEl("tr");
    mtimeRow.createEl("td", { text: "Modified" });
    mtimeRow.createEl("td", { text: formatTime(this.info.localMtime) });
    mtimeRow.createEl("td", { text: formatTime(this.info.remoteMtime) });

    if (this.info.diffPreview) {
      const details = contentEl.createEl("details");
      details.createEl("summary", { text: "Diff preview" });
      const pre = details.createEl("pre", { cls: "drive9-conflict-diff" });
      pre.createEl("code", { text: this.info.diffPreview });
    }

    const actions = contentEl.createEl("div", { cls: "drive9-conflict-actions" });

    const localBtn = actions.createEl("button", { text: "Keep Local", cls: "mod-warning" });
    localBtn.addEventListener("click", () => this.choose("keep_local"));

    const remoteBtn = actions.createEl("button", { text: "Keep Remote" });
    remoteBtn.addEventListener("click", () => this.choose("keep_remote"));

    const bothBtn = actions.createEl("button", { text: "Keep Both" });
    bothBtn.addEventListener("click", () => this.choose("keep_both"));
  }

  onClose(): void {
    if (this.resolve) {
      this.resolve(null);
      this.resolve = null;
    }
    this.contentEl.empty();
  }

  private choose(choice: ConflictChoice): void {
    if (this.resolve) {
      const r = this.resolve;
      this.resolve = null;
      this.close();
      r(choice);
    }
  }
}

export function createConflictInfo(
  path: string,
  localSize: number,
  localMtime: number,
  remoteSize: number,
  remoteMtime: number,
  diffPreview?: string,
): ConflictInfo {
  const ext = path.split(".").pop()?.toLowerCase() ?? "";
  const textExtensions = new Set([
    "md", "txt", "json", "yaml", "yml", "toml", "csv", "xml", "html",
    "css", "js", "ts", "py", "rb", "go", "rs", "java", "c", "cpp", "h",
    "sh", "bash", "zsh", "fish", "lua", "tex", "bib", "ini", "cfg",
    "conf", "log", "svg",
  ]);
  return {
    path,
    localSize,
    localMtime,
    remoteSize,
    remoteMtime,
    isText: textExtensions.has(ext),
    diffPreview,
  };
}

export function isTextFile(path: string): boolean {
  const ext = path.split(".").pop()?.toLowerCase() ?? "";
  const textExtensions = new Set([
    "md", "txt", "json", "yaml", "yml", "toml", "csv", "xml", "html",
    "css", "js", "ts", "py", "rb", "go", "rs", "java", "c", "cpp", "h",
    "sh", "bash", "zsh", "fish", "lua", "tex", "bib", "ini", "cfg",
    "conf", "log", "svg",
  ]);
  return textExtensions.has(ext);
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTime(ms: number): string {
  if (ms === 0) return "unknown";
  return new Date(ms).toLocaleString();
}
