import { App, SuggestModal, Notice } from "obsidian";
import { Drive9Client, Drive9Error, sanitizeError } from "./client";
import { isTextFile } from "./conflict-modal";
import type { SearchResult } from "./types";

const DEBOUNCE_MS = 300;
const MIN_QUERY_LENGTH = 3;
const SEARCH_LIMIT = 20;
const PREVIEW_MAX_CHARS = 200;

/** Sentinel results used to render empty/loading/no-results states. */
const EMPTY_STATE: SearchResult = { path: "__drive9_empty__", name: "", size_bytes: 0 };
const LOADING_STATE: SearchResult = { path: "__drive9_loading__", name: "", size_bytes: 0 };
const NO_RESULTS_STATE: SearchResult = { path: "__drive9_no_results__", name: "", size_bytes: 0 };

function isSentinel(r: SearchResult): boolean {
  return r.path.startsWith("__drive9_");
}

export class Drive9SearchModal extends SuggestModal<SearchResult> {
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private lastQuery = "";
  private cachedResults: SearchResult[] = [];
  private previewCache = new Map<string, string | null>();
  private searching = false;

  constructor(
    app: App,
    private client: Drive9Client,
  ) {
    super(app);
    this.setPlaceholder("Search files in drive9...");
    this.setInstructions([
      { command: "↑↓", purpose: "navigate" },
      { command: "↵", purpose: "open file" },
      { command: "esc", purpose: "dismiss" },
    ]);
  }

  getSuggestions(query: string): SearchResult[] | Promise<SearchResult[]> {
    if (query.length < MIN_QUERY_LENGTH) {
      this.lastQuery = "";
      this.cachedResults = [];
      return [EMPTY_STATE];
    }

    if (query === this.lastQuery && this.cachedResults.length > 0) {
      return this.cachedResults;
    }
    if (query === this.lastQuery && !this.searching) {
      return [NO_RESULTS_STATE];
    }

    return new Promise<SearchResult[]>((resolve) => {
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.searching = true;
      this.debounceTimer = setTimeout(async () => {
        try {
          const results = await this.client.grep(query, SEARCH_LIMIT);
          this.lastQuery = query;
          this.cachedResults = results;
          this.searching = false;
          resolve(results.length > 0 ? results : [NO_RESULTS_STATE]);
        } catch (e) {
          this.searching = false;
          if (e instanceof Drive9Error) {
            new Notice(`drive9 search: ${e.message}`);
          } else {
            new Notice(`drive9 search: ${sanitizeError(e instanceof Error ? e.message : String(e))}`);
          }
          resolve([]);
        }
      }, DEBOUNCE_MS);
      // Show loading state immediately while debouncing
      resolve([LOADING_STATE]);
    });
  }

  renderSuggestion(result: SearchResult, el: HTMLElement): void {
    if (result === EMPTY_STATE) {
      el.createDiv({ cls: "drive9-search-state", text: "Type at least 3 characters to search" });
      el.style.color = "var(--text-muted)";
      el.style.fontStyle = "italic";
      return;
    }
    if (result === LOADING_STATE) {
      el.createDiv({ cls: "drive9-search-state", text: "Searching..." });
      el.style.color = "var(--text-muted)";
      el.style.fontStyle = "italic";
      return;
    }
    if (result === NO_RESULTS_STATE) {
      el.createDiv({ cls: "drive9-search-state", text: "No results found" });
      el.style.color = "var(--text-muted)";
      el.style.fontStyle = "italic";
      return;
    }

    const container = el.createDiv({ cls: "drive9-search-result" });

    container.createDiv({
      cls: "drive9-search-path",
      text: result.path,
    });

    const meta = container.createDiv({ cls: "drive9-search-meta" });
    meta.style.fontSize = "0.85em";
    meta.style.color = "var(--text-muted)";

    const parts: string[] = [];
    if (result.size_bytes > 0) {
      parts.push(formatSize(result.size_bytes));
    }
    if (result.score != null) {
      parts.push(`score: ${result.score.toFixed(2)}`);
    }
    if (parts.length > 0) {
      meta.setText(parts.join(" · "));
    }

    // Lazy preview: fetch snippet for this result
    if (isTextFile(result.path) && result.size_bytes > 0) {
      const previewEl = container.createDiv({ cls: "drive9-search-preview" });
      previewEl.style.fontSize = "0.8em";
      previewEl.style.color = "var(--text-faint)";
      previewEl.style.marginTop = "2px";
      previewEl.style.whiteSpace = "nowrap";
      previewEl.style.overflow = "hidden";
      previewEl.style.textOverflow = "ellipsis";

      const cached = this.previewCache.get(result.path);
      if (cached !== undefined) {
        if (cached) previewEl.setText(cached);
      } else {
        previewEl.setText("loading preview...");
        this.fetchPreview(result.path).then((text) => {
          if (text) {
            previewEl.setText(text);
          } else {
            previewEl.remove();
          }
        });
      }
    }
  }

  onChooseSuggestion(result: SearchResult): void {
    if (isSentinel(result)) return;
    const file = this.app.vault.getAbstractFileByPath(result.path);
    if (file) {
      void this.app.workspace.openLinkText(result.path, "", false);
    } else {
      new Notice(`drive9: file not found locally — ${result.path}`);
    }
  }

  onClose(): void {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
  }

  private async fetchPreview(path: string): Promise<string | null> {
    if (this.previewCache.has(path)) {
      return this.previewCache.get(path) ?? null;
    }
    try {
      const data = await this.client.read(path);
      const text = new TextDecoder().decode(data.slice(0, PREVIEW_MAX_CHARS * 4));
      const preview = text.slice(0, PREVIEW_MAX_CHARS).replace(/\n/g, " ").trim();
      this.previewCache.set(path, preview || null);
      return preview || null;
    } catch {
      this.previewCache.set(path, null);
      return null;
    }
  }
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
