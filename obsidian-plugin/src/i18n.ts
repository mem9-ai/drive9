/**
 * Lightweight i18n helper for drive9 Obsidian plugin.
 *
 * - Follows Obsidian's language setting (moment.locale())
 * - Falls back to English for unknown locales
 * - Supports simple {param} interpolation
 */
import en from "./locales/en";
import zhCN from "./locales/zh-CN";
import type { LocaleKeys } from "./locales/en";

type LocaleDict = Record<LocaleKeys, string>;

const locales: Record<string, LocaleDict> = {
  en,
  "zh-cn": zhCN,
  zh: zhCN,
};

let currentLocale: LocaleDict = en;

/**
 * Detect Obsidian's language and set the active locale.
 * Call once during plugin onload().
 */
export function initLocale(): void {
  // Obsidian uses moment.locale() which reflects the user's language setting.
  // Common values: "en", "zh-cn", "zh-tw", "ja", "ko", "de", "fr", etc.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const m = (globalThis as any).moment;
  const lang = (m?.locale?.() ?? "en").toLowerCase();

  // Try exact match first, then base language
  currentLocale = locales[lang] ?? locales[lang.split("-")[0]] ?? en;
}

/**
 * Translate a key with optional parameter interpolation.
 *
 * Usage:
 *   t("notice.uploading", { count: 42 })
 *   // → "drive9: uploading 42 files to drive9..."
 */
export function t(key: LocaleKeys, params?: Record<string, string | number>): string {
  let text = currentLocale[key] ?? en[key] ?? key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      text = text.replace(new RegExp(`\\{${k}\\}`, "g"), String(v));
    }
  }
  return text;
}
