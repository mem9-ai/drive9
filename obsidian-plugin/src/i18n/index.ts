import en from "./en";
import zhCN from "./zh-CN";

type MessageKey = keyof typeof en;
type Messages = Record<string, string>;

const locales: Record<string, Messages> = {
  en,
  "zh-CN": zhCN,
  zh: zhCN,
};

let currentLocale: Messages = en;

/**
 * Set the active locale. Call once at plugin load.
 * Falls back to English for unknown locales.
 */
export function setLocale(locale: string): void {
  currentLocale = locales[locale] ?? locales[locale.split("-")[0]] ?? en;
}

/**
 * Translate a message key with optional interpolation.
 * Usage: t("status.syncing", { count: 5 })
 */
export function t(key: MessageKey, params?: Record<string, string | number>): string {
  let msg = currentLocale[key] ?? en[key] ?? key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      msg = msg.replace(new RegExp(`\\{${k}\\}`, "g"), String(v));
    }
  }
  return msg;
}
