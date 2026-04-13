/**
 * Check if a vault path should be ignored for sync.
 * Patterns support:
 *   - "*.ext" — match extension anywhere
 *   - "dir/**" — match everything under dir
 *   - exact filename — match by name component
 */
export function shouldIgnore(path: string, patterns: string[]): boolean {
  for (const pattern of patterns) {
    if (matchPattern(path, pattern)) return true;
  }
  return false;
}

function matchPattern(path: string, pattern: string): boolean {
  // "dir/**" — prefix match
  if (pattern.endsWith("/**")) {
    const prefix = pattern.slice(0, -3);
    if (path === prefix || path.startsWith(prefix + "/")) return true;
    return false;
  }

  // "*.ext" — extension match
  if (pattern.startsWith("*.")) {
    const ext = pattern.slice(1); // ".ext"
    return path.endsWith(ext);
  }

  // exact name match (matches the file name component)
  const name = path.split("/").pop() || "";
  if (name === pattern) return true;

  // exact path match
  return path === pattern;
}
