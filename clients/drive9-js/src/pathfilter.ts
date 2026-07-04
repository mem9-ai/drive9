// pathfilter.ts — path-pattern matching for archive and bulk operations.
// Mirrors the Go pkg/pathfilter semantics: three pattern forms are supported,
//   - **/x/**   matches any path containing the x subpath (e.g. **/node_modules/**)
//   - prefix/** matches everything under a prefix (e.g. dist/**)
//   - name      exact name or glob (e.g. *.log, go.mod)
// Patterns are canonicalized (whitespace-trimmed, leading "/" stripped) before
// compilation; runtime paths are matched against the same canonical form.

/** A compiled path-filter pattern. */
export interface Pattern {
  raw: string;
  kind: "subpath" | "prefix" | "exact";
  // subpath form: segments to match as a contiguous subsequence.
  subpath?: string[];
  // prefix form: the leading directory prefix.
  prefix?: string;
  // exact form: the literal/glob pattern (matched via minimatch-style glob).
  exact?: string;
}

/** Bidirectional include/exclude filter. */
export interface Matcher {
  include: Pattern[];
  exclude: Pattern[];
  /** Override restores a path that exclude would drop (profile [remote] rules). */
  override: Pattern[];
}

export interface MatcherOptions {
  include?: string[];
  exclude?: string[];
  override?: string[];
}

function trimSlashes(s: string): string {
  return s.replace(/^\/+/, "").replace(/\/+$/, "");
}

function canonical(value: string): string {
  return trimSlashes(value.trim());
}

function splitSegments(value: string): string[] {
  const c = canonical(value);
  if (c === "" || c === ".") return [];
  return c.split("/").filter((s) => s.length > 0);
}

function containsSubpath(segments: string[], subpath: string[]): boolean {
  if (subpath.length === 0 || segments.length < subpath.length) return false;
  for (let start = 0; start <= segments.length - subpath.length; start++) {
    let matched = true;
    for (let i = 0; i < subpath.length; i++) {
      if (segments[start + i] !== subpath[i]) {
        matched = false;
        break;
      }
    }
    if (matched) return true;
  }
  return false;
}

/** Minimal glob matcher supporting * (single-segment wildcard). */
function globMatch(pattern: string, value: string): boolean {
  if (pattern === value) return true;
  // Convert glob to regex: * → [^/]*, escape regex specials.
  let re = "^";
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i];
    if (c === "*") re += "[^/]*";
    else if ("\\^$.+?()[]{}|".includes(c)) re += "\\" + c;
    else re += c;
  }
  re += "$";
  return new RegExp(re).test(value);
}

/** Compile a single pattern string. Throws on invalid input. */
export function compile(raw: string): Pattern {
  const cleaned = canonical(raw);
  if (cleaned === "") throw new Error(`invalid pattern ${JSON.stringify(raw)}: empty after canonicalization`);
  if (cleaned.startsWith("**/")) {
    let rest = cleaned.slice(3);
    if (rest.endsWith("/**")) rest = rest.slice(0, -3);
    if (rest.endsWith("/")) rest = rest.slice(0, -1);
    if (rest !== "") {
      return { raw, kind: "subpath", subpath: splitSegments(rest) };
    }
  }
  if (cleaned.endsWith("/**")) {
    return { raw, kind: "prefix", prefix: cleaned.slice(0, -3) };
  }
  return { raw, kind: "exact", exact: cleaned };
}

/** Compile a list of patterns, skipping blank entries. Invalid patterns are dropped. */
export function compileAll(patterns: string[]): Pattern[] {
  const out: Pattern[] = [];
  for (const raw of patterns) {
    const trimmed = raw.trim();
    if (trimmed === "") continue;
    try {
      out.push(compile(trimmed));
    } catch {
      // tolerate invalid patterns at compile time; validate() surfaces them
    }
  }
  return out;
}

/** Validate patterns and return the first error, or null if all valid. */
export function validate(...patternLists: string[][]): Error | null {
  for (const list of patternLists) {
    for (const raw of list) {
      const trimmed = raw.trim();
      if (trimmed === "") continue;
      try {
        compile(trimmed);
      } catch (e) {
        return e instanceof Error ? e : new Error(String(e));
      }
    }
  }
  return null;
}

/** Match a canonicalized path against a single pattern. */
export function matchPattern(p: Pattern, value: string): boolean {
  const cleaned = canonical(value);
  return matchCanonical(p, cleaned);
}

function matchCanonical(p: Pattern, cleaned: string): boolean {
  if (p.kind === "subpath" && p.subpath) {
    return containsSubpath(splitSegments(cleaned), p.subpath);
  }
  if (p.kind === "prefix" && p.prefix !== undefined) {
    return cleaned === p.prefix || cleaned.startsWith(p.prefix + "/");
  }
  if (p.kind === "exact" && p.exact !== undefined) {
    return globMatch(p.exact, cleaned) || cleaned === p.exact;
  }
  return false;
}

function matchesAny(patterns: Pattern[], value: string): boolean {
  for (const p of patterns) {
    if (matchPattern(p, value)) return true;
  }
  return false;
}

/** Build a Matcher from option lists. */
export function newMatcher(opts: MatcherOptions): Matcher {
  return {
    include: compileAll(opts.include ?? []),
    exclude: compileAll(opts.exclude ?? []),
    override: compileAll(opts.override ?? []),
  };
}

/**
 * Match reports whether a path should be included.
 *
 * 1. If override matches → include (true).
 * 2. Else if exclude matches → drop (false).
 * 3. Else if include is non-empty and no include pattern matches → drop (false).
 * 4. Otherwise → include (true).
 */
export function match(m: Matcher, path: string): boolean {
  if (m.override.length > 0 && matchesAny(m.override, path)) return true;
  if (matchesAny(m.exclude, path)) return false;
  if (m.include.length > 0 && !matchesAny(m.include, path)) return false;
  return true;
}

export function hasInclude(m: Matcher): boolean {
  return m.include.length > 0;
}

export function hasExclude(m: Matcher): boolean {
  return m.exclude.length > 0;
}