// pathfilter.ts — path-pattern matching for archive and bulk operations.
// Mirrors the Go pkg/pathfilter semantics: three pattern forms are supported,
//   - **/x/**   matches any path containing the x subpath (e.g. **/node_modules/**)
//   - prefix/** matches everything under a prefix (e.g. dist/**)
//   - name      exact name or glob (e.g. *.log, go.mod)
// Each segment after **/ supports globs and literal equality. Recursive globs
// without /** must end at the final segment; literal rules retain subtrees.
// Patterns are canonicalized (whitespace-trimmed, leading "/" stripped) before
// compilation; runtime paths are matched against the same canonical form.

/** A compiled path-filter pattern. */
export interface Pattern {
  raw: string;
  kind: "subpath" | "prefix" | "exact";
  // subpath form: segments to match as a contiguous subsequence.
  subpath?: string[];
  subpathEndOnly?: boolean;
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

function containsSubpath(segments: string[], subpath: string[], endOnly = false): boolean {
  if (subpath.length === 0 || segments.length < subpath.length) return false;
  const first = endOnly ? segments.length - subpath.length : 0;
  for (let start = first; start <= segments.length - subpath.length; start++) {
    let matched = true;
    for (let i = 0; i < subpath.length; i++) {
      if (globMatch(subpath[i], segments[start + i])) continue;
      matched = false;
      break;
    }
    if (matched) return true;
  }
  return false;
}

/**
 * globMatch mirrors Go's path.Match glob semantics:
 *   *        matches any sequence of non-separator chars
 *   ?        matches any single non-separator char
 *   [abc]    matches one char from the class (with ranges and negation via [^...])
 *   /        is the separator (never matched by a wildcard)
 * Other regex specials are escaped.
 */
function globMatch(pattern: string, value: string): boolean {
  if (pattern === value) return true;
  const chars = Array.from(pattern);
  let re = "^";
  let i = 0;
  while (i < chars.length) {
    const c = chars[i];
    switch (c) {
      case "*":
        re += "[^/]*";
        i++;
        break;
      case "?":
        re += "[^/]";
        i++;
        break;
      case "[": {
        // Parse Go's non-empty character-range grammar, not JS class syntax.
        // Hex escapes keep literal brackets and supplementary runes unambiguous.
        i++;
        const negated = chars[i] === "^";
        if (negated) i++;
        const readChar = (): number | undefined => {
          let ch = chars[i++];
          if (ch === "\\") ch = chars[i++];
          else if (ch === "-" || ch === "]") return undefined;
          if (ch === undefined || i >= chars.length) return undefined;
          return ch.codePointAt(0);
        };
        let ranges = "";
        let count = 0;
        while (i < chars.length && (chars[i] !== "]" || count === 0)) {
          const lo = readChar();
          if (lo === undefined) return false;
          let hi = lo;
          if (chars[i] === "-") {
            i++;
            const end = readChar();
            if (end === undefined) return false;
            hi = end;
          }
          // Go allows reversed ranges; they contribute no matching runes.
          if (lo <= hi) ranges += `\\u{${lo.toString(16)}}-\\u{${hi.toString(16)}}`;
          count++;
        }
        if (chars[i] !== "]" || count === 0) return false;
        i++;
        re += `[${negated ? "^" : ""}${ranges}]`;
        break;
      }
      default:
        if ("\\^$.+(){}|]".includes(c)) re += "\\" + c;
        else re += c;
        i++;
    }
  }
  re += "$";
  return new RegExp(re, "u").test(value);
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
      const subpathEndOnly = !cleaned.endsWith("/**") && /[*?\[]/.test(rest);
      return { raw, kind: "subpath", subpath: splitSegments(rest), subpathEndOnly };
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
    return containsSubpath(splitSegments(cleaned), p.subpath, p.subpathEndOnly);
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

/**
 * matchExcluded reports whether a path is dropped by an exclude pattern that
 * no override restores. This is the "should this directory's subtree be
 * pruned" predicate: a directory pruned by matchExcluded means every
 * descendant is guaranteed to be dropped too (exclude is subtree-inheritable
 * unless override restores it). A directory that matchExcluded returns false
 * for may still fail the include whitelist at match() — but its children
 * must be walked because include matches leaf files, not necessarily their
 * parent directories.
 * Recursive file globs exclude entries without pruning their descendants.
 */
export function matchExcluded(m: Matcher, path: string): boolean {
  if (m.override.length > 0 && matchesAny(m.override, path)) return false;
  return m.exclude.some((p) => !p.subpathEndOnly && matchPattern(p, path));
}
