import { describe, expect, it } from "vitest";
import { compile, compileAll, match, matchExcluded, newMatcher, validate, matchPattern } from "../src/pathfilter.js";

describe("pathfilter compile", () => {
  it("compiles subpath form **/x/**", () => {
    const p = compile("**/node_modules/**");
    expect(matchPattern(p, "proj/src/node_modules/react/index.js")).toBe(true);
    expect(matchPattern(p, "node_modules/react/index.js")).toBe(true);
    expect(matchPattern(p, "proj/src/app/main.go")).toBe(false);
  });

  it("compiles prefix form prefix/**", () => {
    const p = compile("dist/**");
    expect(matchPattern(p, "dist/index.js")).toBe(true);
    expect(matchPattern(p, "dist/a/b/c.js")).toBe(true);
    expect(matchPattern(p, "src/index.js")).toBe(false);
  });

  it("compiles exact/glob form", () => {
    const p = compile("*.log");
    expect(matchPattern(p, "app.log")).toBe(true);
    expect(matchPattern(p, "app.txt")).toBe(false);
    const exact = compile("build");
    expect(matchPattern(exact, "build")).toBe(true);
    expect(matchPattern(exact, "build/dist")).toBe(false);
  });
});

describe("pathfilter recursive glob segments", () => {
  it.each([
    ["**/*-wal", "repro.db-wal", true],
    ["**/*-wal", "/issue-validation/run/case/repro.db-wal", true],
    ["**/repro.db-wal", "/issue-validation/run/case/repro.db-wal", true],
    ["**/*-wal", "proj/other.sqlite-wal", true],
    ["**/*-wal", "proj/repro.db", false],
    ["**/*-wal", "proj/repro.db-shm", false],
    ["**/*-wal", "proj/repro.db-wal.bak", false],
    ["**/*-wal", "/snapshots-wal/data.bin", false],
    ["**/*-wal", "/proj/snapshots-wal/sub/data.bin", false],
    ["**/*-wal", "/snapshots-wal/repro.db-wal", true],
    ["**/cache-*/**", "proj/cache-build/objects/a", true],
    ["**/cache-*", "proj/cache-build/objects/a", false],
    ["**/cache-*", "proj/cache-build", true],
    ["**/cache-*/*.log", "proj/cache-build/output.log", true],
    ["**/cache-*/*.log", "proj/cache-build/nested/output.log", false],
    ["**/cache-*/*.log", "proj/cache-build/output.log/data.bin", false],
    ["**/cache-*/*.log", "cache-first/miss/cache-second/output.log", true],
    ["**/db?.wal", "proj/db1.wal", true],
    ["**/db?.wal", "proj/db12.wal", false],
    ["**/*.[Tt]xt", "proj/a.Txt", true],
    ["**/*.[^Tt]xt", "proj/a.bxt", true],
    ["**/*.[^Tt]xt", "proj/a.txt", false],
    ["**/[cache]/**", "proj/[cache]/item", true],
    ["**/[z-a]/**", "proj/[z-a]/item", true],
    ["**/[z-a]/**", "proj/other/item", false],
    ["**/db?.wal", "proj/db\u{1f600}.wal", true],
    ["**/db??.wal", "proj/db\u{1f600}.wal", false],
    ["**/db[\u{1f600}].wal", "proj/db\u{1f600}.wal", true],
    ["**/db[\u{1f600}]?.wal", "proj/db\u{1f600}.wal", false],
    ["**/db[\u{1f600}-\u{1f64f}].wal", "proj/db\u{1f609}.wal", true],
    ["**/db[^\u{1f600}].wal", "proj/db\u{1f609}.wal", true],
    ["**/db[^a].wal", "proj/db\u{1f600}.wal", true],
    ["**/db[^a]?.wal", "proj/db\u{1f600}.wal", false],
    ["**/db?.wal", "proj/db\n.wal", true],
    ["**/db*.wal", "proj/db\n\u{1f600}.wal", true],
    ["**/cafe\u0301*.txt", "proj/caf\u00e91.txt", true],
    ["**/caf\u00e9*.txt", "proj/cafe\u03011.txt", true],
    ["**/caf?.txt", "proj/cafe\u0301.txt", true],
    ["**/caf[e\u0301].txt", "proj/caf\u00e9.txt", true],
    ["**/*?", "proj/\u{1f600}", true],
    ["**/*??", "proj/\u{1f600}", false],
    ["**/*??", "proj/\u{1f600}a", true],
    ["**/*[^a][^a]", "proj/\u{1f600}", false],
    ["**/*[\ufffd]", "proj/\u{1f600}", false],
    ["**/*??x", "proj/\u{1f600}x", false],
    ["**/*a*??", "proj/a\u{1f600}", false],
    ["**/*a*??", "proj/a\u{1f600}a", true],
    ["**/[*]*??", "proj/*\u{1f600}", false],
    ["**/*-wal", "proj/\u{1f600}-wal", true],
    ["*??", "\u{1f600}", false],
    ["*??/file", "\u{1f600}/file", false],
    ["*?/file", "\u{1f600}/file", true],
    ["*?file", "\u{1f600}/file", false],
    ["**/[a-]/**", "proj/a/item", false],
    ["**/[a-]/**", "proj/[a-]/item", true],
    ["**/[a-]/**", "proj/-/item", false],
    ["**/[-a]/**", "proj/a/item", false],
    ["**/[]/**", "proj/a/item", false],
    ["**/[^]/**", "proj/a/item", false],
    ["**/[]a]/**", "proj/a/item", false],
    ["**/[abc/**", "proj/[/item", false],
    ["**/[a-b-c]/**", "proj/-/item", false],
    ["**/[z-aa]/**", "proj/a/item", true],
    ["**/[^z-a]/**", "proj/a/item", true],
    ["**/[[]/**", "proj/[/item", true],
    ["**/db?].wal", "proj/db1].wal", true],
    ["**/[abc", "proj/[abc/secret", true],
    ["**/[a-]", "proj/[a-]/secret", true],
    ["**/[abc/name", "proj/[abc/name/secret", true],
    ["**/[a/b]", "proj/[a/b]/secret", true],
    ["**/prefix*/[abc", "proj/prefix1/[abc", true],
    ["**/prefix*/[abc", "proj/prefix1/[abc/secret", false],
    ["**/[z-a]", "proj/[z-a]/secret", false],
  ])("%s matching %s returns %s", (pattern, path, want) => {
    expect(matchPattern(compile(pattern), path)).toBe(want);
  });
});

describe("pathfilter matcher", () => {
  it("include whitelist + exclude", () => {
    const m = newMatcher({ include: ["proj/**", "go.mod"], exclude: ["**/vendor/**"] });
    expect(match(m, "proj/src/main.go")).toBe(true);
    expect(match(m, "go.mod")).toBe(true);
    expect(match(m, "proj/vendor/foo.go")).toBe(false);
    expect(match(m, "README.md")).toBe(false);
  });

  it("no include accepts all non-excluded", () => {
    const m = newMatcher({ exclude: ["**/.git/**", "**/node_modules/**"] });
    expect(match(m, "proj/src/app.go")).toBe(true);
    expect(match(m, "proj/node_modules/react/x.js")).toBe(false);
    expect(match(m, "proj/.git/HEAD")).toBe(false);
  });

  it("override restores excluded path", () => {
    const m = newMatcher({
      exclude: ["**/node_modules/**"],
      override: ["proj/node_modules/.package-lock.json"],
    });
    expect(match(m, "proj/node_modules/react/x.js")).toBe(false);
    expect(match(m, "proj/node_modules/.package-lock.json")).toBe(true);
    expect(match(m, "proj/src/app.go")).toBe(true);
  });

  it("exclude wins over include without override", () => {
    const m = newMatcher({ include: ["proj/**"], exclude: ["**/vendor/**"] });
    expect(match(m, "proj/vendor/foo.go")).toBe(false);
    expect(match(m, "proj/src/foo.go")).toBe(true);
  });

  it("empty matcher matches all", () => {
    const m = newMatcher({});
    expect(match(m, "anything/here.go")).toBe(true);
  });
});

describe("pathfilter validate", () => {
  it.each(["**/[\\-]/**", "**/[\\]]/**"])("rejects backslashes in %s", (pattern) => {
    expect(() => compile(pattern)).toThrow("backslash");
    expect(validate([pattern])).toBeInstanceOf(Error);
    expect(compileAll([pattern])).toEqual([]);
  });

  it("returns null for valid patterns", () => {
    expect(validate(["dist/**", "*.log"], ["**/x/**"])).toBeNull();
  });
});

describe("pathfilter compileAll", () => {
  it("skips blank entries", () => {
    expect(compileAll(["", "  ", "dist/**"]).length).toBe(1);
  });
});

describe("pathfilter matchExcluded", () => {
  it("does not prune directories selected only by recursive file globs", () => {
    const m = newMatcher({ exclude: ["**/*-wal", "**/cache-*/**", "**/vendor", "**/[abc"] });
    expect(match(m, "snapshots-wal")).toBe(false);
    expect(matchExcluded(m, "snapshots-wal")).toBe(false);
    expect(match(m, "snapshots-wal/data.bin")).toBe(true);
    expect(match(m, "snapshots-wal/repro.db-wal")).toBe(false);
    expect(matchExcluded(m, "cache-build")).toBe(true);
    expect(matchExcluded(m, "proj/vendor")).toBe(true);
    expect(matchExcluded(m, "proj/[abc")).toBe(true);
    expect(match(m, "proj/[abc/secret")).toBe(false);
  });

  it("returns true only for exclude-matched paths not restored by override", () => {
    const m = newMatcher({
      include: ["src/app.go"],
      exclude: ["**/node_modules/**", "**/.git/**"],
      override: ["proj/node_modules/.package-lock.json"],
    });
    expect(matchExcluded(m, "proj/node_modules/react")).toBe(true);
    expect(matchExcluded(m, "proj/.git")).toBe(true);
    // Override restores — not excluded.
    expect(matchExcluded(m, "proj/node_modules/.package-lock.json")).toBe(false);
    // A directory that only fails the include whitelist is NOT excluded —
    // pruning it would drop its child src/app.go (the B2 bug).
    expect(matchExcluded(m, "src")).toBe(false);
    expect(matchExcluded(m, "src/util.go")).toBe(false);
  });
});

describe("pathfilter glob ? and [abc]", () => {
  it("supports ? as single non-separator char", () => {
    const p = compile("a?.go");
    expect(matchPattern(p, "ab.go")).toBe(true);
    expect(matchPattern(p, "abc.go")).toBe(false); // ? is single char
    expect(matchPattern(p, "a/.go")).toBe(false); // ? does not cross separators
  });

  it("supports character classes", () => {
    const p = compile("*.[Tt]xt");
    expect(matchPattern(p, "a.txt")).toBe(true);
    expect(matchPattern(p, "a.Txt")).toBe(true);
    expect(matchPattern(p, "a.go")).toBe(false);
  });

  it("supports negated character classes", () => {
    const p = compile("*.[^Tt]xt");
    expect(matchPattern(p, "a.txt")).toBe(false);
    expect(matchPattern(p, "a.bxt")).toBe(true);
  });
});
