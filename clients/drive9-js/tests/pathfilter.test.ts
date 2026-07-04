import { describe, expect, it } from "vitest";
import { compile, compileAll, match, newMatcher, validate, matchPattern } from "../src/pathfilter.js";

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
  it("returns null for valid patterns", () => {
    expect(validate(["dist/**", "*.log"], ["**/x/**"])).toBeNull();
  });
});

describe("pathfilter compileAll", () => {
  it("skips blank entries", () => {
    expect(compileAll(["", "  ", "dist/**"]).length).toBe(1);
  });
});