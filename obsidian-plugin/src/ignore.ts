/**
 * Simple glob-based ignore matcher for file paths.
 * Supports: ** (any path), * (any segment), exact match.
 */
export class IgnoreMatcher {
  private patterns: RegExp[];

  constructor(patterns: string[]) {
    this.patterns = patterns.map(globToRegex);
  }

  isIgnored(path: string): boolean {
    return this.patterns.some((re) => re.test(path));
  }
}

function globToRegex(glob: string): RegExp {
  let re = "^";
  let i = 0;
  while (i < glob.length) {
    const c = glob[i];
    if (c === "*" && glob[i + 1] === "*") {
      re += ".*";
      i += 2;
      if (glob[i] === "/") i++; // skip trailing slash after **
    } else if (c === "*") {
      re += "[^/]*";
      i++;
    } else if (c === "?") {
      re += "[^/]";
      i++;
    } else if (".+^${}()|[]\\".includes(c)) {
      re += "\\" + c;
      i++;
    } else {
      re += c;
      i++;
    }
  }
  re += "$";
  return new RegExp(re);
}
