/**
 * Minimal line-based 3-way merge (diff3 algorithm).
 * No external dependencies — suitable for Obsidian plugin bundling.
 */

export interface MergeResult {
  merged: string;
  hasConflicts: boolean;
}

/**
 * Attempt a 3-way merge of text content.
 * @param base  - Common ancestor (shadow store version)
 * @param ours  - Local version
 * @param theirs - Remote version
 * @returns Merged text and whether unresolved conflicts remain.
 */
export function merge3(base: string, ours: string, theirs: string): MergeResult {
  if (ours === theirs) {
    return { merged: ours, hasConflicts: false };
  }
  if (ours === base) {
    return { merged: theirs, hasConflicts: false };
  }
  if (theirs === base) {
    return { merged: ours, hasConflicts: false };
  }

  const baseLines = splitLines(base);
  const oursLines = splitLines(ours);
  const theirsLines = splitLines(theirs);

  const oursEdits = diffLines(baseLines, oursLines);
  const theirsEdits = diffLines(baseLines, theirsLines);

  const result: string[] = [];
  let hasConflicts = false;

  let oIdx = 0;
  let tIdx = 0;

  while (oIdx < oursEdits.length || tIdx < theirsEdits.length) {
    const oEdit = oIdx < oursEdits.length ? oursEdits[oIdx] : null;
    const tEdit = tIdx < theirsEdits.length ? theirsEdits[tIdx] : null;

    if (oEdit && tEdit) {
      if (oEdit.type === "keep" && tEdit.type === "keep") {
        result.push(oEdit.line);
        oIdx++;
        tIdx++;
      } else if (oEdit.type === "keep" && tEdit.type !== "keep") {
        // theirs changed, ours kept — take theirs
        if (tEdit.type === "add") {
          result.push(tEdit.line);
          tIdx++;
        } else {
          // tEdit.type === "remove" — skip this base line
          oIdx++;
          tIdx++;
        }
      } else if (oEdit.type !== "keep" && tEdit.type === "keep") {
        // ours changed, theirs kept — take ours
        if (oEdit.type === "add") {
          result.push(oEdit.line);
          oIdx++;
        } else {
          // oEdit.type === "remove" — skip this base line
          oIdx++;
          tIdx++;
        }
      } else {
        // Both changed the same region — conflict
        hasConflicts = true;
        result.push("<<<<<<< LOCAL");
        while (oIdx < oursEdits.length && oursEdits[oIdx].type !== "keep") {
          if (oursEdits[oIdx].type === "add") {
            result.push(oursEdits[oIdx].line);
          }
          oIdx++;
        }
        result.push("=======");
        while (tIdx < theirsEdits.length && theirsEdits[tIdx].type !== "keep") {
          if (theirsEdits[tIdx].type === "add") {
            result.push(theirsEdits[tIdx].line);
          }
          tIdx++;
        }
        result.push(">>>>>>> REMOTE");
      }
    } else if (oEdit) {
      if (oEdit.type === "add" || oEdit.type === "keep") {
        result.push(oEdit.line);
      }
      oIdx++;
    } else if (tEdit) {
      if (tEdit.type === "add" || tEdit.type === "keep") {
        result.push(tEdit.line);
      }
      tIdx++;
    }
  }

  return { merged: joinLines(result), hasConflicts };
}

interface Edit {
  type: "keep" | "add" | "remove";
  line: string;
}

/**
 * Produce a sequence of edits that transforms `from` into `to`.
 * Uses a simple LCS-based diff.
 */
function diffLines(from: string[], to: string[]): Edit[] {
  const lcs = longestCommonSubsequence(from, to);
  const edits: Edit[] = [];

  let fi = 0;
  let ti = 0;
  let li = 0;

  while (li < lcs.length) {
    while (fi < lcs[li].fi) {
      edits.push({ type: "remove", line: from[fi] });
      fi++;
    }
    while (ti < lcs[li].ti) {
      edits.push({ type: "add", line: to[ti] });
      ti++;
    }
    edits.push({ type: "keep", line: from[fi] });
    fi++;
    ti++;
    li++;
  }

  while (fi < from.length) {
    edits.push({ type: "remove", line: from[fi] });
    fi++;
  }
  while (ti < to.length) {
    edits.push({ type: "add", line: to[ti] });
    ti++;
  }

  return edits;
}

interface LCSEntry {
  fi: number;
  ti: number;
}

function longestCommonSubsequence(a: string[], b: string[]): LCSEntry[] {
  const m = a.length;
  const n = b.length;

  // For large files, use a space-optimized approach
  const dp: number[][] = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));

  for (let i = m - 1; i >= 0; i--) {
    for (let j = n - 1; j >= 0; j--) {
      if (a[i] === b[j]) {
        dp[i][j] = dp[i + 1][j + 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
  }

  const result: LCSEntry[] = [];
  let i = 0;
  let j = 0;
  while (i < m && j < n) {
    if (a[i] === b[j]) {
      result.push({ fi: i, ti: j });
      i++;
      j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      i++;
    } else {
      j++;
    }
  }

  return result;
}

function splitLines(text: string): string[] {
  if (text === "") return [];
  return text.split("\n");
}

function joinLines(lines: string[]): string {
  return lines.join("\n");
}
