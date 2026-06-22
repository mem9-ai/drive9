import { describe, expect, it } from "vitest";

import { Client, StreamWriter } from "../src/index.js";
import { coveredClientMethods, coveredStreamWriterMethods } from "../examples/cookbook.js";

describe("cookbook coverage", () => {
  it("tracks every public Client prototype method", () => {
    const methods = Object.getOwnPropertyNames(Client.prototype).filter((name) => name !== "constructor").sort();
    const covered = [...coveredClientMethods].sort();
    expect(covered).toEqual(methods);
  });

  it("tracks every public StreamWriter prototype method", () => {
    const methods = Object.getOwnPropertyNames(StreamWriter.prototype)
      .filter((name) => name !== "constructor" && name !== "initiate" && name !== "waitInflight")
      .sort();
    const covered = [...coveredStreamWriterMethods].sort();
    expect(covered).toEqual(methods);
  });
});
