import { describe, expect, it } from "vitest";
import { canonicalize, matchesPrefix } from "../src/index.js";

describe("dockbay core", () => {
  it("canonicalizes deterministically", () => {
    expect(canonicalize({ b: 2, a: 1, c: undefined })).toBe('{"a":1,"b":2}');
  });

  it("matches key prefixes structurally", () => {
    expect(matchesPrefix({ runId: "run_1", seq: 2 }, { runId: "run_1" })).toBe(true);
    expect(matchesPrefix({ runId: "run_2", seq: 2 }, { runId: "run_1" })).toBe(false);
  });
});
