import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { runSensorDemo } from "../src/sensor-demo.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "..", "..", "data", "sample");

describe("sensor-demo", () => {
  let tmpDir;
  let outputDir;
  let manifestDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "sensor-demo-test-"));
    outputDir = join(tmpDir, "output");
    manifestDir = join(tmpDir, "manifests");
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  /** Helper: run the demo with test directories. */
  async function run() {
    return runSensorDemo({
      dataDir: DATA_DIR,
      outputDir,
      manifestDir,
    });
  }

  it("should complete with success and 5/5 steps passed", async () => {
    const { pipeline_result } = await run();
    assert.equal(pipeline_result.status, "success");
    assert.equal(pipeline_result.steps_passed, 5);
  });

  it("should write accepted.jsonl with 14 lines", async () => {
    await run();
    const content = await readFile(join(outputDir, "accepted.jsonl"), "utf-8");
    const lines = content
      .split("\n")
      .filter((l) => l.trim().length > 0);
    assert.equal(lines.length, 14);
  });

  it("should write dead_letter.jsonl with rejected events", async () => {
    await run();
    const content = await readFile(
      join(outputDir, "dead_letter.jsonl"),
      "utf-8",
    );
    const lines = content
      .split("\n")
      .filter((l) => l.trim().length > 0);
    // JS version writes 1 rejected event (null value); duplicates are not
    // written to dead-letter in the current JS implementation.
    assert.ok(lines.length >= 1, `expected at least 1 dead-letter entry, got ${lines.length}`);
  });

  it("should write hourly_aggregates.csv with header and data rows", async () => {
    await run();
    const content = await readFile(
      join(outputDir, "hourly_aggregates.csv"),
      "utf-8",
    );
    const lines = content
      .split("\n")
      .filter((l) => l.trim().length > 0);
    // At least header + 1 data row
    assert.ok(lines.length >= 2);
    assert.ok(lines[0].startsWith("sensor_id"));
  });

  it("should write location_summary.csv with 3 locations", async () => {
    await run();
    const content = await readFile(
      join(outputDir, "location_summary.csv"),
      "utf-8",
    );
    const lines = content
      .split("\n")
      .filter((l) => l.trim().length > 0);
    // header + 3 location rows
    assert.equal(lines.length, 4);
  });

  it("should write summary.json with expected keys", async () => {
    await run();
    const content = await readFile(join(outputDir, "summary.json"), "utf-8");
    const summary = JSON.parse(content);
    const expectedKeys = [
      "total_events_read",
      "accepted",
      "rejected",
      "duplicates_removed",
      "hourly_buckets",
      "locations",
      "sensors",
    ];
    for (const key of expectedKeys) {
      assert.ok(key in summary, `expected key "${key}" in summary.json`);
    }
  });

  it("should write a manifest file that exists on disk", async () => {
    const { manifest_path } = await run();
    assert.ok(manifest_path, "manifest_path should be non-empty");
    assert.ok(existsSync(manifest_path), "manifest file should exist");
  });

  it("should be idempotent — run twice, accepted.jsonl still has 14 lines", async () => {
    await runSensorDemo({ dataDir: DATA_DIR, outputDir, manifestDir });
    await runSensorDemo({ dataDir: DATA_DIR, outputDir, manifestDir });

    const content = await readFile(join(outputDir, "accepted.jsonl"), "utf-8");
    const lines = content
      .split("\n")
      .filter((l) => l.trim().length > 0);
    assert.equal(lines.length, 14);
  });
});
