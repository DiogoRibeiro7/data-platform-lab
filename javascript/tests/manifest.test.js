import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  generateRunId,
  writeManifest,
  readManifest,
  validateManifest,
  MANIFEST_REQUIRED_KEYS,
} from "../src/manifest.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTempDir() {
  return mkdtempSync(join(tmpdir(), "manifest-test-"));
}

// ---------------------------------------------------------------------------
// generateRunId
// ---------------------------------------------------------------------------

describe("generateRunId", () => {
  it("returns a 15-character string with underscore at position 8", () => {
    const id = generateRunId();

    assert.equal(id.length, 15);
    assert.equal(id[8], "_");
    assert.match(id, /^\d{8}_\d{6}$/);
  });
});

// ---------------------------------------------------------------------------
// writeManifest
// ---------------------------------------------------------------------------

describe("writeManifest", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("creates a JSON file on disk", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "test_pipe",
      run_id: "20260101_120000",
      source: "input.csv",
      output: "output.csv",
      row_count: 42,
      manifestDir,
    });

    assert.ok(existsSync(filePath), `File not found: ${filePath}`);
    assert.ok(filePath.endsWith(".json"));
  });

  it("includes all MANIFEST_REQUIRED_KEYS", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "test_pipe",
      run_id: "20260101_120000",
      source: "input.csv",
      output: "output.csv",
      row_count: 42,
      manifestDir,
    });

    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    for (const key of MANIFEST_REQUIRED_KEYS) {
      assert.ok(key in data, `Missing required key: ${key}`);
    }
  });

  it("includes schema_hint when provided", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "test_pipe",
      run_id: "20260101_120000",
      source: "input.csv",
      output: "output.csv",
      row_count: 10,
      schema_hint: ["id", "name", "email"],
      manifestDir,
    });

    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    assert.deepStrictEqual(data.schema_hint, ["id", "name", "email"]);
  });

  it("includes warnings when provided", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "test_pipe",
      run_id: "20260101_120000",
      source: "input.csv",
      output: "output.csv",
      row_count: 10,
      warnings: ["skipped 2 rows", "column mismatch"],
      manifestDir,
    });

    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    assert.deepStrictEqual(data.warnings, [
      "skipped 2 rows",
      "column mismatch",
    ]);
  });

  it("merges extras into the manifest", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "test_pipe",
      run_id: "20260101_120000",
      source: "input.csv",
      output: "output.csv",
      row_count: 10,
      extras: {
        duplicates_removed: 3,
        files_processed: ["a.csv", "b.csv"],
      },
      manifestDir,
    });

    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    assert.equal(data.duplicates_removed, 3);
    assert.deepStrictEqual(data.files_processed, ["a.csv", "b.csv"]);
  });

  it("omits schema_hint and warnings when not provided", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "test_pipe",
      run_id: "20260101_120000",
      source: "input.csv",
      output: "output.csv",
      row_count: 10,
      manifestDir,
    });

    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    assert.ok(!("schema_hint" in data), "schema_hint should be absent");
    assert.ok(!("warnings" in data), "warnings should be absent");
  });

  it("serialises source as an array when given an array", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "multi_src",
      run_id: "20260101_120000",
      source: ["a.csv", "b.csv", "c.csv"],
      output: "merged.csv",
      row_count: 30,
      manifestDir,
    });

    const data = JSON.parse(readFileSync(filePath, "utf-8"));
    assert.deepStrictEqual(data.source, ["a.csv", "b.csv", "c.csv"]);
  });
});

// ---------------------------------------------------------------------------
// readManifest
// ---------------------------------------------------------------------------

describe("readManifest", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("round-trips through write then read", () => {
    const manifestDir = join(tempDir, "manifests");
    const filePath = writeManifest({
      pipeline_name: "roundtrip",
      run_id: "20260101_120000",
      source: "src.csv",
      output: "dst.csv",
      row_count: 99,
      schema_hint: ["col_a", "col_b"],
      manifestDir,
    });

    const data = readManifest(filePath);

    assert.equal(data.pipeline_name, "roundtrip");
    assert.equal(data.run_id, "20260101_120000");
    assert.equal(data.source, "src.csv");
    assert.equal(data.output, "dst.csv");
    assert.equal(data.row_count, 99);
    assert.deepStrictEqual(data.schema_hint, ["col_a", "col_b"]);
  });
});

// ---------------------------------------------------------------------------
// validateManifest
// ---------------------------------------------------------------------------

describe("validateManifest", () => {
  it("returns empty array for a complete manifest", () => {
    const data = {
      pipeline_name: "valid",
      run_id: "20260101_120000",
      created_at: "2026-01-01T12:00:00.000Z",
      source: "in.csv",
      output: "out.csv",
      row_count: 1,
      status: "success",
    };

    const missing = validateManifest(data);
    assert.deepStrictEqual(missing, []);
  });

  it("returns missing key names for an incomplete manifest", () => {
    const data = {
      pipeline_name: "incomplete",
      run_id: "20260101_120000",
      created_at: "2026-01-01T12:00:00.000Z",
      row_count: 0,
      status: "success",
    };

    const missing = validateManifest(data);
    assert.deepStrictEqual(new Set(missing), new Set(["source", "output"]));
  });
});
