import { describe, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

import { runDemo } from "../src/demo.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SAMPLE_DIR = join(__dirname, "..", "..", "data", "sample");

describe("e-commerce demo pipeline", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "demo-test-"));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("happy path — full run against sample data", async () => {
    const outputDir = join(tempDir, "silver");
    const manifestDir = join(tempDir, "manifests");

    const result = await runDemo({
      dataDir: SAMPLE_DIR,
      outputDir,
      manifestDir,
    });

    const meta = result.metadata;
    const tables = result.tables;

    // Overall counts
    assert.equal(meta.status, "success");
    assert.equal(meta.rows_read, 60);
    assert.equal(meta.rows_written, 57);
    assert.equal(meta.rows_rejected, 3);
    assert.equal(meta.files_processed, 4);

    // Per-table counts
    assert.equal(tables.customers.rows_read, 13);
    assert.equal(tables.customers.rows_out, 12);
    assert.equal(tables.customers.duplicates_removed, 1);

    assert.equal(tables.products.rows_read, 12);
    assert.equal(tables.products.rows_out, 11);
    assert.equal(tables.products.rows_filtered, 1);

    assert.equal(tables.orders.rows_read, 15);
    assert.equal(tables.orders.rows_out, 15);
    assert.equal(tables.orders.orphan_customer_ids, 1);

    assert.equal(tables.order_items.rows_read, 20);
    assert.equal(tables.order_items.rows_out, 19);
    assert.equal(tables.order_items.duplicates_removed, 1);
  });

  test("output files created", async () => {
    const outputDir = join(tempDir, "silver");

    await runDemo({
      dataDir: SAMPLE_DIR,
      outputDir,
      manifestDir: join(tempDir, "mf"),
    });

    for (const name of ["customers.csv", "products.csv", "orders.csv", "order_items.csv"]) {
      const filePath = join(outputDir, name);
      assert.ok(existsSync(filePath), `${name} not created`);
      const lines = readFileSync(filePath, "utf-8").trim().split("\n");
      assert.ok(lines.length >= 2, `${name} has no data rows`);
    }
  });

  test("manifest written with correct structure", async () => {
    const manifestDir = join(tempDir, "manifests");

    const result = await runDemo({
      dataDir: SAMPLE_DIR,
      outputDir: join(tempDir, "silver"),
      manifestDir,
    });

    assert.ok(existsSync(result.manifest_path));
    const manifest = JSON.parse(readFileSync(result.manifest_path, "utf-8"));

    assert.equal(manifest.run.status, "success");
    assert.equal(manifest.run.rows_read, 60);
    assert.ok("customers" in manifest.tables);
    assert.ok("products" in manifest.tables);
    assert.ok("orders" in manifest.tables);
    assert.ok("order_items" in manifest.tables);
  });

  test("warnings captured for known data quality issues", async () => {
    const result = await runDemo({
      dataDir: SAMPLE_DIR,
      outputDir: join(tempDir, "silver"),
      manifestDir: join(tempDir, "mf"),
    });

    const warnings = result.metadata.warnings;
    assert.ok(warnings.some((w) => w.includes("duplicate")));
    assert.ok(warnings.some((w) => w.includes("invalid price")));
    assert.ok(warnings.some((w) => w.includes("non-existent customer_id")));
  });
});
