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

  test("golden output — customers deduplicated, products filtered", async () => {
    const outputDir = join(tempDir, "silver");

    await runDemo({
      dataDir: SAMPLE_DIR,
      outputDir,
      manifestDir: join(tempDir, "mf"),
    });

    const customerLines = readFileSync(join(outputDir, "customers.csv"), "utf-8")
      .trim()
      .split("\n");
    // 12 data rows + 1 header
    assert.equal(customerLines.length, 13);
    // C003 duplicate removed — appears exactly once
    const c003 = customerLines.filter((l) => l.startsWith("C003,"));
    assert.equal(c003.length, 1);

    // P009 (negative price) filtered from products
    const productLines = readFileSync(join(outputDir, "products.csv"), "utf-8")
      .trim()
      .split("\n");
    const p009 = productLines.filter((l) => l.startsWith("P009,"));
    assert.equal(p009.length, 0);
  });

  test("manifest JSON has expected structure", async () => {
    const result = await runDemo({
      dataDir: SAMPLE_DIR,
      outputDir: join(tempDir, "silver"),
      manifestDir: join(tempDir, "mf"),
    });

    const manifest = JSON.parse(readFileSync(result.manifest_path, "utf-8"));

    // Top-level keys
    assert.deepEqual(Object.keys(manifest).sort(), ["run", "tables"]);

    // Run metadata has required keys
    const runKeys = Object.keys(manifest.run).sort();
    assert.ok(runKeys.includes("pipeline_name"));
    assert.ok(runKeys.includes("status"));
    assert.ok(runKeys.includes("rows_read"));
    assert.ok(runKeys.includes("rows_written"));
    assert.ok(runKeys.includes("warnings"));

    // All 4 tables present with required fields
    for (const table of ["customers", "products", "orders", "order_items"]) {
      assert.ok(table in manifest.tables, `${table} missing from manifest`);
      assert.ok("rows_read" in manifest.tables[table]);
      assert.ok("rows_out" in manifest.tables[table]);
    }
  });

  test("rerun produces identical output", async () => {
    const run1Dir = join(tempDir, "run1");
    const run2Dir = join(tempDir, "run2");

    await runDemo({ dataDir: SAMPLE_DIR, outputDir: join(run1Dir, "silver"), manifestDir: join(run1Dir, "mf") });
    await runDemo({ dataDir: SAMPLE_DIR, outputDir: join(run2Dir, "silver"), manifestDir: join(run2Dir, "mf") });

    for (const name of ["customers.csv", "products.csv", "orders.csv", "order_items.csv"]) {
      const c1 = readFileSync(join(run1Dir, "silver", name), "utf-8");
      const c2 = readFileSync(join(run2Dir, "silver", name), "utf-8");
      assert.equal(c1, c2, `${name} differs between runs`);
    }
  });
});
