import { describe, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync,
  rmSync,
  readFileSync,
  existsSync,
} from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

import { runDemo } from "../src/demo.js";
import { createDatabase, runAnalytics, QUERIES } from "../src/analytics.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SAMPLE_DIR = join(__dirname, "..", "..", "data", "sample");

async function produceSilver(tempDir) {
  const silverDir = join(tempDir, "silver");
  await runDemo({
    dataDir: SAMPLE_DIR,
    outputDir: silverDir,
    manifestDir: join(tempDir, "manifests"),
  });
  return silverDir;
}

describe("SQLite analytics", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "analytics-test-"));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("loads all tables with correct row counts", async () => {
    const silverDir = await produceSilver(tempDir);
    const db = createDatabase(silverDir);

    const counts = {};
    for (const table of ["customers", "products", "orders", "order_items"]) {
      const row = db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get();
      counts[table] = row.n;
    }

    assert.equal(counts.customers, 12);
    assert.equal(counts.products, 11);
    assert.equal(counts.orders, 15);
    assert.equal(counts.order_items, 19);

    db.close();
  });

  test("end-to-end analytics produces all reports", async () => {
    const silverDir = await produceSilver(tempDir);
    const reportDir = join(tempDir, "gold");

    const result = runAnalytics({ silverDir, reportDir });

    // All 5 queries ran
    assert.equal(result.queries.length, 5);

    // Report CSVs written
    for (const q of result.queries) {
      const csvPath = join(reportDir, `${q.name}.csv`);
      assert.ok(existsSync(csvPath), `${q.name}.csv missing`);
    }

    // Summary JSON written
    assert.ok(existsSync(result.summary_path));
    const summary = JSON.parse(readFileSync(result.summary_path, "utf-8"));
    assert.equal(summary.queries.length, 5);
  });

  test("query row counts match expected values", async () => {
    const silverDir = await produceSilver(tempDir);

    const result = runAnalytics({
      silverDir,
      reportDir: join(tempDir, "gold"),
    });

    const counts = {};
    for (const q of result.queries) {
      counts[q.name] = q.row_count;
    }

    assert.equal(counts.daily_revenue, 11);
    assert.equal(counts.top_products, 10);
    assert.equal(counts.customer_orders, 12);
    assert.equal(counts.orphan_foreign_keys, 1);
    assert.equal(counts.duplicate_detection, 0);
  });

  test("orphan FK query finds C099", async () => {
    const silverDir = await produceSilver(tempDir);

    const result = runAnalytics({
      silverDir,
      reportDir: join(tempDir, "gold"),
    });

    const orphanQuery = result.queries.find(
      (q) => q.name === "orphan_foreign_keys",
    );
    assert.equal(orphanQuery.rows.length, 1);
    assert.equal(orphanQuery.rows[0].missing_customer_id, "C099");
  });

  test("no duplicates after demo pipeline cleaning", async () => {
    const silverDir = await produceSilver(tempDir);

    const result = runAnalytics({
      silverDir,
      reportDir: join(tempDir, "gold"),
    });

    const dupQuery = result.queries.find(
      (q) => q.name === "duplicate_detection",
    );
    assert.equal(dupQuery.rows.length, 0);
  });
});
