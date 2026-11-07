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
import { createDatabase, runAnalytics } from "../src/analytics.js";
import { readCsvFile } from "../src/ingestion/csv-pipeline.js";

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
    assert.deepEqual(summary.tables_loaded, ["customers", "products", "orders", "order_items"]);
  });

  test("daily revenue includes only completed orders with positive revenue", async () => {
    const silverDir = await produceSilver(tempDir);
    const result = runAnalytics({ silverDir, reportDir: join(tempDir, "gold") });
    const { rows } = result.queries.find((q) => q.name === "daily_revenue");

    assert.equal(rows.length, 11);
    assert.ok(rows.every((row) => typeof row.order_date === "string" && row.daily_revenue > 0));
    assert.deepEqual(rows[0], {
      order_date: "2024-06-01", order_count: 1, daily_revenue: 59.98, avg_order_value: 59.98,
    });
    for (const date of ["2024-06-15", "2024-06-20", "2024-07-05", "2024-08-01"]) {
      assert.ok(!rows.some((row) => row.order_date === date), `${date} is not completed`);
    }
  });

  test("top products are ranked by revenue descending", async () => {
    const silverDir = await produceSilver(tempDir);
    const result = runAnalytics({ silverDir, reportDir: join(tempDir, "gold") });
    const { rows } = result.queries.find((q) => q.name === "top_products");

    assert.equal(rows[0].product_name, "Mechanical Keyboard");
    const revenues = rows.map((row) => row.total_revenue);
    assert.deepEqual(revenues, [...revenues].sort((a, b) => b - a));
  });

  test("customer orders retains customers without orders and reports zero spend", async () => {
    const silverDir = await produceSilver(tempDir);
    const result = runAnalytics({ silverDir, reportDir: join(tempDir, "gold") });
    const { rows } = result.queries.find((q) => q.name === "customer_orders");

    assert.equal(rows.length, 12);
    const customer = rows.find((row) => row.customer_id === "C012");
    assert.ok(customer, "customer without orders must be retained by the LEFT JOIN");
    assert.equal(customer.order_count, 0);
    assert.equal(customer.total_spend, 0);
  });

  test("revenue totals match the sample orders and product line items", async () => {
    const silverDir = await produceSilver(tempDir);
    const result = runAnalytics({ silverDir, reportDir: join(tempDir, "gold") });
    const daily = result.queries.find((q) => q.name === "daily_revenue");
    // The 11 completed orders total 628.35; compare integer cents.
    assert.equal(daily.rows.reduce((sum, row) => sum + Math.round(row.daily_revenue * 100), 0), 62835);

    const top = result.queries.find((q) => q.name === "top_products");
    // Four keyboard line items at 89.50 each.
    const keyboard = top.rows.find((row) => row.product_id === "P002");
    assert.equal(keyboard.units_sold, 4);
    assert.equal(keyboard.total_revenue, 358);
  });

  test("report CSV headers and values match query results, including empty reports", async () => {
    const silverDir = await produceSilver(tempDir);
    const reportDir = join(tempDir, "gold");
    const result = runAnalytics({ silverDir, reportDir });

    for (const query of result.queries) {
      const csvPath = join(reportDir, `${query.name}.csv`);
      if (query.row_count === 0) {
        assert.equal(readFileSync(csvPath, "utf-8"), "", query.name);
        continue;
      }
      const { headers, rows } = await readCsvFile(csvPath);
      assert.deepEqual(headers, Object.keys(query.rows[0]), query.name);
      assert.equal(rows.length, query.row_count, query.name);
      assert.deepEqual(rows, query.rows.map((row) =>
        headers.map((header) => String(row[header] ?? ""))), query.name);
    }
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
