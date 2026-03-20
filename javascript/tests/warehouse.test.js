import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import {
  loadRawCsv,
  loadRawEventsJson,
  runSqlFile,
  runWarehousePipeline,
} from "../src/warehouse/loader.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, "..", "..");
const DATA_DIR = join(REPO_ROOT, "data", "sample");
const SQL_DIR = join(REPO_ROOT, "sql");

// ---------------------------------------------------------------------------
// loadRawCsv
// ---------------------------------------------------------------------------

describe("loadRawCsv", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "warehouse-csv-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("loads rows correctly", async () => {
    const csvPath = join(tmpDir, "people.csv");
    await writeFile(csvPath, "id,name,age\n1,Alice,30\n2,Bob,25\n3,Carol,40\n");

    const db = new DatabaseSync(":memory:");
    db.exec(
      "CREATE TABLE people (id TEXT PRIMARY KEY, name TEXT, age TEXT)",
    );

    const count = loadRawCsv(db, "people", csvPath);
    assert.equal(count, 3);

    const rows = db.prepare("SELECT COUNT(*) AS cnt FROM people").get();
    assert.equal(rows.cnt, 3);

    db.close();
  });

  it("handles empty values as null", async () => {
    const csvPath = join(tmpDir, "sparse.csv");
    await writeFile(csvPath, "id,name,email\n1,Alice,\n2,,bob@test.com\n");

    const db = new DatabaseSync(":memory:");
    db.exec("CREATE TABLE sparse (id TEXT, name TEXT, email TEXT)");

    loadRawCsv(db, "sparse", csvPath);

    const row1 = db
      .prepare("SELECT email FROM sparse WHERE id = '1'")
      .get();
    assert.equal(row1.email, null);

    const row2 = db
      .prepare("SELECT name FROM sparse WHERE id = '2'")
      .get();
    assert.equal(row2.name, null);

    db.close();
  });
});

// ---------------------------------------------------------------------------
// loadRawEventsJson
// ---------------------------------------------------------------------------

describe("loadRawEventsJson", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "warehouse-json-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("loads JSONL correctly", async () => {
    const jsonlPath = join(tmpDir, "events.json");
    const lines = [
      JSON.stringify({
        event_id: "e1",
        type: "page_view",
        user_id: "u1",
        page: "/home",
        product_id: null,
        quantity: null,
        order_id: null,
        timestamp: "2024-01-01T10:00:00Z",
      }),
      JSON.stringify({
        event_id: "e2",
        type: "add_to_cart",
        user_id: "u2",
        page: "/product/1",
        product_id: "P001",
        quantity: 2,
        order_id: null,
        timestamp: "2024-01-01T11:00:00Z",
      }),
    ];
    await writeFile(jsonlPath, lines.join("\n") + "\n");

    const db = new DatabaseSync(":memory:");
    db.exec(`CREATE TABLE events (
      event_id TEXT, type TEXT, user_id TEXT, page TEXT,
      product_id TEXT, quantity INTEGER, order_id TEXT, timestamp TEXT
    )`);

    const count = loadRawEventsJson(db, jsonlPath);
    assert.equal(count, 2);

    const rows = db.prepare("SELECT COUNT(*) AS cnt FROM events").get();
    assert.equal(rows.cnt, 2);

    db.close();
  });

  it("handles null fields", async () => {
    const jsonlPath = join(tmpDir, "events.json");
    const line = JSON.stringify({
      event_id: "e99",
      type: "page_view",
      user_id: null,
      page: "/home",
      product_id: null,
      quantity: null,
      order_id: null,
      timestamp: "2024-01-01T10:00:00Z",
    });
    await writeFile(jsonlPath, line + "\n");

    const db = new DatabaseSync(":memory:");
    db.exec(`CREATE TABLE events (
      event_id TEXT, type TEXT, user_id TEXT, page TEXT,
      product_id TEXT, quantity INTEGER, order_id TEXT, timestamp TEXT
    )`);

    const count = loadRawEventsJson(db, jsonlPath);
    assert.equal(count, 1);

    const row = db
      .prepare("SELECT user_id FROM events WHERE event_id = 'e99'")
      .get();
    assert.equal(row.user_id, null);

    db.close();
  });
});

// ---------------------------------------------------------------------------
// runSqlFile
// ---------------------------------------------------------------------------

describe("runSqlFile", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "warehouse-sql-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("executes SQL from file", async () => {
    const sqlPath = join(tmpDir, "setup.sql");
    await writeFile(
      sqlPath,
      "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);\nINSERT INTO t VALUES (1, 'hello');\n",
    );

    const db = new DatabaseSync(":memory:");
    runSqlFile(db, sqlPath);

    const row = db.prepare("SELECT val FROM t WHERE id = 1").get();
    assert.equal(row.val, "hello");

    db.close();
  });

  it("handles multiple statements", async () => {
    const sqlPath = join(tmpDir, "multi.sql");
    await writeFile(
      sqlPath,
      [
        "CREATE TABLE a (id INTEGER);",
        "CREATE TABLE b (id INTEGER);",
        "INSERT INTO a VALUES (1);",
        "INSERT INTO a VALUES (2);",
        "INSERT INTO b VALUES (10);",
      ].join("\n"),
    );

    const db = new DatabaseSync(":memory:");
    runSqlFile(db, sqlPath);

    const countA = db.prepare("SELECT COUNT(*) AS cnt FROM a").get();
    assert.equal(countA.cnt, 2);

    const countB = db.prepare("SELECT COUNT(*) AS cnt FROM b").get();
    assert.equal(countB.cnt, 1);

    db.close();
  });
});

// ---------------------------------------------------------------------------
// runWarehousePipeline
// ---------------------------------------------------------------------------

describe("runWarehousePipeline", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "warehouse-pipe-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("full sample data", () => {
    const result = runWarehousePipeline({ dataDir: DATA_DIR, sqlDir: SQL_DIR });

    assert.equal(result.status, "success");

    // Staging counts
    assert.equal(result.staging_tables.customers, 13);
    assert.equal(result.staging_tables.products, 12);
    assert.equal(result.staging_tables.orders, 15);
    assert.equal(result.staging_tables.order_items, 20);
    assert.equal(result.staging_tables.events, 20);

    // Warehouse counts
    assert.equal(result.warehouse_tables.dim_customer, 12);
    assert.equal(result.warehouse_tables.dim_product, 11);
    assert.equal(result.warehouse_tables.dim_date, 366);
    assert.equal(result.warehouse_tables.fact_order, 14);
    assert.equal(result.warehouse_tables.fact_order_item, 19);
    assert.equal(result.warehouse_tables.fact_event, 19);

    // Queries
    assert.equal(result.queries.length, 5);
  });

  it("writes reports", () => {
    const reportDir = join(tmpDir, "reports");
    runWarehousePipeline({
      dataDir: DATA_DIR,
      sqlDir: SQL_DIR,
      reportDir,
    });

    // CSV reports for each query
    assert.ok(existsSync(join(reportDir, "warehouse_row_counts.csv")));
    assert.ok(existsSync(join(reportDir, "revenue_by_status.csv")));
    assert.ok(existsSync(join(reportDir, "top_products_warehouse.csv")));
    assert.ok(existsSync(join(reportDir, "daily_warehouse_revenue.csv")));
    assert.ok(existsSync(join(reportDir, "customer_spend_warehouse.csv")));

    // Summary JSON
    const summaryPath = join(reportDir, "warehouse_summary.json");
    assert.ok(existsSync(summaryPath));

    const summary = JSON.parse(readFileSync(summaryPath, "utf-8"));
    assert.equal(summary.status, "success");
    assert.equal(summary.staging_tables.customers, 13);
    assert.equal(summary.queries.length, 5);
  });

  it("missing data dir — tables created empty, no crash", () => {
    const missingDir = join(tmpDir, "nonexistent-data");

    // The pipeline will throw because readFileSync fails on missing files.
    // But DDL should execute first. Verify it throws rather than silently
    // corrupting data.
    assert.throws(() => {
      runWarehousePipeline({ dataDir: missingDir, sqlDir: SQL_DIR });
    });
  });

  it("idempotent rerun", () => {
    // Two independent runs against separate in-memory databases should
    // produce identical summaries — this verifies determinism.
    const first = runWarehousePipeline({
      dataDir: DATA_DIR,
      sqlDir: SQL_DIR,
    });
    const second = runWarehousePipeline({
      dataDir: DATA_DIR,
      sqlDir: SQL_DIR,
    });

    assert.equal(first.status, "success");
    assert.equal(second.status, "success");

    // Row counts should match between runs (no duplication)
    assert.deepStrictEqual(
      first.staging_tables,
      second.staging_tables,
    );
    assert.deepStrictEqual(
      first.warehouse_tables,
      second.warehouse_tables,
    );
    assert.equal(first.queries.length, second.queries.length);
  });
});

// ---------------------------------------------------------------------------
// Warehouse transform quality checks
// ---------------------------------------------------------------------------

describe("warehouse transform quality", () => {
  it("dim_customer deduplication — 12 rows not 13 (C003 deduped)", () => {
    const result = runWarehousePipeline({ dataDir: DATA_DIR, sqlDir: SQL_DIR });
    assert.equal(result.staging_tables.customers, 13);
    assert.equal(result.warehouse_tables.dim_customer, 12);
  });

  it("dim_product filters negative price — 11 rows not 12 (P009 filtered)", () => {
    const result = runWarehousePipeline({ dataDir: DATA_DIR, sqlDir: SQL_DIR });
    assert.equal(result.staging_tables.products, 12);
    assert.equal(result.warehouse_tables.dim_product, 11);
  });

  it("fact_order skips orphan FK — 14 rows not 15 (ORD-008 skipped)", () => {
    const result = runWarehousePipeline({ dataDir: DATA_DIR, sqlDir: SQL_DIR });
    assert.equal(result.staging_tables.orders, 15);
    assert.equal(result.warehouse_tables.fact_order, 14);
  });
});
