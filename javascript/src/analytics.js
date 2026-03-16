/**
 * SQLite analytics layer for the e-commerce demo.
 *
 * Loads curated CSV outputs into an in-memory SQLite database (using the
 * experimental node:sqlite module in Node 22+) and runs analytical queries.
 *
 * Run from the javascript/ directory:
 *   node src/analytics.js
 *
 * Or with custom paths:
 *   node src/analytics.js --silver-dir ../data/silver/demo --report-dir ../data/gold/reports
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { parseArgs } from "node:util";
import { DatabaseSync } from "node:sqlite";

// ---------------------------------------------------------------------------
// Analytical queries
// ---------------------------------------------------------------------------

/** @type {Array<{name: string, description: string, sql: string}>} */
export const QUERIES = [
  {
    name: "daily_revenue",
    description: "Revenue by date for completed orders",
    sql: `
SELECT
    order_date,
    COUNT(*)              AS order_count,
    ROUND(SUM(total), 2)  AS daily_revenue,
    ROUND(AVG(total), 2)  AS avg_order_value
FROM orders
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date;`,
  },
  {
    name: "top_products",
    description: "Products ranked by total revenue",
    sql: `
SELECT
    p.product_id,
    p.name             AS product_name,
    p.category,
    SUM(oi.quantity)   AS units_sold,
    ROUND(SUM(oi.line_total), 2) AS total_revenue,
    COUNT(DISTINCT oi.order_id)  AS order_count
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue DESC;`,
  },
  {
    name: "customer_orders",
    description: "Order count and total spend per customer",
    sql: `
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.country,
    COUNT(o.order_id)                    AS order_count,
    COALESCE(ROUND(SUM(o.total), 2), 0) AS total_spend
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, full_name, c.country
ORDER BY total_spend DESC;`,
  },
  {
    name: "orphan_foreign_keys",
    description: "Orders referencing non-existent customers",
    sql: `
SELECT
    o.order_id,
    o.customer_id AS missing_customer_id,
    o.order_date,
    o.total
FROM orders o
LEFT JOIN customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;`,
  },
  {
    name: "duplicate_detection",
    description: "Duplicate rows across tables",
    sql: `
SELECT 'customers' AS table_name,
       customer_id AS duplicate_key,
       COUNT(*)    AS occurrences
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'order_items' AS table_name,
       order_id || '|' || product_id || '|' || quantity AS duplicate_key,
       COUNT(*) AS occurrences
FROM order_items
GROUP BY order_id, product_id, quantity
HAVING COUNT(*) > 1;`,
  },
];

// ---------------------------------------------------------------------------
// CSV parsing (minimal — no external deps)
// ---------------------------------------------------------------------------

function parseCsvContent(content) {
  const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length === 0) return { headers: [], rows: [] };
  const headers = lines[0].split(",");
  const rows = lines.slice(1).map((line) => line.split(","));
  return { headers, rows };
}

// ---------------------------------------------------------------------------
// SQLite loader
// ---------------------------------------------------------------------------

const TABLE_SCHEMAS = {
  customers: `CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    email       TEXT,
    city        TEXT,
    country     TEXT,
    created_at  TEXT
  )`,
  products: `CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    name       TEXT,
    category   TEXT,
    price      REAL,
    currency   TEXT,
    stock      INTEGER,
    active     TEXT
  )`,
  orders: `CREATE TABLE orders (
    order_id         TEXT PRIMARY KEY,
    customer_id      TEXT,
    order_date       TEXT,
    status           TEXT,
    total            REAL,
    shipping_country TEXT
  )`,
  order_items: `CREATE TABLE order_items (
    order_id   TEXT,
    product_id TEXT,
    quantity   INTEGER,
    unit_price REAL,
    line_total REAL
  )`,
};

/**
 * Create a SQLite database and load curated CSVs into it.
 *
 * @param {string} silverDir - Directory containing the curated CSVs
 * @param {string} [dbPath=":memory:"] - SQLite database path
 * @returns {DatabaseSync}
 */
export function createDatabase(silverDir, dbPath = ":memory:") {
  const db = new DatabaseSync(dbPath);

  for (const [tableName, ddl] of Object.entries(TABLE_SCHEMAS)) {
    db.exec(`DROP TABLE IF EXISTS ${tableName}`);
    db.exec(ddl);

    const csvPath = join(silverDir, `${tableName}.csv`);
    if (!existsSync(csvPath)) continue;

    const content = readFileSync(csvPath, "utf-8");
    const { headers, rows } = parseCsvContent(content);
    if (headers.length === 0) continue;

    const placeholders = headers.map(() => "?").join(", ");
    const insertSql = `INSERT INTO ${tableName} (${headers.join(", ")}) VALUES (${placeholders})`;
    const stmt = db.prepare(insertSql);

    for (const row of rows) {
      const values = row.map((v) => (v === "" ? null : v));
      stmt.run(...values);
    }
  }

  return db;
}

// ---------------------------------------------------------------------------
// Query runner
// ---------------------------------------------------------------------------

function writeReportCsv(rows, filePath) {
  mkdirSync(dirname(filePath), { recursive: true });
  if (rows.length === 0) {
    writeFileSync(filePath, "", "utf-8");
    return;
  }
  const headers = Object.keys(rows[0]);
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((h) => row[h] ?? "").join(",")),
  ];
  writeFileSync(filePath, lines.join("\n") + "\n", "utf-8");
}

/**
 * Load curated data into SQLite and run all analytical queries.
 *
 * @param {object} [options]
 * @param {string} [options.silverDir="data/silver/demo"]
 * @param {string} [options.reportDir="data/gold/reports"]
 * @param {string} [options.dbPath=":memory:"]
 * @returns {{ summary_path: string, report_dir: string, queries: Array<{name: string, description: string, row_count: number, rows: object[]}> }}
 */
export function runAnalytics({
  silverDir = "data/silver/demo",
  reportDir = "data/gold/reports",
  dbPath = ":memory:",
} = {}) {
  mkdirSync(reportDir, { recursive: true });

  const db = createDatabase(silverDir, dbPath);
  const queryResults = [];

  for (const { name, description, sql } of QUERIES) {
    const stmt = db.prepare(sql);
    const rows = stmt.all().map((row) => ({ ...row }));
    writeReportCsv(rows, join(reportDir, `${name}.csv`));
    queryResults.push({ name, description, row_count: rows.length, rows });
  }

  // Write summary JSON
  const summary = {
    db_path: dbPath,
    tables_loaded: Object.keys(TABLE_SCHEMAS),
    queries: queryResults.map(({ name, description, row_count }) => ({
      name,
      description,
      row_count,
    })),
  };
  const summaryPath = join(reportDir, "analytics_summary.json");
  writeFileSync(summaryPath, JSON.stringify(summary, null, 2), "utf-8");

  db.close();

  return {
    summary_path: summaryPath,
    report_dir: reportDir,
    queries: queryResults,
  };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

function main() {
  const { values } = parseArgs({
    options: {
      "silver-dir": { type: "string", default: "../data/silver/demo" },
      "report-dir": { type: "string", default: "../data/gold/reports" },
      "db-path": { type: "string", default: ":memory:" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });

  if (values.help) {
    console.log(
      `Usage: node src/analytics.js [options]

Options:
  --silver-dir <path>   Directory with curated CSVs (default: ../data/silver/demo)
  --report-dir <path>   Directory for report CSVs (default: ../data/gold/reports)
  --db-path <path>      SQLite DB path (default: :memory:)
  -h, --help            Show this help message`,
    );
    process.exit(0);
  }

  const result = runAnalytics({
    silverDir: values["silver-dir"],
    reportDir: values["report-dir"],
    dbPath: values["db-path"],
  });

  console.log();
  console.log("=== Analytics Report ===");
  console.log();
  for (const q of result.queries) {
    console.log(`  ${q.name}: ${q.row_count} rows`);
    for (const row of q.rows.slice(0, 3)) {
      const cols = Object.entries(row)
        .map(([k, v]) => `${k}=${v}`)
        .join(", ");
      console.log(`    ${cols}`);
    }
    if (q.rows.length > 3) {
      console.log(`    ... (${q.rows.length - 3} more)`);
    }
    console.log();
  }
  console.log(`Reports written to: ${result.report_dir}`);
  console.log(`Summary: ${result.summary_path}`);
}

main();
