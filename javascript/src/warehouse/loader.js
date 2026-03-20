/**
 * Warehouse loader — loads raw data into staging tables, runs warehouse
 * transforms (star schema), and executes analytical queries.
 *
 * Uses the same patterns as analytics.js: node:sqlite DatabaseSync,
 * readFileSync for IO, and a simple CSV parser.
 */

import { readFileSync, writeFileSync, mkdirSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

// ---------------------------------------------------------------------------
// Analytical queries against the warehouse star schema
// ---------------------------------------------------------------------------

/** @type {Array<{name: string, description: string, sql: string}>} */
export const WAREHOUSE_QUERIES = [
  {
    name: "warehouse_row_counts",
    description: "Row counts for all warehouse tables",
    sql: `
      SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM dim_customer
      UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
      UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
      UNION ALL SELECT 'fact_order', COUNT(*) FROM fact_order
      UNION ALL SELECT 'fact_order_item', COUNT(*) FROM fact_order_item
      UNION ALL SELECT 'fact_event', COUNT(*) FROM fact_event;`,
  },
  {
    name: "revenue_by_status",
    description: "Total revenue grouped by order status",
    sql: `
      SELECT fo.status,
             COUNT(*) AS order_count,
             ROUND(SUM(fo.total), 2) AS total_revenue
      FROM fact_order fo
      GROUP BY fo.status
      ORDER BY total_revenue DESC;`,
  },
  {
    name: "top_products_warehouse",
    description: "Top products by revenue from the warehouse layer",
    sql: `
      SELECT dp.product_id, dp.name AS product_name, dp.category,
             SUM(fi.quantity) AS units_sold,
             ROUND(SUM(fi.line_total), 2) AS total_revenue
      FROM fact_order_item fi
      JOIN dim_product dp ON dp.product_key = fi.product_key
      GROUP BY dp.product_id, dp.name, dp.category
      ORDER BY total_revenue DESC;`,
  },
  {
    name: "daily_warehouse_revenue",
    description: "Daily revenue from the warehouse layer",
    sql: `
      SELECT dd.date_key, dd.day_of_week, dd.month_name,
             COUNT(*) AS order_count,
             ROUND(SUM(fo.total), 2) AS daily_revenue
      FROM fact_order fo
      JOIN dim_date dd ON dd.date_key = fo.order_date_key
      GROUP BY dd.date_key
      ORDER BY dd.date_key;`,
  },
  {
    name: "customer_spend_warehouse",
    description: "Customer spend from the warehouse layer",
    sql: `
      SELECT dc.customer_id,
             dc.first_name || ' ' || dc.last_name AS full_name,
             dc.country,
             COUNT(fo.order_key) AS order_count,
             ROUND(SUM(fo.total), 2) AS total_spend
      FROM dim_customer dc
      LEFT JOIN fact_order fo ON fo.customer_key = dc.customer_key
      GROUP BY dc.customer_id, full_name, dc.country
      ORDER BY total_spend DESC;`,
  },
];

// ---------------------------------------------------------------------------
// CSV parsing (minimal — no external deps, same pattern as analytics.js)
// ---------------------------------------------------------------------------

function parseCsvContent(content) {
  const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length === 0) return { headers: [], rows: [] };
  const headers = lines[0].split(",");
  const rows = lines.slice(1).map((line) => line.split(","));
  return { headers, rows };
}

// ---------------------------------------------------------------------------
// Core loader functions
// ---------------------------------------------------------------------------

/**
 * Parse a CSV file and load its rows into a staging table.
 *
 * @param {DatabaseSync} db - Open SQLite database
 * @param {string} tableName - Target staging table (must already exist)
 * @param {string} csvPath - Absolute or relative path to the CSV file
 * @returns {number} Number of rows inserted
 */
export function loadRawCsv(db, tableName, csvPath) {
  const content = readFileSync(csvPath, "utf-8");
  const { headers, rows } = parseCsvContent(content);
  if (headers.length === 0) return 0;

  const placeholders = headers.map(() => "?").join(", ");
  const insertSql = `INSERT OR REPLACE INTO ${tableName} (${headers.join(", ")}) VALUES (${placeholders})`;
  const stmt = db.prepare(insertSql);

  for (const row of rows) {
    const values = row.map((v) => (v === "" ? null : v));
    stmt.run(...values);
  }

  return rows.length;
}

/**
 * Read a JSONL file and insert each event into the events staging table.
 *
 * @param {DatabaseSync} db - Open SQLite database
 * @param {string} eventsPath - Path to the JSONL events file
 * @returns {number} Number of rows inserted
 */
export function loadRawEventsJson(db, eventsPath) {
  const content = readFileSync(eventsPath, "utf-8");
  const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);

  const stmt = db.prepare(
    `INSERT INTO events (event_id, type, user_id, page, product_id, quantity, order_id, timestamp)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
  );

  let count = 0;
  for (const line of lines) {
    const evt = JSON.parse(line);
    stmt.run(
      evt.event_id ?? null,
      evt.type ?? null,
      evt.user_id ?? null,
      evt.page ?? null,
      evt.product_id ?? null,
      evt.quantity ?? null,
      evt.order_id ?? null,
      evt.timestamp ?? null,
    );
    count++;
  }

  return count;
}

/**
 * Read a SQL file and execute it against the database.
 *
 * @param {DatabaseSync} db - Open SQLite database
 * @param {string} sqlPath - Path to the .sql file
 */
export function runSqlFile(db, sqlPath) {
  const content = readFileSync(sqlPath, "utf-8");
  db.exec(content);
}

// ---------------------------------------------------------------------------
// Report helpers
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

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/**
 * Run the full warehouse pipeline: DDL -> staging load -> transforms -> queries.
 *
 * @param {object} options
 * @param {string} [options.dataDir="data/sample"] - Directory with raw CSVs and events.json
 * @param {string} [options.dbPath=":memory:"] - SQLite database path
 * @param {string} [options.reportDir] - If set, write query CSVs and summary JSON here
 * @param {string} [options.sqlDir="sql"] - Root directory for DDL/DML/warehouse SQL
 * @returns {object} Structured summary of the pipeline run
 */
export function runWarehousePipeline({
  dataDir = "data/sample",
  dbPath = ":memory:",
  reportDir,
  sqlDir = "sql",
} = {}) {
  // 1. Create database
  const db = new DatabaseSync(dbPath);

  // 2. Execute DDL files (staging tables 01-05, then warehouse dims/facts 06)
  const ddlDir = join(sqlDir, "ddl");
  const ddlFiles = readdirSync(ddlDir)
    .filter((f) => f.endsWith(".sql"))
    .sort();

  for (const file of ddlFiles) {
    runSqlFile(db, join(ddlDir, file));
  }

  // 3. Load raw CSVs into staging tables
  const stagingTables = {};
  const csvMappings = [
    { table: "customers", file: "customers.csv" },
    { table: "products", file: "products.csv" },
    { table: "orders", file: "orders.csv" },
    { table: "order_items", file: "order_items.csv" },
  ];

  for (const { table, file } of csvMappings) {
    const count = loadRawCsv(db, table, join(dataDir, file));
    stagingTables[table] = count;
  }

  // 4. Load events.json
  const eventsCount = loadRawEventsJson(db, join(dataDir, "events.json"));
  stagingTables["events"] = eventsCount;

  // 5. Execute dim_date loader
  runSqlFile(db, join(sqlDir, "dml", "06_load_dim_date.sql"));

  // 6. Execute warehouse transforms (01-05)
  const warehouseDir = join(sqlDir, "warehouse");
  const warehouseFiles = readdirSync(warehouseDir)
    .filter((f) => f.endsWith(".sql") && f.slice(0, 2) <= "05")
    .sort();

  for (const file of warehouseFiles) {
    runSqlFile(db, join(warehouseDir, file));
  }

  // 7. Run analytical queries
  const queryResults = [];
  for (const { name, description, sql } of WAREHOUSE_QUERIES) {
    const stmt = db.prepare(sql);
    const rows = stmt.all().map((row) => ({ ...row }));
    queryResults.push({ name, description, row_count: rows.length, rows });
  }

  // Collect warehouse table counts
  const warehouseTables = {};
  const rowCountQuery = queryResults.find(
    (q) => q.name === "warehouse_row_counts",
  );
  if (rowCountQuery) {
    for (const row of rowCountQuery.rows) {
      warehouseTables[row.table_name] = row.row_count;
    }
  }

  // 8. Write reports if reportDir provided
  if (reportDir) {
    mkdirSync(reportDir, { recursive: true });

    for (const q of queryResults) {
      writeReportCsv(q.rows, join(reportDir, `${q.name}.csv`));
    }

    const summary = {
      status: "success",
      db_path: dbPath,
      staging_tables: stagingTables,
      warehouse_tables: warehouseTables,
      queries: queryResults.map(({ name, description, row_count }) => ({
        name,
        description,
        row_count,
      })),
    };

    writeFileSync(
      join(reportDir, "warehouse_summary.json"),
      JSON.stringify(summary, null, 2),
      "utf-8",
    );
  }

  db.close();

  // 9. Return structured summary
  return {
    status: "success",
    db_path: dbPath,
    staging_tables: stagingTables,
    warehouse_tables: warehouseTables,
    queries: queryResults.map(({ name, description, row_count }) => ({
      name,
      description,
      row_count,
    })),
  };
}
