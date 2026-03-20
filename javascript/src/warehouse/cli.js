#!/usr/bin/env node

/**
 * CLI entry point for the warehouse loader pipeline.
 *
 * Usage:
 *   node src/warehouse/cli.js
 *   node src/warehouse/cli.js --data-dir ../data/sample --report-dir ../data/gold/warehouse
 */

import { parseArgs } from "node:util";
import { runWarehousePipeline } from "./loader.js";

const { values } = parseArgs({
  options: {
    "data-dir": { type: "string", default: "../data/sample" },
    "db-path": { type: "string", default: ":memory:" },
    "report-dir": { type: "string", default: "../data/gold/warehouse" },
    "sql-dir": { type: "string", default: "../sql" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help) {
  console.log(
    `Usage: node src/warehouse/cli.js [options]

Options:
  --data-dir <path>    Directory with raw CSVs and events.json (default: ../data/sample)
  --db-path <path>     SQLite DB path (default: :memory:)
  --report-dir <path>  Directory for report CSVs and summary (default: ../data/gold/warehouse)
  --sql-dir <path>     Root directory for SQL files (default: ../sql)
  -h, --help           Show this help message`,
  );
  process.exit(0);
}

const result = runWarehousePipeline({
  dataDir: values["data-dir"],
  dbPath: values["db-path"],
  reportDir: values["report-dir"],
  sqlDir: values["sql-dir"],
});

console.log();
console.log("=== Warehouse Pipeline ===");
console.log();

console.log("Staging tables loaded:");
for (const [table, count] of Object.entries(result.staging_tables)) {
  console.log(`  ${table}: ${count} rows`);
}

console.log();
console.log("Warehouse tables populated:");
for (const [table, count] of Object.entries(result.warehouse_tables)) {
  console.log(`  ${table}: ${count} rows`);
}

console.log();
console.log("Query results:");
for (const q of result.queries) {
  console.log(`  ${q.name}: ${q.row_count} rows`);
}

console.log();
console.log(`Reports written to: ${values["report-dir"]}`);
