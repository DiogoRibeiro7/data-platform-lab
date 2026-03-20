#!/usr/bin/env node

/**
 * CLI entry point for the warehouse loader pipeline.
 *
 * Usage:
 *   node src/warehouse/cli.js
 *   node src/warehouse/cli.js --data-dir ../data/sample --report-dir ../data/gold/warehouse
 *   node src/warehouse/cli.js --config pipeline.json
 */

import { parseArgs } from "node:util";
import { runWarehousePipeline } from "./loader.js";
import { loadConfig, validateConfig } from "../config.js";

const { values } = parseArgs({
  options: {
    "data-dir": { type: "string" },
    "db-path": { type: "string" },
    "report-dir": { type: "string" },
    "sql-dir": { type: "string" },
    config: { type: "string", short: "c" },
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
  -c, --config         Path to a JSON config file
  -h, --help           Show this help message`,
  );
  process.exit(0);
}

// Load config if provided
let configData = {};
if (values.config) {
  try {
    configData = loadConfig(values.config);
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
  const errors = validateConfig(configData, {
    known: ["data_dir", "db_path", "report_dir", "sql_dir"],
  });
  if (errors.length > 0) {
    for (const e of errors) console.error(`Config error: ${e}`);
    process.exit(1);
  }
}

// Merge: defaults < config < CLI flags
const dataDir = values["data-dir"] || configData.data_dir || "../data/sample";
const dbPath = values["db-path"] || configData.db_path || ":memory:";
const reportDir = values["report-dir"] || configData.report_dir || "../data/gold/warehouse";
const sqlDir = values["sql-dir"] || configData.sql_dir || "../sql";

const result = runWarehousePipeline({
  dataDir,
  dbPath,
  reportDir,
  sqlDir,
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
console.log(`Reports written to: ${reportDir}`);
