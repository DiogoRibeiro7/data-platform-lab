/**
 * End-to-end e-commerce demo pipeline.
 *
 * Ingests customers, products, orders, and order_items from data/sample/,
 * validates each dataset, cleans and standardises records, writes curated
 * outputs to data/silver/demo/, and produces a structured run summary.
 *
 * Run from the javascript/ directory:
 *   node src/demo.js
 *
 * Or with a custom data directory:
 *   node src/demo.js --data-dir ../data/sample
 */

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join, dirname } from "node:path";
import { parseArgs } from "node:util";

import {
  readCsvFile,
  standardizeHeaders,
  trimFields,
  deduplicate,
} from "./ingestion/csv-pipeline.js";
import {
  checkRequiredColumns,
  checkNoNulls,
  checkUnique,
  checkNumericRange,
  checkAllowedValues,
  checkDateFormat,
  Severity,
} from "./validation/rules.js";
import { runValidation, formatReport } from "./validation/runner.js";
import { RunTracker, formatRunMetadata } from "./observability/tracker.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function readAndPrepare(filePath) {
  const { headers: rawHeaders, rows: rawRows } = await readCsvFile(filePath);
  const headers = standardizeHeaders(rawHeaders);
  const rows = trimFields(rawRows);
  return { headers, rows };
}

function rowsToDicts(headers, rows) {
  return rows.map((row) => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = row[i] ?? "";
    }
    return obj;
  });
}

async function writeCsv(filePath, headers, rows) {
  await mkdir(dirname(filePath), { recursive: true });
  const lines = [headers.join(","), ...rows.map((r) => r.join(","))];
  await writeFile(filePath, lines.join("\n") + "\n", "utf-8");
}

// ---------------------------------------------------------------------------
// Per-table processing
// ---------------------------------------------------------------------------

function processCustomers(headers, rows, tracker) {
  tracker.incFilesProcessed();
  tracker.incRowsRead(rows.length);

  const records = rowsToDicts(headers, rows);
  const report = runValidation(
    records,
    [
      [checkRequiredColumns, { required: ["customer_id", "email", "created_at"] }],
      [checkNoNulls, { columns: ["customer_id", "first_name", "last_name"] }],
      [checkUnique, { columns: ["customer_id"] }],
      [checkDateFormat, { column: "created_at" }],
    ],
    { datasetName: "customers" },
  );

  if (report.failed > 0) {
    tracker.addWarning(`customers: ${report.failed} validation check(s) failed`);
  }

  // Deduplicate
  const { unique_rows: uniqueRows, removed_count: dups } = deduplicate(rows);
  if (dups > 0) {
    tracker.addWarning(`customers: removed ${dups} duplicate row(s)`);
  }

  // Standardise country casing
  const countryIdx = headers.indexOf("country");
  for (const row of uniqueRows) {
    const val = row[countryIdx].trim();
    row[countryIdx] = val.charAt(0).toUpperCase() + val.slice(1).toLowerCase();
  }

  tracker.incRowsRejected(rows.length - uniqueRows.length);

  return {
    headers,
    rows: uniqueRows,
    summary: {
      source: "customers.csv",
      rows_read: rows.length,
      rows_out: uniqueRows.length,
      duplicates_removed: dups,
      validation_status: report.status,
      validation_checks: report.total_checks,
      validation_passed: report.passed,
      validation_failed: report.failed,
    },
  };
}

function processProducts(headers, rows, tracker) {
  tracker.incFilesProcessed();
  tracker.incRowsRead(rows.length);

  const records = rowsToDicts(headers, rows);
  const report = runValidation(
    records,
    [
      [checkRequiredColumns, { required: ["product_id", "name", "price"] }],
      [checkUnique, { columns: ["product_id"] }],
      [checkNumericRange, { column: "price", min: 0, severity: Severity.WARNING }],
      [checkAllowedValues, { column: "currency", allowed: ["EUR"], severity: Severity.WARNING }],
    ],
    { datasetName: "products" },
  );

  if (report.failed > 0) {
    tracker.addWarning(`products: ${report.failed} validation check(s) failed`);
  }

  // Filter negative prices
  const priceIdx = headers.indexOf("price");
  const cleanRows = [];
  let rejected = 0;
  for (const row of rows) {
    const price = parseFloat(row[priceIdx]);
    if (Number.isNaN(price) || price < 0) {
      rejected++;
      continue;
    }
    cleanRows.push(row);
  }

  if (rejected > 0) {
    tracker.addWarning(`products: filtered ${rejected} row(s) with invalid price`);
  }
  tracker.incRowsRejected(rejected);

  return {
    headers,
    rows: cleanRows,
    summary: {
      source: "products.csv",
      rows_read: rows.length,
      rows_out: cleanRows.length,
      rows_filtered: rejected,
      validation_status: report.status,
    },
  };
}

function processOrders(headers, rows, validCustomerIds, tracker) {
  tracker.incFilesProcessed();
  tracker.incRowsRead(rows.length);

  const records = rowsToDicts(headers, rows);
  const report = runValidation(
    records,
    [
      [checkRequiredColumns, { required: ["order_id", "customer_id", "order_date"] }],
      [checkUnique, { columns: ["order_id"] }],
      [checkAllowedValues, { column: "status", allowed: ["completed", "shipped", "pending", "cancelled"] }],
    ],
    { datasetName: "orders" },
  );

  if (report.failed > 0) {
    tracker.addWarning(`orders: ${report.failed} validation check(s) failed`);
  }

  // Fix date format, count orphan FKs
  const cidIdx = headers.indexOf("customer_id");
  const dateIdx = headers.indexOf("order_date");
  let orphanCount = 0;
  for (const row of rows) {
    row[dateIdx] = row[dateIdx].replace(/\//g, "-");
    if (!validCustomerIds.has(row[cidIdx])) {
      orphanCount++;
    }
  }

  if (orphanCount > 0) {
    tracker.addWarning(`orders: ${orphanCount} row(s) reference non-existent customer_id`);
  }

  return {
    headers,
    rows,
    summary: {
      source: "orders.csv",
      rows_read: rows.length,
      rows_out: rows.length,
      orphan_customer_ids: orphanCount,
      validation_status: report.status,
    },
  };
}

function processOrderItems(headers, rows, tracker) {
  tracker.incFilesProcessed();
  tracker.incRowsRead(rows.length);

  const records = rowsToDicts(headers, rows);
  const report = runValidation(
    records,
    [
      [checkRequiredColumns, { required: ["order_id", "product_id", "quantity", "unit_price"] }],
    ],
    { datasetName: "order_items" },
  );

  if (report.failed > 0) {
    tracker.addWarning(`order_items: ${report.failed} validation check(s) failed`);
  }

  const { unique_rows: uniqueRows, removed_count: dups } = deduplicate(rows);
  if (dups > 0) {
    tracker.addWarning(`order_items: removed ${dups} duplicate row(s)`);
  }
  tracker.incRowsRejected(dups);

  return {
    headers,
    rows: uniqueRows,
    summary: {
      source: "order_items.csv",
      rows_read: rows.length,
      rows_out: uniqueRows.length,
      duplicates_removed: dups,
      validation_status: report.status,
    },
  };
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

/**
 * Run the full e-commerce demo pipeline.
 *
 * @param {object} [options]
 * @param {string} [options.dataDir="data/sample"]
 * @param {string} [options.outputDir="data/silver/demo"]
 * @param {string} [options.manifestDir="data/manifests"]
 * @returns {Promise<{ metadata: object, tables: object, manifest_path: string }>}
 */
export async function runDemo({
  dataDir = "data/sample",
  outputDir = "data/silver/demo",
  manifestDir = "data/manifests",
} = {}) {
  const tracker = new RunTracker("ecommerce_demo");
  const tables = {};

  tracker.start();
  try {
    // --- Customers ---
    const cRaw = await readAndPrepare(join(dataDir, "customers.csv"));
    const c = processCustomers(cRaw.headers, cRaw.rows, tracker);
    await writeCsv(join(outputDir, "customers.csv"), c.headers, c.rows);
    tracker.incRowsWritten(c.rows.length);
    tables.customers = c.summary;

    // --- Products ---
    const pRaw = await readAndPrepare(join(dataDir, "products.csv"));
    const p = processProducts(pRaw.headers, pRaw.rows, tracker);
    await writeCsv(join(outputDir, "products.csv"), p.headers, p.rows);
    tracker.incRowsWritten(p.rows.length);
    tables.products = p.summary;

    // --- Orders (needs valid customer IDs) ---
    const validCids = new Set(c.rows.map((row) => row[c.headers.indexOf("customer_id")]));
    const oRaw = await readAndPrepare(join(dataDir, "orders.csv"));
    const o = processOrders(oRaw.headers, oRaw.rows, validCids, tracker);
    await writeCsv(join(outputDir, "orders.csv"), o.headers, o.rows);
    tracker.incRowsWritten(o.rows.length);
    tables.orders = o.summary;

    // --- Order Items ---
    const oiRaw = await readAndPrepare(join(dataDir, "order_items.csv"));
    const oi = processOrderItems(oiRaw.headers, oiRaw.rows, tracker);
    await writeCsv(join(outputDir, "order_items.csv"), oi.headers, oi.rows);
    tracker.incRowsWritten(oi.rows.length);
    tables.order_items = oi.summary;

    tracker.setExtra("tables_processed", Object.keys(tables).length);
    tracker.setExtra("output_dir", outputDir);
    tracker.finish("success");
  } catch (err) {
    tracker.addError(String(err));
    tracker.finish("failed");
    throw err;
  }

  const meta = tracker.metadata;

  // Write JSON manifest
  await mkdir(manifestDir, { recursive: true });
  const manifestPath = join(manifestDir, `ecommerce_demo_${meta.run_id}.json`);
  const manifest = { run: meta, tables };
  await writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf-8");

  return {
    metadata: meta,
    tables,
    manifest_path: manifestPath,
  };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

async function main() {
  const { values } = parseArgs({
    options: {
      "data-dir": { type: "string", default: "../data/sample" },
      "output-dir": { type: "string", default: "../data/silver/demo" },
      "manifest-dir": { type: "string", default: "../data/manifests" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });

  if (values.help) {
    console.log(
      `Usage: node src/demo.js [options]

Options:
  --data-dir <path>      Directory with sample CSVs (default: ../data/sample)
  --output-dir <path>    Directory for cleaned output (default: ../data/silver/demo)
  --manifest-dir <path>  Directory for run manifest (default: ../data/manifests)
  -h, --help             Show this help message`,
    );
    process.exit(0);
  }

  const result = await runDemo({
    dataDir: values["data-dir"],
    outputDir: values["output-dir"],
    manifestDir: values["manifest-dir"],
  });

  console.log();
  console.log(formatRunMetadata(result.metadata));
  console.log();
  for (const [name, summary] of Object.entries(result.tables)) {
    console.log(`  ${name}: ${summary.rows_read} read -> ${summary.rows_out} out`);
  }
  console.log();
  console.log(`Manifest: ${result.manifest_path}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
