/**
 * Customer ETL workflow — an orchestrated pipeline over real modules.
 *
 * Demonstrates the orchestration runner wired to actual ingestion, validation,
 * and cleaning logic using the data/sample/customers.csv dataset.
 *
 * Steps:
 *   1. extract  — read the CSV, standardise headers, trim fields
 *   2. validate — run data-quality checks
 *   3. clean    — deduplicate rows
 *   4. load     — write cleaned CSV to output path
 *   5. report   — build a human-readable summary string
 */

import { writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

import {
  readCsvFile,
  standardizeHeaders,
  trimFields,
  deduplicate,
} from "../ingestion/csv-pipeline.js";
import {
  checkRequiredColumns,
  checkNoNulls,
  checkUnique,
  checkDateFormat,
} from "../validation/rules.js";
import { runValidation, formatReport } from "../validation/runner.js";
import { Pipeline, formatResult } from "./runner.js";

// ---------------------------------------------------------------------------
// Step functions — each receives the shared context object
// ---------------------------------------------------------------------------

async function extract(ctx) {
  const { headers: rawHeaders, rows: rawRows } = await readCsvFile(
    ctx.input_path,
  );

  const headers = standardizeHeaders(rawHeaders);
  const rows = trimFields(rawRows);

  ctx.headers = headers;
  ctx.rows = rows;

  return {
    rows_read: rows.length,
    columns: headers.length,
  };
}

function validate(ctx) {
  const { headers, rows } = ctx;

  // Convert row arrays to dicts for the validation runner
  const records = rows.map((row) => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) {
      obj[headers[i]] = row[i] ?? "";
    }
    return obj;
  });
  ctx.records = records;

  const checks = [
    [checkRequiredColumns, { required: ["customer_id", "email", "created_at"] }],
    [checkNoNulls, { columns: ["customer_id", "first_name", "last_name"] }],
    [checkUnique, { columns: ["customer_id"] }],
    [checkDateFormat, { column: "created_at" }],
  ];

  const report = runValidation(records, checks, {
    datasetName: "customers",
  });
  ctx.validation_report = report;

  return {
    total_checks: report.total_checks,
    passed: report.passed,
    failed: report.failed,
    status: report.status,
  };
}

function clean(ctx) {
  const rowsBefore = ctx.rows.length;
  const { unique_rows: uniqueRows, removed_count: removedCount } = deduplicate(
    ctx.rows,
  );
  ctx.rows = uniqueRows;

  return {
    rows_before: rowsBefore,
    rows_after: uniqueRows.length,
    duplicates_removed: removedCount,
  };
}

async function load(ctx) {
  const outputPath = ctx.output_path;
  await mkdir(dirname(outputPath), { recursive: true });

  const { headers, rows } = ctx;
  const csvLines = [
    headers.join(","),
    ...rows.map((row) => row.join(",")),
  ];
  await writeFile(outputPath, csvLines.join("\n") + "\n", "utf-8");

  return {
    output_path: outputPath,
    rows_written: rows.length,
  };
}

function report(ctx) {
  const parts = [];

  if (ctx.validation_report) {
    parts.push(formatReport(ctx.validation_report));
  }

  return parts.join("\n");
}

// ---------------------------------------------------------------------------
// Workflow builder
// ---------------------------------------------------------------------------

/**
 * Build and return a Pipeline wired to the customer ETL steps.
 *
 * The caller must pass a context object with `input_path` and `output_path`
 * when calling `.run(context)`.
 *
 * @returns {Pipeline}
 */
export function buildCustomerEtl() {
  const pipeline = new Pipeline("customer_etl");
  pipeline.addStep("extract", extract);
  pipeline.addStep("validate", validate, { allowSkip: true });
  pipeline.addStep("clean", clean);
  pipeline.addStep("load", load);
  pipeline.addStep("report", report);
  return pipeline;
}

/**
 * Build and run the customer ETL pipeline.
 *
 * @param {string} inputPath  - Path to the input CSV file
 * @param {string} outputPath - Path to write the cleaned CSV
 * @returns {Promise<object>} Pipeline result
 */
export async function runCustomerEtl(inputPath, outputPath) {
  const pipeline = buildCustomerEtl();
  return pipeline.run({
    input_path: inputPath,
    output_path: outputPath,
  });
}

export { formatResult };
