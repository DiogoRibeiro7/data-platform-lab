#!/usr/bin/env node

import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { runPipeline } from "./csv-pipeline.js";

const { values } = parseArgs({
  options: {
    "input-dir": { type: "string", short: "i" },
    output: { type: "string", short: "o" },
    "required-columns": { type: "string", short: "r" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help || !values["input-dir"] || !values.output) {
  console.log(
    `Usage: node cli.js --input-dir <path> --output <path> [--required-columns col1,col2,...]

Options:
  -i, --input-dir          Directory containing CSV files to ingest
  -o, --output             Path for the cleaned output CSV file
  -r, --required-columns   Comma-separated list of required column names
  -h, --help               Show this help message`,
  );
  process.exit(values.help ? 0 : 1);
}

const inputDir = resolve(values["input-dir"]);
const outputPath = resolve(values.output);
const requiredColumns = values["required-columns"]
  ? values["required-columns"].split(",").map((c) => c.trim())
  : undefined;

const result = await runPipeline({ inputDir, outputPath, requiredColumns });

console.log("\n=== Pipeline Summary ===");
console.log(`Files processed: ${result.files_processed.length}`);
result.files_processed.forEach((f) => console.log(`  - ${f}`));

if (result.files_rejected.length > 0) {
  console.log(`Files rejected:  ${result.files_rejected.length}`);
  result.files_rejected.forEach((r) => console.log(`  - ${r}`));
}

console.log(`Rows read:       ${result.rows_read}`);
console.log(`Rows written:    ${result.rows_written}`);
console.log(`Duplicates removed: ${result.duplicates_removed}`);
