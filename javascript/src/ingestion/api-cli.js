#!/usr/bin/env node

/**
 * CLI entry point for the API ingestion pipeline.
 *
 * Usage:
 *   node javascript/src/ingestion/api-cli.js \
 *     --url <base_url> \
 *     --raw-dir <path> \
 *     --processed-dir <path> \
 *     --page-size <n> \
 *     --max-pages <n>
 *
 * All arguments are optional and have sensible defaults.
 */

import { parseArgs } from "node:util";
import { runApiPipeline } from "./api-pipeline.js";

const { values } = parseArgs({
  options: {
    url: {
      type: "string",
      default: "https://jsonplaceholder.typicode.com/posts",
    },
    "raw-dir": {
      type: "string",
      default: "data/raw/api_posts",
    },
    "processed-dir": {
      type: "string",
      default: "data/bronze/api_posts",
    },
    "page-size": {
      type: "string",
      default: "10",
    },
    "max-pages": {
      type: "string",
      default: "5",
    },
  },
  strict: true,
});

const summary = await runApiPipeline({
  baseUrl: values.url,
  rawDir: values["raw-dir"],
  processedDir: values["processed-dir"],
  pageSize: Number(values["page-size"]),
  maxPages: Number(values["max-pages"]),
});

console.info("\n=== API Ingestion Summary ===");
console.info(`  Run ID:           ${summary.runId}`);
console.info(`  API URL:          ${summary.apiUrl}`);
console.info(`  Pages fetched:    ${summary.pagesFetched}`);
console.info(`  Total records:    ${summary.totalRecords}`);
console.info(`  Records written:  ${summary.recordsWritten}`);
console.info(`  Raw path:         ${summary.rawPath}`);
console.info(`  Processed path:   ${summary.processedPath}`);
console.info(`  Duration:         ${summary.durationSeconds}s`);

if (summary.errors.length > 0) {
  console.warn(`  Errors:           ${summary.errors.join("; ")}`);
  process.exitCode = 1;
}
