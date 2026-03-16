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
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (values.help) {
  console.log(
    `Usage: node api-cli.js [options]

Options:
  --url <url>              Base API URL (default: JSONPlaceholder /posts)
  --raw-dir <path>         Directory for raw JSON output (default: data/raw/api_posts)
  --processed-dir <path>   Directory for processed output (default: data/bronze/api_posts)
  --page-size <n>          Records per page (default: 10)
  --max-pages <n>          Maximum pages to fetch (default: 5)
  -h, --help               Show this help message`,
  );
  process.exit(0);
}

const summary = await runApiPipeline({
  baseUrl: values.url,
  rawDir: values["raw-dir"],
  processedDir: values["processed-dir"],
  pageSize: Number(values["page-size"]),
  maxPages: Number(values["max-pages"]),
});

console.info("\n=== API Ingestion Summary ===");
console.info(`  Run ID:           ${summary.run_id}`);
console.info(`  API URL:          ${summary.api_url}`);
console.info(`  Pages fetched:    ${summary.pages_fetched}`);
console.info(`  Total records:    ${summary.total_records}`);
console.info(`  Records written:  ${summary.records_written}`);
console.info(`  Raw path:         ${summary.raw_path}`);
console.info(`  Processed path:   ${summary.processed_path}`);
console.info(`  Duration:         ${summary.duration_seconds}s`);

if (summary.errors.length > 0) {
  console.warn(`  Errors:           ${summary.errors.join("; ")}`);
  process.exitCode = 1;
}
