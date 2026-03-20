#!/usr/bin/env node

import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { runBenchmark, formatReport, saveReport } from "./runner.js";
import { loadConfig, validateConfig } from "../config.js";

const { values } = parseArgs({
  options: {
    "work-dir": { type: "string" },
    "num-files": { type: "string" },
    "rows-per-file": { type: "string" },
    "max-workers": { type: "string" },
    config: { type: "string", short: "c" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help) {
  console.log(
    `Usage: node src/benchmark/cli.js [options]

Options:
  --work-dir <path>      Root directory for benchmark files (default: ../data/benchmark)
  --num-files <n>        Number of CSV files to generate (default: 50)
  --rows-per-file <n>    Rows per file (default: 100)
  --max-workers <n>      Concurrency limit for pool strategy (default: 4)
  -c, --config           Path to a JSON config file
  -h, --help             Show this help message`,
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
    known: ["work_dir", "num_files", "rows_per_file", "max_workers"],
  });
  if (errors.length > 0) {
    for (const e of errors) console.error(`Config error: ${e}`);
    process.exit(1);
  }
}

// Merge: defaults < config < CLI flags
const workDir = resolve(
  values["work-dir"] || configData.work_dir || "../data/benchmark",
);
const numFiles = values["num-files"]
  ? parseInt(values["num-files"], 10)
  : configData.num_files ?? 50;
const rowsPerFile = values["rows-per-file"]
  ? parseInt(values["rows-per-file"], 10)
  : configData.rows_per_file ?? 100;
const maxWorkers = values["max-workers"]
  ? parseInt(values["max-workers"], 10)
  : configData.max_workers ?? 4;

console.log(`Generating ${numFiles} files (${rowsPerFile} rows each)...`);

const report = await runBenchmark({ workDir, numFiles, rowsPerFile, maxWorkers });

console.log();
console.log(formatReport(report));

const reportPath = resolve(workDir, "benchmark_report.json");
await saveReport(report, reportPath);
console.log(`\nReport saved to ${reportPath}`);
