#!/usr/bin/env node

import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { runBenchmark, formatReport, saveReport } from "./runner.js";

const { values } = parseArgs({
  options: {
    "work-dir": { type: "string", default: "../data/benchmark" },
    "num-files": { type: "string", default: "50" },
    "rows-per-file": { type: "string", default: "100" },
    "max-workers": { type: "string", default: "4" },
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
  -h, --help             Show this help message`,
  );
  process.exit(0);
}

const workDir = resolve(values["work-dir"]);
const numFiles = parseInt(values["num-files"], 10);
const rowsPerFile = parseInt(values["rows-per-file"], 10);
const maxWorkers = parseInt(values["max-workers"], 10);

console.log(`Generating ${numFiles} files (${rowsPerFile} rows each)...`);

const report = await runBenchmark({ workDir, numFiles, rowsPerFile, maxWorkers });

console.log();
console.log(formatReport(report));

const reportPath = resolve(workDir, "benchmark_report.json");
await saveReport(report, reportPath);
console.log(`\nReport saved to ${reportPath}`);
