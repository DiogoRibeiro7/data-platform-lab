/**
 * Benchmark runner — compares sequential, concurrent, and pool-based file processing.
 *
 * @module benchmark/runner
 */

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";

// ── Synthetic data generation ──────────────────────────────────────────

const SAMPLE_NAMES = [
  ["Alice", "Martins"], ["Bob", "Silva"], ["Carol", "Santos"],
  ["David", "Costa"], ["Eva", "Ferreira"], ["Frank", "Oliveira"],
  ["Grace", "Rodrigues"], ["Hugo", "Almeida"], ["Iris", "Pereira"],
  ["Jack", "Sousa"],
];

const COUNTRIES = ["Portugal", "Spain", "France", "Italy", "Germany"];
const CITIES = ["Lisbon", "Madrid", "Paris", "Rome", "Berlin"];

/**
 * Generate synthetic CSV files for benchmarking.
 *
 * @param {string} outputDir - Directory to write files into.
 * @param {number} [numFiles=50] - Number of CSV files.
 * @param {number} [rowsPerFile=100] - Rows per file.
 * @returns {Promise<string[]>} Array of file paths.
 */
export async function generateTestFiles(outputDir, numFiles = 50, rowsPerFile = 100) {
  await mkdir(outputDir, { recursive: true });
  const files = [];

  for (let fIdx = 0; fIdx < numFiles; fIdx++) {
    const fileName = `batch_${String(fIdx).padStart(4, "0")}.csv`;
    const filePath = join(outputDir, fileName);
    const lines = ["customer_id,first_name,last_name,email,city,country,created_at"];

    for (let rIdx = 0; rIdx < rowsPerFile; rIdx++) {
      const rowId = fIdx * rowsPerFile + rIdx;
      const [first, last] = SAMPLE_NAMES[rowId % SAMPLE_NAMES.length];
      let country = COUNTRIES[rowId % COUNTRIES.length];
      const city = CITIES[rowId % CITIES.length];

      let email = "";
      if (rowId % 10 !== 0) {
        email = `${first.toLowerCase()}.${last.toLowerCase()}.${rowId}@example.com`;
      }
      if (rowId % 10 === 3) country = country.toUpperCase();
      if (rowId % 10 === 7) country = country.toLowerCase();

      lines.push(`C${String(rowId).padStart(6, "0")},${first},${last},${email},${city},${country},2024-01-15`);
    }

    await writeFile(filePath, lines.join("\n") + "\n", "utf-8");
    files.push(filePath);
  }

  return files;
}

// ── Single-file processing ─────────────────────────────────────────────

/**
 * @typedef {object} FileResult
 * @property {string}  file_name
 * @property {number}  rows_read
 * @property {number}  rows_valid
 * @property {number}  rows_invalid
 * @property {number}  duration_seconds
 */

/**
 * Process a single CSV: read, validate, clean, write output.
 *
 * @param {string} inputPath - Path to the input CSV.
 * @param {string} outputDir - Directory for cleaned output.
 * @returns {Promise<FileResult>}
 */
export async function processFile(inputPath, outputDir) {
  const start = performance.now();
  await mkdir(outputDir, { recursive: true });

  const raw = await readFile(inputPath, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);

  if (lines.length === 0) {
    return { file_name: inputPath.split(/[\\/]/).pop(), rows_read: 0, rows_valid: 0, rows_invalid: 0, duration_seconds: 0 };
  }

  const header = lines[0];
  const headers = header.split(",");
  const idIdx = headers.indexOf("customer_id");
  const fnIdx = headers.indexOf("first_name");
  const lnIdx = headers.indexOf("last_name");
  const emailIdx = headers.indexOf("email");
  const countryIdx = headers.indexOf("country");

  let rowsRead = 0;
  let rowsValid = 0;
  let rowsInvalid = 0;
  const cleanedLines = [header];

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",");
    rowsRead++;

    if (!cols[idIdx] || !cols[fnIdx] || !cols[lnIdx]) {
      rowsInvalid++;
      continue;
    }

    // Clean
    if (countryIdx >= 0 && cols[countryIdx]) {
      const c = cols[countryIdx].trim();
      cols[countryIdx] = c.charAt(0).toUpperCase() + c.slice(1).toLowerCase();
    }
    if (emailIdx >= 0 && cols[emailIdx]) {
      cols[emailIdx] = cols[emailIdx].trim().toLowerCase();
    }

    rowsValid++;
    cleanedLines.push(cols.join(","));
  }

  const fileName = inputPath.split(/[\\/]/).pop();
  const outputPath = join(outputDir, fileName);
  await writeFile(outputPath, cleanedLines.join("\n") + "\n", "utf-8");

  const duration = (performance.now() - start) / 1000;
  return {
    file_name: fileName,
    rows_read: rowsRead,
    rows_valid: rowsValid,
    rows_invalid: rowsInvalid,
    duration_seconds: Math.round(duration * 1000000) / 1000000,
  };
}

// ── Strategy implementations ───────────────────────────────────────────

/**
 * Process files sequentially.
 * @param {string[]} files
 * @param {string} outputDir
 * @returns {Promise<FileResult[]>}
 */
export async function runSequential(files, outputDir) {
  const results = [];
  for (const f of files) {
    results.push(await processFile(f, outputDir));
  }
  return results;
}

/**
 * Process all files concurrently (Promise.all, no limit).
 * @param {string[]} files
 * @param {string} outputDir
 * @returns {Promise<FileResult[]>}
 */
export async function runConcurrent(files, outputDir) {
  return Promise.all(files.map((f) => processFile(f, outputDir)));
}

/**
 * Process files with limited concurrency.
 * @param {string[]} files
 * @param {string} outputDir
 * @param {number} [maxWorkers=4]
 * @returns {Promise<FileResult[]>}
 */
export async function runPool(files, outputDir, maxWorkers = 4) {
  const results = new Array(files.length);
  let nextIdx = 0;

  async function worker() {
    while (nextIdx < files.length) {
      const idx = nextIdx++;
      results[idx] = await processFile(files[idx], outputDir);
    }
  }

  const workers = Array.from({ length: Math.min(maxWorkers, files.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

// ── Benchmark orchestrator ─────────────────────────────────────────────

/**
 * @typedef {object} StrategyResult
 * @property {string} strategy
 * @property {number} total_seconds
 * @property {number} files_processed
 * @property {number} total_rows_read
 * @property {number} total_rows_valid
 * @property {number} total_rows_invalid
 */

/**
 * @typedef {object} BenchmarkReport
 * @property {number} num_files
 * @property {number} rows_per_file
 * @property {number} total_rows
 * @property {StrategyResult[]} strategies
 */

/**
 * Run the full benchmark.
 *
 * @param {object} options
 * @param {string} options.workDir - Root directory for benchmark files.
 * @param {number} [options.numFiles=50]
 * @param {number} [options.rowsPerFile=100]
 * @param {number} [options.maxWorkers=4]
 * @returns {Promise<BenchmarkReport>}
 */
export async function runBenchmark({
  workDir,
  numFiles = 50,
  rowsPerFile = 100,
  maxWorkers = 4,
}) {
  const inputDir = join(workDir, "input");
  const files = await generateTestFiles(inputDir, numFiles, rowsPerFile);

  const report = {
    num_files: numFiles,
    rows_per_file: rowsPerFile,
    total_rows: numFiles * rowsPerFile,
    strategies: [],
  };

  const strategies = [
    { name: "sequential", fn: (fs, od) => runSequential(fs, od) },
    { name: "concurrent", fn: (fs, od) => runConcurrent(fs, od) },
    { name: "pool", fn: (fs, od) => runPool(fs, od, maxWorkers) },
  ];

  for (const { name, fn } of strategies) {
    const outputDir = join(workDir, `output_${name}`);
    const start = performance.now();
    const results = await fn(files, outputDir);
    const elapsed = (performance.now() - start) / 1000;

    report.strategies.push({
      strategy: name,
      total_seconds: Math.round(elapsed * 10000) / 10000,
      files_processed: results.length,
      total_rows_read: results.reduce((s, r) => s + r.rows_read, 0),
      total_rows_valid: results.reduce((s, r) => s + r.rows_valid, 0),
      total_rows_invalid: results.reduce((s, r) => s + r.rows_invalid, 0),
    });
  }

  return report;
}

/**
 * Format a benchmark report as a human-readable string.
 * @param {BenchmarkReport} report
 * @returns {string}
 */
export function formatReport(report) {
  const lines = [
    "=== Benchmark Report ===",
    `Files: ${report.num_files}  |  Rows/file: ${report.rows_per_file}  |  Total rows: ${report.total_rows}`,
    "",
    `${"Strategy".padEnd(15)} ${"Time (s)".padStart(10)} ${"Files".padStart(8)} ${"Rows".padStart(10)} ${"Valid".padStart(10)} ${"Invalid".padStart(10)}`,
    "-".repeat(65),
  ];

  for (const s of report.strategies) {
    lines.push(
      `${s.strategy.padEnd(15)} ${s.total_seconds.toFixed(4).padStart(10)} ${String(s.files_processed).padStart(8)} ` +
      `${String(s.total_rows_read).padStart(10)} ${String(s.total_rows_valid).padStart(10)} ${String(s.total_rows_invalid).padStart(10)}`,
    );
  }

  if (report.strategies.length >= 2) {
    const seqTime = report.strategies[0].total_seconds;
    if (seqTime > 0) {
      lines.push("");
      lines.push("Relative to sequential:");
      for (const s of report.strategies.slice(1)) {
        if (s.total_seconds > 0) {
          const speedup = seqTime / s.total_seconds;
          lines.push(`  ${s.strategy}: ${speedup.toFixed(2)}x`);
        }
      }
    }
  }

  return lines.join("\n");
}

/**
 * Save a benchmark report as JSON.
 * @param {BenchmarkReport} report
 * @param {string} outputPath
 * @returns {Promise<void>}
 */
export async function saveReport(report, outputPath) {
  const dir = outputPath.substring(0, outputPath.lastIndexOf("/"));
  if (dir) await mkdir(dir, { recursive: true });
  await writeFile(outputPath, JSON.stringify(report, null, 2) + "\n", "utf-8");
}
