import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  generateTestFiles,
  processFile,
  runSequential,
  runConcurrent,
  runPool,
  runBenchmark,
  formatReport,
  saveReport,
} from "../src/benchmark/runner.js";

// ---------------------------------------------------------------------------
// generateTestFiles
// ---------------------------------------------------------------------------
describe("generateTestFiles", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-gen-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("creates correct number of files", async () => {
    const files = await generateTestFiles(tmpDir, 5, 10);
    assert.equal(files.length, 5);
    for (const f of files) {
      assert.ok(existsSync(f), `expected file to exist: ${f}`);
    }
  });

  it("files have correct row count", async () => {
    const rowsPerFile = 15;
    const files = await generateTestFiles(tmpDir, 3, rowsPerFile);

    for (const f of files) {
      const content = await readFile(f, "utf-8");
      const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);
      // header + N data rows
      assert.equal(lines.length, rowsPerFile + 1, `file ${f} should have header + ${rowsPerFile} rows`);
    }
  });

  it("includes quality issues", async () => {
    const files = await generateTestFiles(tmpDir, 1, 10);
    const content = await readFile(files[0], "utf-8");
    const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);
    const dataLines = lines.slice(1);

    // rowId % 10 === 0 => empty email
    const hasEmptyEmail = dataLines.some((line) => {
      const cols = line.split(",");
      // email is the 4th column (index 3)
      return cols[3] === "";
    });
    assert.ok(hasEmptyEmail, "should have at least one row with empty email");

    // rowId % 10 === 3 => uppercase country, rowId % 10 === 7 => lowercase country
    const countries = dataLines.map((line) => line.split(",")[5]);
    const hasUpperCase = countries.some((c) => c === c.toUpperCase() && c.length > 0);
    const hasLowerCase = countries.some((c) => c === c.toLowerCase() && c.length > 0);
    assert.ok(hasUpperCase, "should have at least one uppercase country");
    assert.ok(hasLowerCase, "should have at least one lowercase country");
  });
});

// ---------------------------------------------------------------------------
// processFile
// ---------------------------------------------------------------------------
describe("processFile", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-proc-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("returns correct counts", async () => {
    const inputDir = join(tmpDir, "input");
    const outputDir = join(tmpDir, "output");
    const files = await generateTestFiles(inputDir, 1, 20);

    const result = await processFile(files[0], outputDir);
    assert.equal(result.rows_read, 20);
    // All rows have customer_id, first_name, last_name so all are valid
    assert.equal(result.rows_valid, 20);
    assert.equal(result.rows_invalid, 0);
    assert.ok(result.file_name.endsWith(".csv"));
  });

  it("cleans country casing", async () => {
    const inputDir = join(tmpDir, "input");
    const outputDir = join(tmpDir, "output");
    const files = await generateTestFiles(inputDir, 1, 20);

    await processFile(files[0], outputDir);

    const outputPath = join(outputDir, files[0].split(/[\\/]/).pop());
    const content = await readFile(outputPath, "utf-8");
    const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);
    const dataLines = lines.slice(1);

    for (const line of dataLines) {
      const country = line.split(",")[5];
      if (country) {
        // Title-case: first char uppercase, rest lowercase
        const expected = country.charAt(0).toUpperCase() + country.slice(1).toLowerCase();
        assert.equal(country, expected, `country "${country}" should be title-case`);
      }
    }
  });

  it("lowercases email", async () => {
    const inputDir = join(tmpDir, "input");
    const outputDir = join(tmpDir, "output");
    const files = await generateTestFiles(inputDir, 1, 20);

    await processFile(files[0], outputDir);

    const outputPath = join(outputDir, files[0].split(/[\\/]/).pop());
    const content = await readFile(outputPath, "utf-8");
    const lines = content.split(/\r?\n/).filter((l) => l.trim().length > 0);
    const dataLines = lines.slice(1);

    for (const line of dataLines) {
      const email = line.split(",")[3];
      if (email) {
        assert.equal(email, email.toLowerCase(), `email "${email}" should be lowercase`);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Strategy runners
// ---------------------------------------------------------------------------
describe("runSequential", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-seq-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("processes all files", async () => {
    const inputDir = join(tmpDir, "input");
    const outputDir = join(tmpDir, "output");
    const files = await generateTestFiles(inputDir, 3, 10);

    const results = await runSequential(files, outputDir);
    assert.equal(results.length, 3);
    for (const r of results) {
      assert.ok(r.rows_read > 0);
      assert.ok(r.file_name.endsWith(".csv"));
      const outputPath = join(outputDir, r.file_name);
      assert.ok(existsSync(outputPath), `output file should exist: ${outputPath}`);
    }
  });
});

describe("runConcurrent", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-conc-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("processes all files", async () => {
    const inputDir = join(tmpDir, "input");
    const outputDir = join(tmpDir, "output");
    const files = await generateTestFiles(inputDir, 3, 10);

    const results = await runConcurrent(files, outputDir);
    assert.equal(results.length, 3);
    for (const r of results) {
      assert.ok(r.rows_read > 0);
      assert.ok(r.file_name.endsWith(".csv"));
      const outputPath = join(outputDir, r.file_name);
      assert.ok(existsSync(outputPath), `output file should exist: ${outputPath}`);
    }
  });
});

describe("runPool", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-pool-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("processes all files", async () => {
    const inputDir = join(tmpDir, "input");
    const outputDir = join(tmpDir, "output");
    const files = await generateTestFiles(inputDir, 3, 10);

    const results = await runPool(files, outputDir);
    assert.equal(results.length, 3);
    for (const r of results) {
      assert.ok(r.rows_read > 0);
      assert.ok(r.file_name.endsWith(".csv"));
      const outputPath = join(outputDir, r.file_name);
      assert.ok(existsSync(outputPath), `output file should exist: ${outputPath}`);
    }
  });
});

// ---------------------------------------------------------------------------
// All strategies produce same counts
// ---------------------------------------------------------------------------
describe("all strategies produce same counts", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-same-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("5 files, all 3 strategies produce identical totals", async () => {
    const inputDir = join(tmpDir, "input");
    const files = await generateTestFiles(inputDir, 5, 20);

    const seqResults = await runSequential(files, join(tmpDir, "out_seq"));
    const concResults = await runConcurrent(files, join(tmpDir, "out_conc"));
    const poolResults = await runPool(files, join(tmpDir, "out_pool"));

    const sum = (arr, key) => arr.reduce((s, r) => s + r[key], 0);

    const seqRead = sum(seqResults, "rows_read");
    const seqValid = sum(seqResults, "rows_valid");

    assert.equal(sum(concResults, "rows_read"), seqRead);
    assert.equal(sum(concResults, "rows_valid"), seqValid);
    assert.equal(sum(poolResults, "rows_read"), seqRead);
    assert.equal(sum(poolResults, "rows_valid"), seqValid);
  });
});

// ---------------------------------------------------------------------------
// runBenchmark
// ---------------------------------------------------------------------------
describe("runBenchmark", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-run-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("returns correct report shape", async () => {
    const report = await runBenchmark({
      workDir: tmpDir,
      numFiles: 5,
      rowsPerFile: 20,
    });

    assert.equal(report.num_files, 5);
    assert.equal(report.rows_per_file, 20);
    assert.equal(report.total_rows, 100);
    assert.equal(report.strategies.length, 3);

    const names = report.strategies.map((s) => s.strategy);
    assert.deepEqual(names, ["sequential", "concurrent", "pool"]);
  });

  it("strategies have timing", async () => {
    const report = await runBenchmark({
      workDir: tmpDir,
      numFiles: 5,
      rowsPerFile: 20,
    });

    for (const s of report.strategies) {
      assert.ok(s.total_seconds >= 0, `${s.strategy} should have total_seconds >= 0`);
      assert.equal(s.files_processed, 5, `${s.strategy} should have files_processed === 5`);
    }
  });
});

// ---------------------------------------------------------------------------
// formatReport
// ---------------------------------------------------------------------------
describe("formatReport", () => {
  it("contains expected text", () => {
    const report = {
      num_files: 5,
      rows_per_file: 20,
      total_rows: 100,
      strategies: [
        { strategy: "sequential", total_seconds: 0.1, files_processed: 5, total_rows_read: 100, total_rows_valid: 90, total_rows_invalid: 10 },
        { strategy: "concurrent", total_seconds: 0.05, files_processed: 5, total_rows_read: 100, total_rows_valid: 90, total_rows_invalid: 10 },
        { strategy: "pool", total_seconds: 0.06, files_processed: 5, total_rows_read: 100, total_rows_valid: 90, total_rows_invalid: 10 },
      ],
    };

    const text = formatReport(report);
    assert.ok(text.includes("Benchmark Report"), "should contain 'Benchmark Report'");
    assert.ok(text.includes("sequential"), "should contain 'sequential'");
    assert.ok(text.includes("concurrent"), "should contain 'concurrent'");
    assert.ok(text.includes("pool"), "should contain 'pool'");
    assert.ok(text.includes("Relative to sequential"), "should contain 'Relative to sequential'");
  });
});

// ---------------------------------------------------------------------------
// saveReport
// ---------------------------------------------------------------------------
describe("saveReport", () => {
  let tmpDir;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "bench-save-"));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("writes valid JSON", async () => {
    const report = {
      num_files: 3,
      rows_per_file: 10,
      total_rows: 30,
      strategies: [
        { strategy: "sequential", total_seconds: 0.1, files_processed: 3, total_rows_read: 30, total_rows_valid: 27, total_rows_invalid: 3 },
      ],
    };

    const outputPath = join(tmpDir, "report.json");
    await saveReport(report, outputPath);

    assert.ok(existsSync(outputPath), "report file should exist");
    const raw = await readFile(outputPath, "utf-8");
    const parsed = JSON.parse(raw);
    assert.equal(parsed.num_files, 3);
    assert.equal(parsed.rows_per_file, 10);
    assert.equal(parsed.total_rows, 30);
    assert.equal(parsed.strategies.length, 1);
    assert.equal(parsed.strategies[0].strategy, "sequential");
  });
});
