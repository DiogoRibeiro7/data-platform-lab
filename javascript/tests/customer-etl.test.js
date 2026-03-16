import { describe, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync,
  writeFileSync,
  rmSync,
  readFileSync,
  existsSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  buildCustomerEtl,
  runCustomerEtl,
  formatResult,
} from "../src/orchestration/customer-etl.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_CSV =
  "customer_id,first_name,last_name,email,city,country,created_at\n" +
  "C001,Alice,Martins,alice@example.com,Lisbon,Portugal,2024-01-15\n" +
  "C002,Bob,Silva,bob@example.com,Porto,Portugal,2024-02-20\n" +
  "C001,Alice,Martins,alice@example.com,Lisbon,Portugal,2024-01-15\n";

function makeTempDir() {
  return mkdtempSync(join(tmpdir(), "customer-etl-test-"));
}

// ---------------------------------------------------------------------------
// End-to-end pipeline tests
// ---------------------------------------------------------------------------

describe("customer ETL workflow", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("end-to-end success", async () => {
    const csvPath = join(tempDir, "customers.csv");
    writeFileSync(csvPath, SAMPLE_CSV, "utf-8");
    const outputPath = join(tempDir, "output", "cleaned.csv");

    const result = await runCustomerEtl(csvPath, outputPath);

    assert.equal(result.status, "success");
    assert.equal(result.pipeline_name, "customer_etl");
    assert.equal(result.steps.length, 5);
    assert.equal(result.steps_passed, 5);
    assert.equal(result.steps_failed, 0);

    // Verify step results
    assert.equal(result.steps[0].result.rows_read, 3);
    assert.equal(result.steps[2].result.duplicates_removed, 1);
    assert.equal(result.steps[3].result.rows_written, 2);

    // Verify output file
    assert.ok(existsSync(outputPath));
    const outputLines = readFileSync(outputPath, "utf-8").trim().split("\n");
    assert.equal(outputLines[0], "customer_id,first_name,last_name,email,city,country,created_at");
    assert.equal(outputLines.length, 3); // header + 2 data rows
  });

  test("validation step detects quality issues", async () => {
    const csvPath = join(tempDir, "customers.csv");
    writeFileSync(csvPath, SAMPLE_CSV, "utf-8");
    const outputPath = join(tempDir, "output", "cleaned.csv");

    const result = await runCustomerEtl(csvPath, outputPath);

    const validateResult = result.steps[1].result;
    assert.equal(validateResult.total_checks, 4);
    assert.ok(validateResult.passed >= 1);
    assert.equal(validateResult.status === "passed" || validateResult.status === "failed" || validateResult.status === "warning", true);
  });

  test("pipeline stops on extract failure", async () => {
    const outputPath = join(tempDir, "output", "cleaned.csv");

    const result = await runCustomerEtl(
      join(tempDir, "nonexistent.csv"),
      outputPath,
    );

    assert.equal(result.status, "failed");
    assert.equal(result.steps[0].status, "failed");
    assert.equal(result.steps_failed, 1);
    // Pipeline stops after extract — remaining steps not executed
    assert.equal(result.steps.length, 1);
  });

  test("format_result produces readable output", async () => {
    const csvPath = join(tempDir, "customers.csv");
    writeFileSync(csvPath, SAMPLE_CSV, "utf-8");
    const outputPath = join(tempDir, "output", "cleaned.csv");

    const result = await runCustomerEtl(csvPath, outputPath);
    const text = formatResult(result);

    assert.ok(text.includes("customer_etl"));
    assert.ok(text.includes("success"));
    assert.ok(text.includes("[PASS] extract"));
    assert.ok(text.includes("[PASS] load"));
  });

  test("pipeline builder returns correct structure", () => {
    const pipeline = buildCustomerEtl("input.csv", "output.csv");

    assert.equal(pipeline.name, "customer_etl");
    assert.equal(pipeline.steps.length, 5);
    assert.equal(pipeline.steps[0].name, "extract");
    assert.equal(pipeline.steps[1].name, "validate");
    assert.equal(pipeline.steps[2].name, "clean");
    assert.equal(pipeline.steps[3].name, "load");
    assert.equal(pipeline.steps[4].name, "report");
    // validate is skippable
    assert.equal(pipeline.steps[1].allowSkip, true);
  });
});
