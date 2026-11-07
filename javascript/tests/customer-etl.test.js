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
import { fileURLToPath } from "node:url";

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

const steps = Object.fromEntries(
  buildCustomerEtl().steps.map(({ name, fn }) => [name, fn]),
);

// ---------------------------------------------------------------------------
// Step and pipeline tests
// ---------------------------------------------------------------------------

describe("customer ETL workflow", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("extract populates the shared context and reports input dimensions", async () => {
    const csvPath = join(tempDir, "customers.csv");
    writeFileSync(csvPath, SAMPLE_CSV, "utf-8");
    const context = { input_path: csvPath };

    assert.deepEqual(await steps.extract(context), { rows_read: 3, columns: 7 });
    assert.deepEqual(context.headers, [
      "customer_id", "first_name", "last_name", "email", "city", "country", "created_at",
    ]);
    assert.equal(context.rows.length, 3);
    assert.deepEqual(context.rows[0], [
      "C001", "Alice", "Martins", "alice@example.com", "Lisbon", "Portugal", "2024-01-15",
    ]);
  });

  test("extract standardizes headers and trims fields", async () => {
    const csvPath = join(tempDir, "messy.csv");
    writeFileSync(csvPath, "  First Name , Last Name \nAlice , Martins \n", "utf-8");
    const context = { input_path: csvPath };

    await steps.extract(context);

    assert.deepEqual(context.headers, ["first_name", "last_name"]);
    assert.deepEqual(context.rows, [["Alice", "Martins"]]);
  });

  test("validate stores records and the detailed report in context", () => {
    const context = {
      headers: ["customer_id", "first_name", "last_name", "email", "created_at"],
      rows: [["C001", "Alice", "Martins", "alice@example.com", "2024-01-15"]],
    };

    assert.deepEqual(steps.validate(context), {
      total_checks: 4, passed: 4, failed: 0, status: "passed",
    });
    assert.deepEqual(context.records, [{
      customer_id: "C001", first_name: "Alice", last_name: "Martins",
      email: "alice@example.com", created_at: "2024-01-15",
    }]);
    assert.equal(context.validation_report.dataset_name, "customers");
    assert.equal(context.validation_report.checks.length, 4);
    assert.ok(context.validation_report.checks.every((check) => check.passed));
  });

  test("clean replaces context rows with unique rows in input order", () => {
    const context = { rows: [["C001", "Alice"], ["C002", "Bob"], ["C001", "Alice"]] };

    assert.deepEqual(steps.clean(context), {
      rows_before: 3, rows_after: 2, duplicates_removed: 1,
    });
    assert.deepEqual(context.rows, [["C001", "Alice"], ["C002", "Bob"]]);
  });

  test("load creates nested directories and writes context headers and rows", async () => {
    const outputPath = join(tempDir, "nested", "output", "customers.csv");
    const context = {
      output_path: outputPath,
      headers: ["customer_id", "first_name"],
      rows: [["C001", "Alice"], ["C002", "Bob"]],
    };

    assert.deepEqual(await steps.load(context), { output_path: outputPath, rows_written: 2 });
    assert.equal(readFileSync(outputPath, "utf-8"), "customer_id,first_name\nC001,Alice\nC002,Bob\n");
  });

  test("report includes validation status, counts, and failed checks", () => {
    const context = {
      headers: ["customer_id", "first_name", "last_name", "email", "created_at"],
      rows: [["C001", "Alice", "Martins", "alice@example.com", "not-a-date"]],
    };
    steps.validate(context);

    const text = steps.report(context);

    assert.match(text, /Validation Report: customers/);
    assert.match(text, /Status: WARNING/);
    assert.match(text, /Total checks: 4/);
    assert.match(text, /Passed: 3/);
    assert.match(text, /Failed: 1/);
    assert.match(text, /\[FAIL\] checkDateFormat/);
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
    writeFileSync(csvPath,
      "customer_id,email,created_at\nC001,a@b.com,2024-01-15\nC001,c@d.com,not-a-date\n",
      "utf-8",
    );
    const outputPath = join(tempDir, "output", "cleaned.csv");

    const result = await runCustomerEtl(csvPath, outputPath);

    assert.deepEqual(result.steps[1].result, {
      total_checks: 4, passed: 1, failed: 3, status: "failed",
    });
    assert.match(result.steps[4].result, /\[FAIL\] checkNoNulls/);
    assert.match(result.steps[4].result, /\[FAIL\] checkUnique/);
    assert.match(result.steps[4].result, /\[FAIL\] checkDateFormat/);
  });

  test("real sample data removes the duplicated customer", async () => {
    const samplePath = fileURLToPath(new URL("../../data/sample/customers.csv", import.meta.url));
    const outputPath = join(tempDir, "cleaned.csv");

    const result = await runCustomerEtl(samplePath, outputPath);

    assert.equal(result.status, "success");
    assert.equal(result.steps[0].result.rows_read, 13);
    assert.equal(result.steps[2].result.duplicates_removed, 1);
    assert.equal(result.steps[3].result.rows_written, 12);
    const lines = readFileSync(outputPath, "utf-8").trim().split("\n");
    assert.equal(lines.length, 13);
    assert.equal(lines.filter((line) => line.startsWith("C003,")).length, 1);
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
    const pipeline = buildCustomerEtl();

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
