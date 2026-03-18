import { describe, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  readCsvFile,
  validateColumns,
  standardizeHeaders,
  trimFields,
  deduplicate,
  runPipeline,
} from "../src/ingestion/csv-pipeline.js";

/**
 * Helper: create a temporary directory and return its path.
 */
function makeTempDir() {
  return mkdtempSync(join(tmpdir(), "csv-pipeline-test-"));
}

/**
 * Helper: write a file with the given content inside a directory.
 */
function writeTemp(dir, fileName, content) {
  const filePath = join(dir, fileName);
  writeFileSync(filePath, content, "utf-8");
  return filePath;
}

// ---------------------------------------------------------------------------
// readCsvFile
// ---------------------------------------------------------------------------

describe("readCsvFile", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("reads a valid CSV file", async () => {
    const csv = "name,age,city\nAlice,30,Lisbon\nBob,25,Porto\n";
    const filePath = writeTemp(tempDir, "valid.csv", csv);

    const { headers, rows } = await readCsvFile(filePath);

    assert.deepStrictEqual(headers, ["name", "age", "city"]);
    assert.equal(rows.length, 2);
    assert.deepStrictEqual(rows[0], ["Alice", "30", "Lisbon"]);
    assert.deepStrictEqual(rows[1], ["Bob", "25", "Porto"]);
  });

  test("handles quoted fields with commas", async () => {
    const csv = 'name,address\nAlice,"Rua da Paz, 10"\nBob,"Av. Central"\n';
    const filePath = writeTemp(tempDir, "quoted.csv", csv);

    const { headers, rows } = await readCsvFile(filePath);

    assert.deepStrictEqual(headers, ["name", "address"]);
    assert.deepStrictEqual(rows[0], ["Alice", "Rua da Paz, 10"]);
    assert.deepStrictEqual(rows[1], ["Bob", "Av. Central"]);
  });

  test("handles empty file", async () => {
    const filePath = writeTemp(tempDir, "empty.csv", "");

    const { headers, rows } = await readCsvFile(filePath);

    assert.deepStrictEqual(headers, []);
    assert.deepStrictEqual(rows, []);
  });
});

// ---------------------------------------------------------------------------
// validateColumns
// ---------------------------------------------------------------------------

describe("validateColumns", () => {
  test("returns empty array when all required columns are present", () => {
    const headers = ["name", "age", "city"];
    const required = ["name", "city"];

    const missing = validateColumns(headers, required);

    assert.deepStrictEqual(missing, []);
  });

  test("returns missing column names", () => {
    const headers = ["name", "age"];
    const required = ["name", "city", "email"];

    const missing = validateColumns(headers, required);

    assert.deepStrictEqual(missing, ["city", "email"]);
  });

  test("performs case-insensitive comparison", () => {
    const headers = ["Name", "AGE", " City "];
    const required = ["name", "age", "city"];

    const missing = validateColumns(headers, required);

    assert.deepStrictEqual(missing, []);
  });
});

// ---------------------------------------------------------------------------
// standardizeHeaders
// ---------------------------------------------------------------------------

describe("standardizeHeaders", () => {
  test("lowercases, trims, and replaces spaces with underscores", () => {
    const headers = [" First Name ", "LAST NAME", "  Email Address  ", "city"];

    const result = standardizeHeaders(headers);

    assert.deepStrictEqual(result, [
      "first_name",
      "last_name",
      "email_address",
      "city",
    ]);
  });

  test("collapses multiple spaces into a single underscore", () => {
    const headers = ["some   header"];

    const result = standardizeHeaders(headers);

    assert.deepStrictEqual(result, ["some_header"]);
  });
});

// ---------------------------------------------------------------------------
// trimFields
// ---------------------------------------------------------------------------

describe("trimFields", () => {
  test("strips whitespace from all fields", () => {
    const rows = [
      [" Alice ", " 30 ", " Lisbon "],
      ["Bob", "25 ", "  Porto"],
    ];

    const result = trimFields(rows);

    assert.deepStrictEqual(result, [
      ["Alice", "30", "Lisbon"],
      ["Bob", "25", "Porto"],
    ]);
  });

  test("handles empty rows", () => {
    const result = trimFields([]);
    assert.deepStrictEqual(result, []);
  });
});

// ---------------------------------------------------------------------------
// deduplicate
// ---------------------------------------------------------------------------

describe("deduplicate", () => {
  test("removes duplicate rows and returns correct count", () => {
    const rows = [
      ["Alice", "30", "Lisbon"],
      ["Bob", "25", "Porto"],
      ["Alice", "30", "Lisbon"],
      ["Carla", "28", "Berlin"],
      ["Bob", "25", "Porto"],
    ];

    const { unique_rows, removed_count } = deduplicate(rows);

    assert.equal(unique_rows.length, 3);
    assert.equal(removed_count, 2);
    assert.deepStrictEqual(unique_rows[0], ["Alice", "30", "Lisbon"]);
    assert.deepStrictEqual(unique_rows[1], ["Bob", "25", "Porto"]);
    assert.deepStrictEqual(unique_rows[2], ["Carla", "28", "Berlin"]);
  });

  test("returns zero removed_count when no duplicates", () => {
    const rows = [
      ["Alice", "30"],
      ["Bob", "25"],
    ];

    const { unique_rows, removed_count } = deduplicate(rows);

    assert.equal(unique_rows.length, 2);
    assert.equal(removed_count, 0);
  });
});

// ---------------------------------------------------------------------------
// runPipeline
// ---------------------------------------------------------------------------

describe("runPipeline", () => {
  let tempDir;
  let outputPath;

  beforeEach(() => {
    tempDir = makeTempDir();
    outputPath = join(tempDir, "output.csv");
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("full pipeline on valid CSVs", async () => {
    writeTemp(
      tempDir,
      "a.csv",
      "Name,Age,City\nAlice,30,Lisbon\nBob,25,Porto\n",
    );
    writeTemp(
      tempDir,
      "b.csv",
      "Name,Age,City\nCarla,28,Berlin\nAlice,30,Lisbon\n",
    );

    const result = await runPipeline({
      inputDir: tempDir,
      outputPath,
    });

    assert.deepStrictEqual(result.files_processed, ["a.csv", "b.csv"]);
    assert.deepStrictEqual(result.files_rejected, []);
    assert.equal(result.rows_read, 4);
    assert.equal(result.rows_written, 3); // Alice duplicate removed
    assert.equal(result.duplicates_removed, 1);

    const output = readFileSync(outputPath, "utf-8");
    const lines = output.trim().split("\n");
    assert.equal(lines[0], "name,age,city"); // standardized headers
    assert.equal(lines.length, 4); // header + 3 data rows
  });

  test("rejects files with missing required columns", async () => {
    writeTemp(
      tempDir,
      "good.csv",
      "name,age,city\nAlice,30,Lisbon\n",
    );
    writeTemp(
      tempDir,
      "bad.csv",
      "name,score\nBob,99\n",
    );

    const result = await runPipeline({
      inputDir: tempDir,
      outputPath,
      requiredColumns: ["name", "age", "city"],
    });

    assert.deepStrictEqual(result.files_processed, ["good.csv"]);
    assert.equal(result.files_rejected.length, 1);
    assert.ok(result.files_rejected[0].includes("bad.csv"));
    assert.ok(result.files_rejected[0].includes("missing required columns"));
    assert.equal(result.rows_read, 1);
    assert.equal(result.rows_written, 1);
  });

  test("handles empty input directory", async () => {
    const emptyDir = makeTempDir();

    try {
      const result = await runPipeline({
        inputDir: emptyDir,
        outputPath,
      });

      assert.deepStrictEqual(result.files_processed, []);
      assert.deepStrictEqual(result.files_rejected, []);
      assert.equal(result.rows_read, 0);
      assert.equal(result.rows_written, 0);
      assert.equal(result.duplicates_removed, 0);
    } finally {
      rmSync(emptyDir, { recursive: true, force: true });
    }
  });

  test("handles malformed CSV gracefully", async () => {
    // A file with only a header and no data
    writeTemp(tempDir, "headeronly.csv", "name,age\n");

    // A completely empty file
    writeTemp(tempDir, "empty.csv", "");

    // A valid file so the pipeline still produces output
    writeTemp(
      tempDir,
      "valid.csv",
      "name,age\nAlice,30\n",
    );

    const result = await runPipeline({
      inputDir: tempDir,
      outputPath,
    });

    // The empty file should be rejected; header-only should be processed
    // (zero rows is valid).
    assert.ok(
      result.files_rejected.some((r) => r.includes("empty.csv")),
      "Empty file should be rejected",
    );
    assert.ok(
      result.files_processed.includes("valid.csv"),
      "Valid file should be processed",
    );
  });

  test("trims whitespace from fields in output", async () => {
    writeTemp(
      tempDir,
      "spaces.csv",
      "name , age , city \n Alice , 30 , Lisbon \n",
    );

    const result = await runPipeline({
      inputDir: tempDir,
      outputPath,
    });

    assert.equal(result.files_processed.length, 1);
    assert.equal(result.rows_written, 1);

    const output = readFileSync(outputPath, "utf-8");
    const lines = output.trim().split("\n");
    assert.equal(lines[0], "name,age,city");
    assert.equal(lines[1], "Alice,30,Lisbon");
  });
});
