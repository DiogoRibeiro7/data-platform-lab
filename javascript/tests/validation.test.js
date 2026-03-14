import { describe, test } from "node:test";
import assert from "node:assert/strict";

import {
  Severity,
  checkRequiredColumns,
  checkNoNulls,
  checkUnique,
  checkNumericRange,
  checkAllowedValues,
  checkDateFormat,
  runValidation,
  formatReport,
} from "../src/validation/index.js";

const VALID_RECORDS = [
  { id: 1, name: "Alice", email: "alice@example.com", age: 30, status: "active", createdAt: "2024-01-15" },
  { id: 2, name: "Bob", email: "bob@example.com", age: 25, status: "active", createdAt: "2024-02-20" },
  { id: 3, name: "Carla", email: "carla@example.com", age: 28, status: "inactive", createdAt: "2024-03-10" },
];

const BAD_RECORDS = [
  { id: 1, name: "Alice", email: "alice@example.com", age: 30, status: "active", createdAt: "2024-01-15" },
  { id: 2, name: "", email: null, age: -5, status: "unknown", createdAt: "15/01/2024" },
  { id: 1, name: "Duplicate", email: "dup@example.com", age: 200, status: "active", createdAt: "2024-13-01" },
  { id: 4, name: "David", age: 35, status: "active", createdAt: "2024-04-05" },
];

// ---------------------------------------------------------------------------
// checkRequiredColumns
// ---------------------------------------------------------------------------
describe("checkRequiredColumns", () => {
  test("all present", () => {
    const result = checkRequiredColumns(VALID_RECORDS, {
      required: ["id", "name", "email"],
    });
    assert.equal(result.passed, true);
    assert.equal(result.severity, Severity.CRITICAL);
    assert.deepEqual(result.failingRows, []);
  });

  test("missing columns", () => {
    const result = checkRequiredColumns(VALID_RECORDS, {
      required: ["id", "phone", "address"],
    });
    assert.equal(result.passed, false);
    assert.ok(result.message.includes("phone"));
    assert.ok(result.message.includes("address"));
  });

  test("empty records", () => {
    const result = checkRequiredColumns([], {
      required: ["id"],
    });
    // No rows means no keys detected, so columns are "missing"
    // but with no rows there's nothing to violate — the spec says
    // columns present if at least one record contains the key.
    // Empty dataset has no keys, so required columns are missing.
    assert.equal(result.passed, false);
  });
});

// ---------------------------------------------------------------------------
// checkNoNulls
// ---------------------------------------------------------------------------
describe("checkNoNulls", () => {
  test("no nulls pass", () => {
    const result = checkNoNulls(VALID_RECORDS, {
      columns: ["id", "name", "email"],
    });
    assert.equal(result.passed, true);
    assert.deepEqual(result.failingRows, []);
  });

  test("nulls found", () => {
    const result = checkNoNulls(BAD_RECORDS, {
      columns: ["email"],
    });
    assert.equal(result.passed, false);
    // Row 1 has null email, row 3 is missing email entirely (undefined)
    assert.ok(result.failingRows.includes(1));
    assert.ok(result.failingRows.includes(3));
  });

  test("empty strings count as null", () => {
    const result = checkNoNulls(BAD_RECORDS, {
      columns: ["name"],
    });
    assert.equal(result.passed, false);
    // Row 1 has empty-string name
    assert.ok(result.failingRows.includes(1));
  });
});

// ---------------------------------------------------------------------------
// checkUnique
// ---------------------------------------------------------------------------
describe("checkUnique", () => {
  test("all unique", () => {
    const result = checkUnique(VALID_RECORDS, {
      columns: ["id"],
    });
    assert.equal(result.passed, true);
    assert.deepEqual(result.failingRows, []);
  });

  test("duplicates found", () => {
    const result = checkUnique(BAD_RECORDS, {
      columns: ["id"],
    });
    assert.equal(result.passed, false);
    // Rows 0 and 2 share id=1
    assert.ok(result.failingRows.includes(0));
    assert.ok(result.failingRows.includes(2));
  });
});

// ---------------------------------------------------------------------------
// checkNumericRange
// ---------------------------------------------------------------------------
describe("checkNumericRange", () => {
  test("within range", () => {
    const result = checkNumericRange(VALID_RECORDS, {
      column: "age",
      min: 0,
      max: 120,
    });
    assert.equal(result.passed, true);
    assert.deepEqual(result.failingRows, []);
  });

  test("out of range", () => {
    const result = checkNumericRange(BAD_RECORDS, {
      column: "age",
      min: 0,
      max: 120,
    });
    assert.equal(result.passed, false);
    // Row 1 has age -5 (below min), row 2 has age 200 (above max)
    assert.ok(result.failingRows.includes(1));
    assert.ok(result.failingRows.includes(2));
  });

  test("only min bound", () => {
    const result = checkNumericRange(BAD_RECORDS, {
      column: "age",
      min: 0,
    });
    assert.equal(result.passed, false);
    // Only row 1 (age -5) fails when there is no max
    assert.deepEqual(result.failingRows, [1]);
  });
});

// ---------------------------------------------------------------------------
// checkAllowedValues
// ---------------------------------------------------------------------------
describe("checkAllowedValues", () => {
  test("all allowed", () => {
    const result = checkAllowedValues(VALID_RECORDS, {
      column: "status",
      allowed: ["active", "inactive"],
    });
    assert.equal(result.passed, true);
    assert.deepEqual(result.failingRows, []);
  });

  test("disallowed values", () => {
    const result = checkAllowedValues(BAD_RECORDS, {
      column: "status",
      allowed: new Set(["active", "inactive"]),
    });
    assert.equal(result.passed, false);
    // Row 1 has status "unknown"
    assert.ok(result.failingRows.includes(1));
  });
});

// ---------------------------------------------------------------------------
// checkDateFormat
// ---------------------------------------------------------------------------
describe("checkDateFormat", () => {
  test("valid dates", () => {
    const result = checkDateFormat(VALID_RECORDS, {
      column: "createdAt",
    });
    assert.equal(result.passed, true);
    assert.deepEqual(result.failingRows, []);
  });

  test("invalid dates", () => {
    const result = checkDateFormat(BAD_RECORDS, {
      column: "createdAt",
    });
    assert.equal(result.passed, false);
    // Row 1: "15/01/2024" wrong format, Row 2: "2024-13-01" month 13 invalid
    assert.ok(result.failingRows.includes(1));
    assert.ok(result.failingRows.includes(2));
  });
});

// ---------------------------------------------------------------------------
// runValidation
// ---------------------------------------------------------------------------
describe("runValidation", () => {
  test("all pass", () => {
    const report = runValidation(VALID_RECORDS, [
      [checkRequiredColumns, { required: ["id", "name"] }],
      [checkNoNulls, { columns: ["id", "name"] }],
      [checkUnique, { columns: ["id"] }],
    ]);
    assert.equal(report.status, "passed");
    assert.equal(report.totalChecks, 3);
    assert.equal(report.passed, 3);
    assert.equal(report.failed, 0);
  });

  test("mixed results", () => {
    const report = runValidation(
      BAD_RECORDS,
      [
        [checkRequiredColumns, { required: ["id", "name"] }],
        [checkNoNulls, { columns: ["email"] }],
        [checkNumericRange, { column: "age", min: 0, max: 120 }],
        [checkAllowedValues, { column: "status", allowed: ["active", "inactive"] }],
      ],
      { datasetName: "users" },
    );
    assert.equal(report.datasetName, "users");
    assert.equal(report.totalChecks, 4);
    assert.ok(report.failed > 0);
  });

  test("status logic", () => {
    // All critical pass, one warning fails => status "warning"
    const warningReport = runValidation(VALID_RECORDS, [
      [checkRequiredColumns, { required: ["id"] }],
      [checkAllowedValues, { column: "status", allowed: ["active"], severity: Severity.WARNING }],
    ]);
    assert.equal(warningReport.status, "warning");

    // One critical fails => status "failed"
    const failedReport = runValidation(BAD_RECORDS, [
      [checkNoNulls, { columns: ["email"], severity: Severity.CRITICAL }],
    ]);
    assert.equal(failedReport.status, "failed");

    // All pass => status "passed"
    const passedReport = runValidation(VALID_RECORDS, [
      [checkRequiredColumns, { required: ["id"] }],
      [checkNoNulls, { columns: ["id"] }],
    ]);
    assert.equal(passedReport.status, "passed");
  });
});

// ---------------------------------------------------------------------------
// formatReport
// ---------------------------------------------------------------------------
describe("formatReport", () => {
  test("produces readable output", () => {
    const report = runValidation(
      VALID_RECORDS,
      [
        [checkRequiredColumns, { required: ["id", "name"] }],
        [checkNoNulls, { columns: ["id"] }],
      ],
      { datasetName: "test-data" },
    );
    const output = formatReport(report);
    assert.ok(typeof output === "string");
    assert.ok(output.includes("test-data"));
    assert.ok(output.includes("PASSED"));
    assert.ok(output.includes("PASS"));
    assert.ok(output.includes("checkRequiredColumns"));
    assert.ok(output.includes("checkNoNulls"));
  });
});
