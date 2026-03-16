import { describe, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  rmSync,
  readFileSync,
  existsSync,
} from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

import {
  parseCsv,
  readSnapshot,
  indexByKey,
  compareRows,
  compareSnapshots,
  writeDiffFiles,
  formatSummary,
} from "../src/transform/snapshot-diff.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const __dirname = dirname(fileURLToPath(import.meta.url));
const sampleDir = join(__dirname, "..", "..", "data", "sample");

function makeTempDir() {
  return mkdtempSync(join(tmpdir(), "snapshot-diff-test-"));
}

function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(","), ...rows.map((r) => r.join(","))];
  writeFileSync(filePath, lines.join("\n") + "\n", "utf-8");
}

// ---------------------------------------------------------------------------
// parseCsv
// ---------------------------------------------------------------------------

describe("parseCsv", () => {
  test("parses basic CSV", () => {
    const content = "id,name,age\n1,Alice,30\n2,Bob,25\n";
    const { headers, rows } = parseCsv(content);

    assert.deepEqual(headers, ["id", "name", "age"]);
    assert.equal(rows.length, 2);
    assert.deepEqual(rows[0], { id: "1", name: "Alice", age: "30" });
    assert.deepEqual(rows[1], { id: "2", name: "Bob", age: "25" });
  });

  test("handles quoted fields with commas", () => {
    const content = 'id,name,address\n1,Alice,"123 Main St, Apt 4"\n';
    const { headers, rows } = parseCsv(content);

    assert.deepEqual(headers, ["id", "name", "address"]);
    assert.equal(rows[0].address, "123 Main St, Apt 4");
  });

  test("strips whitespace from headers and values", () => {
    const content = " id , name , age \n 1 , Alice , 30 \n";
    const { headers, rows } = parseCsv(content);

    assert.deepEqual(headers, ["id", "name", "age"]);
    assert.deepEqual(rows[0], { id: "1", name: "Alice", age: "30" });
  });

  test("skips blank lines", () => {
    const content = "id,name\n\n1,Alice\n\n2,Bob\n\n";
    const { headers, rows } = parseCsv(content);

    assert.deepEqual(headers, ["id", "name"]);
    assert.equal(rows.length, 2);
    assert.deepEqual(rows[0], { id: "1", name: "Alice" });
    assert.deepEqual(rows[1], { id: "2", name: "Bob" });
  });
});

// ---------------------------------------------------------------------------
// indexByKey
// ---------------------------------------------------------------------------

describe("indexByKey", () => {
  test("indexes by single column", () => {
    const rows = [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ];
    const index = indexByKey(rows, ["id"]);

    assert.equal(index.size, 2);
    assert.deepEqual(index.get("1"), { id: "1", name: "Alice" });
    assert.deepEqual(index.get("2"), { id: "2", name: "Bob" });
  });

  test("indexes by composite key", () => {
    const rows = [
      { year: "2024", month: "01", value: "100" },
      { year: "2024", month: "02", value: "200" },
    ];
    const index = indexByKey(rows, ["year", "month"]);

    assert.equal(index.size, 2);
    assert.deepEqual(index.get("2024\x0001"), {
      year: "2024",
      month: "01",
      value: "100",
    });
    assert.deepEqual(index.get("2024\x0002"), {
      year: "2024",
      month: "02",
      value: "200",
    });
  });

  test("throws on duplicate key", () => {
    const rows = [
      { id: "1", name: "Alice" },
      { id: "1", name: "Alice2" },
    ];

    assert.throws(() => indexByKey(rows, ["id"]), {
      message: /Duplicate key found/,
    });
  });

  test("throws on missing key column", () => {
    const rows = [{ id: "1", name: "Alice" }];

    assert.throws(() => indexByKey(rows, ["missing_col"]), {
      message: /Key column "missing_col" not found in data/,
    });
  });
});

// ---------------------------------------------------------------------------
// compareRows
// ---------------------------------------------------------------------------

describe("compareRows", () => {
  test("no changes returns empty array", () => {
    const oldRow = { id: "1", name: "Alice", age: "30" };
    const newRow = { id: "1", name: "Alice", age: "30" };

    const changes = compareRows(oldRow, newRow, ["id"]);
    assert.deepEqual(changes, []);
  });

  test("detects changes", () => {
    const oldRow = { id: "1", name: "Alice", age: "30" };
    const newRow = { id: "1", name: "Alice", age: "31" };

    const changes = compareRows(oldRow, newRow, ["id"]);
    assert.equal(changes.length, 1);
    assert.deepEqual(changes[0], {
      column: "age",
      old_value: "30",
      new_value: "31",
    });
  });

  test("ignores key columns", () => {
    const oldRow = { id: "1", name: "Alice" };
    const newRow = { id: "1", name: "Alice" };

    const changes = compareRows(oldRow, newRow, ["id"]);
    assert.deepEqual(changes, []);
    // Ensure id is not in the comparison even if values differ conceptually
  });

  test("ignores specified columns", () => {
    const oldRow = { id: "1", name: "Alice", updated_at: "2024-01-01" };
    const newRow = { id: "1", name: "Alice", updated_at: "2024-06-01" };

    const changes = compareRows(oldRow, newRow, ["id"], ["updated_at"]);
    assert.deepEqual(changes, []);
  });
});

// ---------------------------------------------------------------------------
// compareSnapshots
// ---------------------------------------------------------------------------

describe("compareSnapshots", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("pure inserts", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name"], [["1", "Alice"]]);
    writeCsv(newPath, ["id", "name"], [
      ["1", "Alice"],
      ["2", "Bob"],
      ["3", "Carla"],
    ]);

    const result = await compareSnapshots(oldPath, newPath, ["id"]);

    assert.equal(result.inserts, 2);
    assert.equal(result.updates, 0);
    assert.equal(result.deletes, 0);
    assert.equal(result.unchanged, 1);
    assert.equal(result.old_row_count, 1);
    assert.equal(result.new_row_count, 3);
  });

  test("pure deletes", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name"], [
      ["1", "Alice"],
      ["2", "Bob"],
      ["3", "Carla"],
    ]);
    writeCsv(newPath, ["id", "name"], [["1", "Alice"]]);

    const result = await compareSnapshots(oldPath, newPath, ["id"]);

    assert.equal(result.inserts, 0);
    assert.equal(result.updates, 0);
    assert.equal(result.deletes, 2);
    assert.equal(result.unchanged, 1);
  });

  test("pure updates", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name", "city"], [
      ["1", "Alice", "Lisbon"],
      ["2", "Bob", "Porto"],
    ]);
    writeCsv(newPath, ["id", "name", "city"], [
      ["1", "Alice", "Berlin"],
      ["2", "Bob", "Madrid"],
    ]);

    const result = await compareSnapshots(oldPath, newPath, ["id"]);

    assert.equal(result.inserts, 0);
    assert.equal(result.updates, 2);
    assert.equal(result.deletes, 0);
    assert.equal(result.unchanged, 0);

    const update1 = result.changes.find((c) => c.key.id === "1");
    assert.equal(update1.changed_columns.length, 1);
    assert.equal(update1.changed_columns[0].column, "city");
    assert.equal(update1.changed_columns[0].old_value, "Lisbon");
    assert.equal(update1.changed_columns[0].new_value, "Berlin");
  });

  test("mixed changes (sample data)", async () => {
    const oldPath = join(sampleDir, "old_snapshot.csv");
    const newPath = join(sampleDir, "new_snapshot.csv");

    const result = await compareSnapshots(oldPath, newPath, ["customer_id"]);

    assert.equal(result.old_row_count, 7);
    assert.equal(result.new_row_count, 8);
    assert.equal(result.inserts, 2);
    assert.equal(result.updates, 3);
    assert.equal(result.deletes, 1);
    assert.equal(result.unchanged, 3);

    // Verify inserts are C008 and C009
    const insertKeys = result.changes
      .filter((c) => c.change_type === "insert")
      .map((c) => c.key.customer_id)
      .sort();
    assert.deepEqual(insertKeys, ["C008", "C009"]);

    // Verify delete is C004
    const deleteKeys = result.changes
      .filter((c) => c.change_type === "delete")
      .map((c) => c.key.customer_id);
    assert.deepEqual(deleteKeys, ["C004"]);

    // Verify updates are C001, C002, C005
    const updateKeys = result.changes
      .filter((c) => c.change_type === "update")
      .map((c) => c.key.customer_id)
      .sort();
    assert.deepEqual(updateKeys, ["C001", "C002", "C005"]);

    // Check specific update details
    const c001 = result.changes.find(
      (c) => c.key.customer_id === "C001" && c.change_type === "update",
    );
    assert.ok(c001);
    const cityChange = c001.changed_columns.find((d) => d.column === "city");
    assert.equal(cityChange.old_value, "Lisbon");
    assert.equal(cityChange.new_value, "Porto");

    const c005 = result.changes.find(
      (c) => c.key.customer_id === "C005" && c.change_type === "update",
    );
    assert.ok(c005);
    const activeChange = c005.changed_columns.find(
      (d) => d.column === "active",
    );
    assert.equal(activeChange.old_value, "true");
    assert.equal(activeChange.new_value, "false");
  });

  test("no changes (identical snapshots)", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name"], [
      ["1", "Alice"],
      ["2", "Bob"],
    ]);
    writeCsv(newPath, ["id", "name"], [
      ["1", "Alice"],
      ["2", "Bob"],
    ]);

    const result = await compareSnapshots(oldPath, newPath, ["id"]);

    assert.equal(result.inserts, 0);
    assert.equal(result.updates, 0);
    assert.equal(result.deletes, 0);
    assert.equal(result.unchanged, 2);
    assert.equal(result.changes.length, 0);
  });

  test("duplicate keys throws", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name"], [
      ["1", "Alice"],
      ["1", "Alice2"],
    ]);
    writeCsv(newPath, ["id", "name"], [["1", "Alice"]]);

    await assert.rejects(() => compareSnapshots(oldPath, newPath, ["id"]), {
      message: /Duplicate key found/,
    });
  });

  test("missing key column throws", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name"], [["1", "Alice"]]);
    writeCsv(newPath, ["id", "name"], [["1", "Alice"]]);

    await assert.rejects(
      () => compareSnapshots(oldPath, newPath, ["missing_col"]),
      {
        message: /Key column "missing_col" not found in data/,
      },
    );
  });

  test("both snapshots empty (headers only)", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name"], []);
    writeCsv(newPath, ["id", "name"], []);

    const result = await compareSnapshots(oldPath, newPath, ["id"]);

    assert.equal(result.inserts, 0);
    assert.equal(result.updates, 0);
    assert.equal(result.deletes, 0);
    assert.equal(result.unchanged, 0);
    assert.equal(result.old_row_count, 0);
    assert.equal(result.new_row_count, 0);
  });

  test("composite key", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["region", "id", "value"], [
      ["US", "1", "100"],
      ["EU", "1", "200"],
    ]);
    writeCsv(newPath, ["region", "id", "value"], [
      ["US", "1", "150"],
      ["EU", "1", "200"],
    ]);

    const result = await compareSnapshots(oldPath, newPath, ["region", "id"]);

    assert.equal(result.inserts, 0);
    assert.equal(result.deletes, 0);
    assert.equal(result.updates, 1);
    assert.equal(result.unchanged, 1);

    const updated = result.changes.filter((c) => c.change_type === "update");
    assert.deepEqual(updated[0].key, { region: "US", id: "1" });
  });

  test("with ignore_columns", async () => {
    const oldPath = join(tempDir, "old.csv");
    const newPath = join(tempDir, "new.csv");

    writeCsv(oldPath, ["id", "name", "updated_at"], [
      ["1", "Alice", "2024-01-01"],
      ["2", "Bob", "2024-01-01"],
    ]);
    writeCsv(newPath, ["id", "name", "updated_at"], [
      ["1", "Alice", "2024-06-01"],
      ["2", "Bob", "2024-06-01"],
    ]);

    // Without ignoring: 2 updates
    const withoutIgnore = await compareSnapshots(oldPath, newPath, ["id"]);
    assert.equal(withoutIgnore.updates, 2);

    // With ignoring: 0 updates
    const withIgnore = await compareSnapshots(oldPath, newPath, ["id"], [
      "updated_at",
    ]);
    assert.equal(withIgnore.updates, 0);
    assert.equal(withIgnore.unchanged, 2);
  });
});

// ---------------------------------------------------------------------------
// writeDiffFiles
// ---------------------------------------------------------------------------

describe("writeDiffFiles", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("writes correct files", async () => {
    const summary = {
      old_row_count: 3,
      new_row_count: 4,
      inserts: 1,
      updates: 1,
      deletes: 1,
      unchanged: 1,
      changes: [
        {
          change_type: "insert",
          key: { id: "3" },
          row: { id: "3", name: "Carla" },
          changed_columns: [],
        },
        {
          change_type: "update",
          key: { id: "1" },
          row: { id: "1", name: "Alice2" },
          changed_columns: [
            { column: "name", old_value: "Alice", new_value: "Alice2" },
          ],
        },
        {
          change_type: "delete",
          key: { id: "2" },
          row: { id: "2", name: "Bob" },
          changed_columns: [],
        },
      ],
    };

    const outputDir = join(tempDir, "output");
    const files = await writeDiffFiles(summary, outputDir, ["id", "name"]);

    // All four file types should be written
    assert.ok(files.inserts);
    assert.ok(files.updates);
    assert.ok(files.deletes);
    assert.ok(files.summary);

    // Verify inserts.csv
    const insertsContent = readFileSync(files.inserts, "utf-8");
    assert.ok(insertsContent.includes("id,name"));
    assert.ok(insertsContent.includes("3,Carla"));

    // Verify updates.csv has extra changed_columns header
    const updatesContent = readFileSync(files.updates, "utf-8");
    assert.ok(updatesContent.includes("id,name,changed_columns"));
    assert.ok(updatesContent.includes("1,Alice2"));

    // Verify deletes.csv
    const deletesContent = readFileSync(files.deletes, "utf-8");
    assert.ok(deletesContent.includes("id,name"));
    assert.ok(deletesContent.includes("2,Bob"));

    // Verify summary.json
    const summaryContent = JSON.parse(readFileSync(files.summary, "utf-8"));
    assert.equal(summaryContent.inserts, 1);
    assert.equal(summaryContent.updates, 1);
    assert.equal(summaryContent.deletes, 1);
  });

  test("skips empty categories", async () => {
    const summary = {
      old_row_count: 2,
      new_row_count: 2,
      inserts: 0,
      updates: 1,
      deletes: 0,
      unchanged: 1,
      changes: [
        {
          change_type: "update",
          key: { id: "1" },
          row: { id: "1", name: "Alice2" },
          changed_columns: [
            { column: "name", old_value: "Alice", new_value: "Alice2" },
          ],
        },
      ],
    };

    const outputDir = join(tempDir, "output");
    const files = await writeDiffFiles(summary, outputDir, ["id", "name"]);

    // Only updates and summary should exist
    assert.ok(files.updates);
    assert.ok(files.summary);
    assert.equal(files.inserts, undefined);
    assert.equal(files.deletes, undefined);

    // Verify files don't exist on disk
    assert.equal(existsSync(join(outputDir, "inserts.csv")), false);
    assert.equal(existsSync(join(outputDir, "deletes.csv")), false);
  });
});

// ---------------------------------------------------------------------------
// formatSummary
// ---------------------------------------------------------------------------

describe("formatSummary", () => {
  test("produces readable output", () => {
    const summary = {
      old_row_count: 7,
      new_row_count: 8,
      inserts: 2,
      updates: 3,
      deletes: 1,
      unchanged: 3,
      changes: [],
    };

    const output = formatSummary(summary);

    assert.ok(output.includes("=== Snapshot Diff Summary ==="));
    assert.ok(output.includes("Old snapshot: 7 rows"));
    assert.ok(output.includes("New snapshot: 8 rows"));
    assert.ok(
      output.includes(
        "Inserts: 2 | Updates: 3 | Deletes: 1 | Unchanged: 3",
      ),
    );
  });
});
