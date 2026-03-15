import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";

/**
 * Parse a single CSV line, respecting double-quoted fields that may
 * contain commas or escaped quotes ("").
 *
 * @param {string} line - A single CSV row as a string.
 * @returns {string[]} Array of field values.
 */
function parseCsvLine(line) {
  const fields = [];
  let current = "";
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const ch = line[i];

    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      current += ch;
      i++;
    } else {
      if (ch === '"') {
        inQuotes = true;
        i++;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
        i++;
      } else {
        current += ch;
        i++;
      }
    }
  }

  fields.push(current);
  return fields;
}

/**
 * Serialize a row of field values into a CSV line, quoting fields that
 * contain commas, double quotes, or newlines.
 *
 * @param {string[]} row - Field values.
 * @returns {string} A single CSV-formatted line.
 */
function rowToCsvLine(row) {
  return row
    .map((field) => {
      if (
        field.includes(",") ||
        field.includes('"') ||
        field.includes("\n") ||
        field.includes("\r")
      ) {
        return '"' + field.replace(/"/g, '""') + '"';
      }
      return field;
    })
    .join(",");
}

/**
 * Parse a CSV string into headers and row objects.
 * Uses a simple parser that handles quoted fields with commas and escaped
 * quotes (""). Strips whitespace from headers and values. Skips blank lines.
 *
 * @param {string} content - Raw CSV string.
 * @returns {{ headers: string[], rows: Object[] }} Headers and array of row objects.
 */
export function parseCsv(content) {
  const lines = content
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0);

  if (lines.length === 0) {
    return { headers: [], rows: [] };
  }

  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i]).map((v) => v.trim());
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[j] ?? "";
    }
    rows.push(row);
  }

  return { headers, rows };
}

/**
 * Read a CSV file and return headers and row objects.
 *
 * @param {string} filePath - Path to the CSV file.
 * @returns {Promise<{ headers: string[], rows: Object[] }>} Parsed CSV data.
 */
export async function readSnapshot(filePath) {
  const content = await readFile(filePath, "utf-8");
  return parseCsv(content);
}

/**
 * Index rows by composite primary key.
 *
 * Builds a Map from a key string (column values joined by \0) to the row
 * object. Throws if a key column is missing from the data or if a duplicate
 * key is found.
 *
 * @param {Object[]} rows - Array of row objects.
 * @param {string[]} keyColumns - Column names forming the primary key.
 * @returns {Map<string, Object>} Map from key string to row object.
 * @throws {Error} If a key column is missing from the data.
 * @throws {Error} If a duplicate key is found.
 */
export function indexByKey(rows, keyColumns) {
  if (rows.length > 0) {
    const firstRowKeys = Object.keys(rows[0]);
    for (const col of keyColumns) {
      if (!firstRowKeys.includes(col)) {
        throw new Error(`Key column "${col}" not found in data`);
      }
    }
  }

  const map = new Map();

  for (const row of rows) {
    const keyStr = keyColumns.map((col) => row[col]).join("\0");
    if (map.has(keyStr)) {
      throw new Error(`Duplicate key found: ${keyStr}`);
    }
    map.set(keyStr, row);
  }

  return map;
}

/**
 * Compare two row objects and return a list of column changes.
 * Excludes key columns and ignored columns from comparison.
 *
 * @param {Object} oldRow - Row object from the old snapshot.
 * @param {Object} newRow - Row object from the new snapshot.
 * @param {string[]} keyColumns - Column names forming the primary key.
 * @param {string[]} [ignoreColumns=[]] - Column names to exclude from comparison.
 * @returns {{ column: string, old_value: string, new_value: string }[]} List of changes.
 */
export function compareRows(oldRow, newRow, keyColumns, ignoreColumns = []) {
  const skip = new Set([...keyColumns, ...ignoreColumns]);
  const changes = [];

  const allColumns = new Set([...Object.keys(oldRow), ...Object.keys(newRow)]);

  for (const col of allColumns) {
    if (skip.has(col)) continue;
    const oldVal = oldRow[col] ?? "";
    const newVal = newRow[col] ?? "";
    if (oldVal !== newVal) {
      changes.push({ column: col, old_value: oldVal, new_value: newVal });
    }
  }

  return changes;
}

/**
 * Build a key object from a row and key columns.
 *
 * @param {Object} row - A row object.
 * @param {string[]} keyColumns - Column names forming the primary key.
 * @returns {Object} Object with only the key columns.
 */
function buildKeyObject(row, keyColumns) {
  const key = {};
  for (const col of keyColumns) {
    key[col] = row[col];
  }
  return key;
}

/**
 * Compare two CSV snapshots and produce a diff summary.
 *
 * Reads both files, indexes by key columns, then classifies every row as
 * an insert, update, delete, or unchanged.
 *
 * @param {string} oldPath - Path to old snapshot CSV.
 * @param {string} newPath - Path to new snapshot CSV.
 * @param {string[]} keyColumns - Column names forming the primary key.
 * @param {string[]} [ignoreColumns=[]] - Column names to exclude from comparison.
 * @returns {Promise<{
 *   old_row_count: number,
 *   new_row_count: number,
 *   inserts: number,
 *   updates: number,
 *   deletes: number,
 *   unchanged: number,
 *   changes: Array<{
 *     change_type: "insert"|"update"|"delete",
 *     key: Object,
 *     row: Object,
 *     changed_columns: Array<{column: string, old_value: string, new_value: string}>
 *   }>
 * }>}
 */
export async function compareSnapshots(
  oldPath,
  newPath,
  keyColumns,
  ignoreColumns = [],
) {
  const oldSnapshot = await readSnapshot(oldPath);
  const newSnapshot = await readSnapshot(newPath);

  const oldIndex = indexByKey(oldSnapshot.rows, keyColumns);
  const newIndex = indexByKey(newSnapshot.rows, keyColumns);

  const changes = [];
  let inserts = 0;
  let updates = 0;
  let deletes = 0;
  let unchanged = 0;

  // Iterate new index: keys not in old are inserts, keys in both need comparison
  for (const [keyStr, newRow] of newIndex) {
    const keyObj = buildKeyObject(newRow, keyColumns);

    if (!oldIndex.has(keyStr)) {
      inserts++;
      changes.push({
        change_type: "insert",
        key: keyObj,
        row: newRow,
        changed_columns: [],
      });
    } else {
      const oldRow = oldIndex.get(keyStr);
      const diffs = compareRows(oldRow, newRow, keyColumns, ignoreColumns);
      if (diffs.length > 0) {
        updates++;
        changes.push({
          change_type: "update",
          key: keyObj,
          row: newRow,
          changed_columns: diffs,
        });
      } else {
        unchanged++;
      }
    }
  }

  // Iterate old index: keys not in new are deletes
  for (const [keyStr, oldRow] of oldIndex) {
    if (!newIndex.has(keyStr)) {
      deletes++;
      const keyObj = buildKeyObject(oldRow, keyColumns);
      changes.push({
        change_type: "delete",
        key: keyObj,
        row: oldRow,
        changed_columns: [],
      });
    }
  }

  // Sort changes by key string for determinism
  changes.sort((a, b) => {
    const aKey = keyColumns.map((col) => a.key[col]).join("\0");
    const bKey = keyColumns.map((col) => b.key[col]).join("\0");
    return aKey < bKey ? -1 : aKey > bKey ? 1 : 0;
  });

  return {
    old_row_count: oldSnapshot.rows.length,
    new_row_count: newSnapshot.rows.length,
    inserts,
    updates,
    deletes,
    unchanged,
    changes,
  };
}

/**
 * Write diff output files.
 *
 * Creates CSV files for inserts, updates, and deletes (only when non-empty),
 * plus a summary.json with the full diff summary. The updates CSV includes an
 * extra `changed_columns` column with a JSON representation of the changes.
 *
 * @param {Object} summary - DiffSummary from compareSnapshots.
 * @param {string} outputDir - Directory to write output files into.
 * @param {string[]} headers - Column headers for CSV output.
 * @returns {Promise<Object>} Map of file type to path.
 */
export async function writeDiffFiles(summary, outputDir, headers) {
  await mkdir(outputDir, { recursive: true });

  const files = {};

  const insertRows = summary.changes.filter(
    (c) => c.change_type === "insert",
  );
  const updateRows = summary.changes.filter(
    (c) => c.change_type === "update",
  );
  const deleteRows = summary.changes.filter(
    (c) => c.change_type === "delete",
  );

  if (insertRows.length > 0) {
    const path = join(outputDir, "inserts.csv");
    const lines = [
      rowToCsvLine(headers),
      ...insertRows.map((c) =>
        rowToCsvLine(headers.map((h) => c.row[h] ?? "")),
      ),
    ];
    await writeFile(path, lines.join("\n") + "\n", "utf-8");
    files.inserts = path;
  }

  if (updateRows.length > 0) {
    const updateHeaders = [...headers, "changed_columns"];
    const path = join(outputDir, "updates.csv");
    const lines = [
      rowToCsvLine(updateHeaders),
      ...updateRows.map((c) => {
        const values = headers.map((h) => c.row[h] ?? "");
        values.push(JSON.stringify(c.changed_columns));
        return rowToCsvLine(values);
      }),
    ];
    await writeFile(path, lines.join("\n") + "\n", "utf-8");
    files.updates = path;
  }

  if (deleteRows.length > 0) {
    const path = join(outputDir, "deletes.csv");
    const lines = [
      rowToCsvLine(headers),
      ...deleteRows.map((c) =>
        rowToCsvLine(headers.map((h) => c.row[h] ?? "")),
      ),
    ];
    await writeFile(path, lines.join("\n") + "\n", "utf-8");
    files.deletes = path;
  }

  const summaryPath = join(outputDir, "summary.json");
  await writeFile(summaryPath, JSON.stringify(summary, null, 2), "utf-8");
  files.summary = summaryPath;

  return files;
}

/**
 * Format a diff summary as a human-readable string.
 *
 * @param {Object} summary - DiffSummary from compareSnapshots.
 * @returns {string} Formatted summary text.
 */
export function formatSummary(summary) {
  return [
    "=== Snapshot Diff Summary ===",
    `Old snapshot: ${summary.old_row_count} rows`,
    `New snapshot: ${summary.new_row_count} rows`,
    `Inserts: ${summary.inserts} | Updates: ${summary.updates} | Deletes: ${summary.deletes} | Unchanged: ${summary.unchanged}`,
  ].join("\n");
}
