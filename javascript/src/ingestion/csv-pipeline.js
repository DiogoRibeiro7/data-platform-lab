import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * Parse a single line of CSV, respecting double-quoted fields that may
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
        // Look ahead: doubled quote is an escaped quote inside a quoted field
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i += 2;
          continue;
        }
        // Otherwise the quoted section ends
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

  // Push the last field
  fields.push(current);
  return fields;
}

/**
 * Read a single CSV file and return its headers and rows.
 *
 * @param {string} filePath - Absolute or relative path to the CSV file.
 * @returns {Promise<{ headers: string[], rows: string[][] }>} Parsed CSV data.
 */
export async function readCsvFile(filePath) {
  const content = await readFile(filePath, "utf-8");
  const lines = content
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0);

  if (lines.length === 0) {
    return { headers: [], rows: [] };
  }

  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map((line) => parseCsvLine(line));

  return { headers, rows };
}

/**
 * Validate that all required columns are present in the headers.
 *
 * @param {string[]} headers - The header row of a CSV file.
 * @param {string[]} requiredColumns - Column names that must be present.
 * @returns {string[]} Array of missing column names (empty if all present).
 */
export function validateColumns(headers, requiredColumns) {
  const headerSet = new Set(headers.map((h) => h.toLowerCase().trim()));
  return requiredColumns.filter(
    (col) => !headerSet.has(col.toLowerCase().trim()),
  );
}

/**
 * Standardize header names: lowercase, trim whitespace, replace spaces
 * with underscores.
 *
 * @param {string[]} headers - Original header names.
 * @returns {string[]} Standardized header names.
 */
export function standardizeHeaders(headers) {
  return headers.map((h) => h.trim().toLowerCase().replace(/\s+/g, "_"));
}

/**
 * Trim whitespace from every string field in the rows.
 *
 * @param {string[][]} rows - Array of row arrays.
 * @returns {string[][]} New array with all fields trimmed.
 */
export function trimFields(rows) {
  return rows.map((row) => row.map((field) => field.trim()));
}

/**
 * Remove exact duplicate rows.
 *
 * @param {string[][]} rows - Array of row arrays.
 * @returns {{ uniqueRows: string[][], removedCount: number }} Deduplicated rows and count of removed duplicates.
 */
export function deduplicate(rows) {
  const seen = new Set();
  const uniqueRows = [];

  for (const row of rows) {
    const key = row.join("\x00");
    if (!seen.has(key)) {
      seen.add(key);
      uniqueRows.push(row);
    }
  }

  return {
    uniqueRows,
    removedCount: rows.length - uniqueRows.length,
  };
}

/**
 * Serialize a row into a CSV line, quoting fields that contain commas,
 * double quotes, or newlines.
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
 * Run the full CSV ingestion and cleaning pipeline.
 *
 * 1. Reads all .csv files from inputDir.
 * 2. Validates required columns (if given), rejecting non-conforming files.
 * 3. Standardizes headers and trims fields.
 * 4. Merges rows from all valid files.
 * 5. Deduplicates across the merged set.
 * 6. Writes the cleaned output CSV.
 *
 * @param {object} options
 * @param {string} options.inputDir - Directory containing CSV files.
 * @param {string} options.outputPath - Path for the output CSV file.
 * @param {string[]} [options.requiredColumns] - Optional list of required column names.
 * @returns {Promise<{
 *   filesProcessed: string[],
 *   filesRejected: string[],
 *   rowsRead: number,
 *   rowsWritten: number,
 *   duplicatesRemoved: number
 * }>} Pipeline result summary.
 */
export async function runPipeline({ inputDir, outputPath, requiredColumns }) {
  const result = {
    filesProcessed: [],
    filesRejected: [],
    rowsRead: 0,
    rowsWritten: 0,
    duplicatesRemoved: 0,
  };

  // 1. Discover CSV files
  let entries;
  try {
    entries = await readdir(inputDir);
  } catch (err) {
    console.warn(`Could not read input directory: ${err.message}`);
    return result;
  }

  const csvFiles = entries
    .filter((f) => f.toLowerCase().endsWith(".csv"))
    .sort();

  if (csvFiles.length === 0) {
    console.info("No CSV files found in input directory.");
    await writeFile(outputPath, "", "utf-8");
    return result;
  }

  let mergedHeaders = null;
  const allRows = [];

  for (const fileName of csvFiles) {
    const filePath = join(inputDir, fileName);

    // 2. Read CSV
    let parsed;
    try {
      parsed = await readCsvFile(filePath);
    } catch (err) {
      const reason = `${fileName}: read error — ${err.message}`;
      console.warn(reason);
      result.filesRejected.push(reason);
      continue;
    }

    if (parsed.headers.length === 0) {
      const reason = `${fileName}: empty or malformed file`;
      console.warn(reason);
      result.filesRejected.push(reason);
      continue;
    }

    // 3. Validate required columns (before standardizing, compare case-insensitively)
    if (requiredColumns && requiredColumns.length > 0) {
      const missing = validateColumns(parsed.headers, requiredColumns);
      if (missing.length > 0) {
        const reason = `${fileName}: missing required columns — ${missing.join(", ")}`;
        console.warn(reason);
        result.filesRejected.push(reason);
        continue;
      }
    }

    // 4. Standardize and trim
    const stdHeaders = standardizeHeaders(parsed.headers);
    const trimmedRows = trimFields(parsed.rows);

    result.rowsRead += trimmedRows.length;

    // 5. Merge — use the headers from the first valid file as canonical
    if (mergedHeaders === null) {
      mergedHeaders = stdHeaders;
    } else {
      // Reorder columns of this file to match the canonical header order
      // (skip files whose standardized headers don't match the canonical set)
      const canonicalSet = new Set(mergedHeaders);
      const thisSet = new Set(stdHeaders);
      const extraCols = stdHeaders.filter((h) => !canonicalSet.has(h));
      const missingCols = mergedHeaders.filter((h) => !thisSet.has(h));

      if (extraCols.length > 0 || missingCols.length > 0) {
        const reason = `${fileName}: header mismatch — extra: [${extraCols.join(", ")}], missing: [${missingCols.join(", ")}]`;
        console.warn(reason);
        result.filesRejected.push(reason);
        continue;
      }

      // Reorder rows to match canonical header order if columns are in a different order
      if (stdHeaders.join(",") !== mergedHeaders.join(",")) {
        const indexMap = mergedHeaders.map((h) => stdHeaders.indexOf(h));
        for (const row of trimmedRows) {
          allRows.push(indexMap.map((i) => row[i] ?? ""));
        }
        result.filesProcessed.push(fileName);
        continue;
      }
    }

    allRows.push(...trimmedRows);
    result.filesProcessed.push(fileName);
  }

  // 6. Deduplicate
  const { uniqueRows, removedCount } = deduplicate(allRows);
  result.duplicatesRemoved = removedCount;
  result.rowsWritten = uniqueRows.length;

  // 7. Write output
  if (mergedHeaders) {
    const lines = [
      rowToCsvLine(mergedHeaders),
      ...uniqueRows.map(rowToCsvLine),
    ];
    await writeFile(outputPath, lines.join("\n") + "\n", "utf-8");
  } else {
    await writeFile(outputPath, "", "utf-8");
  }

  console.info(
    `Pipeline complete: ${result.filesProcessed.length} file(s) processed, ` +
      `${result.filesRejected.length} rejected, ` +
      `${result.rowsRead} rows read, ${result.rowsWritten} written, ` +
      `${result.duplicatesRemoved} duplicates removed.`,
  );

  return result;
}
