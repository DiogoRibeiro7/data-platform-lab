/**
 * Validation rules — pure functions that check data quality constraints.
 *
 * Each rule receives an array of record objects and an options bag,
 * and returns a CheckResult describing whether the check passed.
 */

/** @enum {string} */
export const Severity = Object.freeze({
  WARNING: "warning",
  CRITICAL: "critical",
});

/**
 * @typedef {object} CheckResult
 * @property {string}   name         - Human-readable rule name
 * @property {boolean}  passed       - Whether every row satisfied the rule
 * @property {string}   severity     - "warning" | "critical"
 * @property {string}   message      - Explanation of the outcome
 * @property {number[]} failing_rows - 0-based indices of rows that failed
 */

/**
 * Check that all required column names exist in the records.
 *
 * Columns are considered "present" if at least one record contains the key.
 * An empty dataset is treated as passing (no rows to violate the constraint).
 *
 * @param {object[]} records
 * @param {object}   options
 * @param {string[]} options.required - Column names that must exist
 * @param {string}   [options.severity="critical"]
 * @returns {CheckResult}
 */
export function checkRequiredColumns(records, { required, severity = Severity.CRITICAL }) {
  const presentColumns = new Set();
  for (const record of records) {
    for (const key of Object.keys(record)) {
      presentColumns.add(key);
    }
  }

  const missing = required.filter((col) => !presentColumns.has(col));
  const passed = missing.length === 0;

  return {
    name: "checkRequiredColumns",
    passed,
    severity,
    message: passed
      ? "All required columns are present."
      : `Missing columns: ${missing.join(", ")}`,
    failing_rows: [],
  };
}

/**
 * Check that specified columns have no null, undefined, or empty-string values.
 *
 * @param {object[]} records
 * @param {object}   options
 * @param {string[]} options.columns
 * @param {string}   [options.severity="critical"]
 * @returns {CheckResult}
 */
export function checkNoNulls(records, { columns, severity = Severity.CRITICAL }) {
  const failingRows = [];

  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    for (const col of columns) {
      const value = row[col];
      if (value === null || value === undefined || value === "") {
        failingRows.push(i);
        break; // one failure per row is enough
      }
    }
  }

  const passed = failingRows.length === 0;

  return {
    name: "checkNoNulls",
    passed,
    severity,
    message: passed
      ? `No null values found in columns: ${columns.join(", ")}.`
      : `Found null/empty values in ${failingRows.length} row(s) for columns: ${columns.join(", ")}.`,
    failing_rows: failingRows,
  };
}

/**
 * Check that the combination of specified columns is unique across all rows.
 *
 * @param {object[]} records
 * @param {object}   options
 * @param {string[]} options.columns
 * @param {string}   [options.severity="critical"]
 * @returns {CheckResult}
 */
export function checkUnique(records, { columns, severity = Severity.CRITICAL }) {
  const seen = new Map(); // composite key -> first row index
  const failingRows = [];

  for (let i = 0; i < records.length; i++) {
    const key = columns.map((col) => String(records[i][col])).join("\0");
    if (seen.has(key)) {
      // Mark both the original and the duplicate if not already marked
      const firstIndex = seen.get(key);
      if (!failingRows.includes(firstIndex)) {
        failingRows.push(firstIndex);
      }
      failingRows.push(i);
    } else {
      seen.set(key, i);
    }
  }

  failingRows.sort((a, b) => a - b);
  const passed = failingRows.length === 0;

  return {
    name: "checkUnique",
    passed,
    severity,
    message: passed
      ? `All rows are unique on columns: ${columns.join(", ")}.`
      : `Found duplicate values in ${failingRows.length} row(s) on columns: ${columns.join(", ")}.`,
    failing_rows: failingRows,
  };
}

/**
 * Check that a numeric column falls within [min, max].
 *
 * Rows where the column is missing or not a finite number are skipped
 * (they are not counted as failures).
 *
 * @param {object[]} records
 * @param {object}   options
 * @param {string}   options.column
 * @param {number}   [options.min]  - Lower bound (inclusive). Omit for no lower bound.
 * @param {number}   [options.max]  - Upper bound (inclusive). Omit for no upper bound.
 * @param {string}   [options.severity="warning"]
 * @returns {CheckResult}
 */
export function checkNumericRange(records, { column, min, max, severity = Severity.WARNING }) {
  const failingRows = [];

  for (let i = 0; i < records.length; i++) {
    const value = records[i][column];
    if (value === null || value === undefined || typeof value !== "number" || !Number.isFinite(value)) {
      continue; // skip non-numeric
    }
    if ((min !== undefined && value < min) || (max !== undefined && value > max)) {
      failingRows.push(i);
    }
  }

  const passed = failingRows.length === 0;
  const boundsDesc =
    min !== undefined && max !== undefined
      ? `[${min}, ${max}]`
      : min !== undefined
        ? `[${min}, Infinity)`
        : `(-Infinity, ${max}]`;

  return {
    name: "checkNumericRange",
    passed,
    severity,
    message: passed
      ? `All numeric values in "${column}" are within ${boundsDesc}.`
      : `${failingRows.length} row(s) have "${column}" outside ${boundsDesc}.`,
    failing_rows: failingRows,
  };
}

/**
 * Check that a column contains only values from the allowed set.
 *
 * @param {object[]} records
 * @param {object}          options
 * @param {string}          options.column
 * @param {string[]|Set<string>} options.allowed
 * @param {string}          [options.severity="warning"]
 * @returns {CheckResult}
 */
export function checkAllowedValues(records, { column, allowed, severity = Severity.WARNING }) {
  const allowedSet = allowed instanceof Set ? allowed : new Set(allowed);
  const failingRows = [];

  for (let i = 0; i < records.length; i++) {
    const value = records[i][column];
    if (!allowedSet.has(value)) {
      failingRows.push(i);
    }
  }

  const passed = failingRows.length === 0;

  return {
    name: "checkAllowedValues",
    passed,
    severity,
    message: passed
      ? `All values in "${column}" are within the allowed set.`
      : `${failingRows.length} row(s) have disallowed values in "${column}".`,
    failing_rows: failingRows,
  };
}

/**
 * Return the number of days in a given month (1-12) of a given year.
 * @param {number} year
 * @param {number} month - 1-based month number
 * @returns {number}
 */
function daysInMonth(year, month) {
  const days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (month === 2) {
    const leap = (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
    return leap ? 29 : 28;
  }
  return days[month - 1];
}

/**
 * Check that a column matches an expected date format.
 *
 * Currently supports "YYYY-MM-DD" validation using regex plus calendar
 * validity checks (month 1-12, day valid for that month/year).
 *
 * @param {object[]} records
 * @param {object}   options
 * @param {string}   options.column
 * @param {string}   [options.format="YYYY-MM-DD"]
 * @param {string}   [options.severity="warning"]
 * @returns {CheckResult}
 */
export function checkDateFormat(records, { column, format = "YYYY-MM-DD", severity = Severity.WARNING }) {
  const failingRows = [];

  for (let i = 0; i < records.length; i++) {
    const value = records[i][column];

    if (typeof value !== "string") {
      failingRows.push(i);
      continue;
    }

    if (format === "YYYY-MM-DD") {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        failingRows.push(i);
        continue;
      }
      const [yearStr, monthStr, dayStr] = value.split("-");
      const year = Number(yearStr);
      const month = Number(monthStr);
      const day = Number(dayStr);

      if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
        failingRows.push(i);
      }
    } else {
      // Unsupported format — fail the row to be safe
      failingRows.push(i);
    }
  }

  const passed = failingRows.length === 0;

  return {
    name: "checkDateFormat",
    passed,
    severity,
    message: passed
      ? `All values in "${column}" match format ${format}.`
      : `${failingRows.length} row(s) have invalid date format in "${column}" (expected ${format}).`,
    failing_rows: failingRows,
  };
}
