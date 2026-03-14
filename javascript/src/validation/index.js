/**
 * Validation — enforce schemas and data quality checks at pipeline boundaries.
 *
 * Covers schema definition, contract enforcement, anomaly detection, row-level
 * and dataset-level checks, and dead-letter routing for invalid records.
 */

export {
  Severity,
  checkRequiredColumns,
  checkNoNulls,
  checkUnique,
  checkNumericRange,
  checkAllowedValues,
  checkDateFormat,
} from "./rules.js";

export {
  runValidation,
  formatReport,
} from "./runner.js";
