/**
 * Validation runner — executes multiple checks and aggregates results
 * into a single report with overall status.
 */

import { Severity } from "./rules.js";

/**
 * @typedef {import("./rules.js").CheckResult} CheckResult
 */

/**
 * @typedef {object} ValidationReport
 * @property {string}        datasetName      - Name of the dataset being validated
 * @property {number}        totalChecks      - Total number of checks executed
 * @property {number}        passed           - Number of checks that passed
 * @property {number}        failed           - Number of checks that failed
 * @property {number}        warnings         - Number of failed checks with WARNING severity
 * @property {number}        criticalFailures - Number of failed checks with CRITICAL severity
 * @property {"passed"|"warning"|"failed"} status - Overall validation status
 * @property {CheckResult[]} checks           - Individual check results
 */

/**
 * Run a list of validation checks and return an aggregated report.
 *
 * Each entry in `checks` is a tuple of `[ruleFn, options]` where `ruleFn`
 * is one of the exported rule functions and `options` is the options bag
 * it expects.
 *
 * @param {object[]}             records              - The dataset to validate
 * @param {Array<[Function, object]>} checks          - Array of [ruleFn, options] tuples
 * @param {object}               [options]
 * @param {string}               [options.datasetName="dataset"]
 * @returns {ValidationReport}
 */
export function runValidation(records, checks, { datasetName = "dataset" } = {}) {
  const results = [];

  for (const [ruleFn, ruleOptions] of checks) {
    const result = ruleFn(records, ruleOptions);
    results.push(result);
  }

  let passed = 0;
  let failed = 0;
  let warnings = 0;
  let criticalFailures = 0;

  for (const result of results) {
    if (result.passed) {
      passed++;
    } else {
      failed++;
      if (result.severity === Severity.CRITICAL) {
        criticalFailures++;
      } else {
        warnings++;
      }
    }
  }

  let status;
  if (criticalFailures > 0) {
    status = "failed";
  } else if (warnings > 0) {
    status = "warning";
  } else {
    status = "passed";
  }

  return {
    datasetName,
    totalChecks: results.length,
    passed,
    failed,
    warnings,
    criticalFailures,
    status,
    checks: results,
  };
}

/**
 * Format a ValidationReport as a human-readable string.
 *
 * @param {ValidationReport} report
 * @returns {string}
 */
export function formatReport(report) {
  const lines = [];

  lines.push(`Validation Report: ${report.datasetName}`);
  lines.push("=".repeat(40));
  lines.push(`Status: ${report.status.toUpperCase()}`);
  lines.push(`Total checks: ${report.totalChecks}`);
  lines.push(`Passed: ${report.passed}`);
  lines.push(`Failed: ${report.failed}`);
  lines.push(`  Warnings: ${report.warnings}`);
  lines.push(`  Critical: ${report.criticalFailures}`);
  lines.push("");

  for (const check of report.checks) {
    const icon = check.passed ? "PASS" : "FAIL";
    lines.push(`[${icon}] ${check.name} (${check.severity})`);
    lines.push(`       ${check.message}`);
    if (!check.passed && check.failingRows.length > 0) {
      lines.push(`       Failing rows: ${check.failingRows.join(", ")}`);
    }
  }

  return lines.join("\n");
}
