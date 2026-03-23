/**
 * Shared manifest writer — platform convention for recording pipeline outputs.
 *
 * A manifest is a lightweight JSON file that records what a pipeline run
 * produced: source inputs, output files, row counts, timestamps, and status.
 *
 * See docs/platform-conventions.md for the canonical field definitions.
 *
 * @module manifest
 */

import { writeFileSync, readFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";

/**
 * Generate a timestamp-based run ID (YYYYMMDD_HHMMSS).
 * @returns {string}
 */
export function generateRunId() {
  const now = new Date();
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const hh = String(now.getUTCHours()).padStart(2, "0");
  const mm = String(now.getUTCMinutes()).padStart(2, "0");
  const ss = String(now.getUTCSeconds()).padStart(2, "0");
  return `${y}${m}${d}_${hh}${mm}${ss}`;
}

/**
 * @typedef {object} ManifestOptions
 * @property {string}   pipeline_name
 * @property {string}   run_id
 * @property {string|string[]} source
 * @property {string|string[]} output
 * @property {number}   row_count
 * @property {string}   [status="success"]
 * @property {string[]} [schema_hint]
 * @property {string[]} [warnings]
 * @property {Object<string, *>} [extras]
 * @property {string}   [manifestDir="data/manifests"]
 */

/**
 * Write a manifest JSON file following platform conventions.
 *
 * @param {ManifestOptions} options
 * @returns {string} Path to the written manifest file.
 */
export function writeManifest({
  pipeline_name,
  run_id,
  source,
  output,
  row_count,
  status = "success",
  schema_hint,
  warnings,
  extras,
  manifestDir = "data/manifests",
}) {
  mkdirSync(manifestDir, { recursive: true });

  const manifest = {
    pipeline_name,
    run_id,
    created_at: new Date().toISOString(),
    source,
    output,
    row_count,
    status,
  };

  if (schema_hint) manifest.schema_hint = schema_hint;
  if (warnings && warnings.length > 0) manifest.warnings = warnings;
  if (extras) Object.assign(manifest, extras);

  const filePath = join(manifestDir, `${pipeline_name}_${run_id}.json`);
  writeFileSync(filePath, JSON.stringify(manifest, null, 2) + "\n", "utf-8");

  return filePath;
}

/**
 * Read and parse a manifest JSON file.
 * @param {string} filePath
 * @returns {object}
 */
export function readManifest(filePath) {
  return JSON.parse(readFileSync(filePath, "utf-8"));
}

/** Required keys for a valid manifest. */
export const MANIFEST_REQUIRED_KEYS = [
  "pipeline_name",
  "run_id",
  "created_at",
  "source",
  "output",
  "row_count",
  "status",
];

/**
 * Validate a manifest object, return list of missing required keys.
 * @param {object} data
 * @returns {string[]}
 */
export function validateManifest(data) {
  return MANIFEST_REQUIRED_KEYS.filter((k) => !(k in data));
}
