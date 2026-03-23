/**
 * Shared test helpers — reusable utilities used across multiple test files.
 *
 * Only genuinely duplicated helpers live here. Module-specific factories
 * (like makeEvent) stay in their test files.
 */

import { writeFile, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

// ---------------------------------------------------------------------------
// Repository path constants
// ---------------------------------------------------------------------------

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Absolute path to the repository root. */
export const REPO_ROOT = join(__dirname, "..", "..");

/** `data/sample/` — committed sample datasets. */
export const SAMPLE_DIR = join(REPO_ROOT, "data", "sample");

/** `sql/` — SQL asset directory. */
export const SQL_DIR = join(REPO_ROOT, "sql");

// ---------------------------------------------------------------------------
// File writers
// ---------------------------------------------------------------------------

/**
 * Write a list of objects (or raw strings) as JSONL.
 *
 * @param {string} filePath
 * @param {Array<object|string>} records
 * @returns {Promise<void>}
 */
export async function writeJsonl(filePath, records) {
  const content =
    records
      .map((r) => (typeof r === "string" ? r : JSON.stringify(r)))
      .join("\n") + "\n";
  await writeFile(filePath, content, "utf-8");
}

/**
 * Create a temporary directory with a given prefix.
 *
 * @param {string} [prefix="test-"]
 * @returns {Promise<string>} Path to the created temp directory.
 */
export async function makeTempDir(prefix = "test-") {
  return mkdtemp(join(tmpdir(), prefix));
}
