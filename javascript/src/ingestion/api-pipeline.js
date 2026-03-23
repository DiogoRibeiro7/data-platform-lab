/**
 * API ingestion pipeline — fetch, transform, and persist data from a REST API.
 *
 * Uses the JSONPlaceholder API by default. Supports pagination, retry logic
 * for transient failures, and saves both raw and processed output as JSON.
 */

import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { writeManifest } from "../manifest.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Generate a run identifier from the current timestamp (YYYYMMDD_HHMMSS).
 * @returns {string}
 */
function generateRunId() {
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return [
    now.getFullYear(),
    pad(now.getMonth() + 1),
    pad(now.getDate()),
    "_",
    pad(now.getHours()),
    pad(now.getMinutes()),
    pad(now.getSeconds()),
  ].join("");
}

/**
 * Sleep for a given number of milliseconds.
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// fetchPage
// ---------------------------------------------------------------------------

/**
 * Fetch a single page from the API.
 *
 * @param {string} baseUrl  Full URL to the API resource (e.g. https://jsonplaceholder.typicode.com/posts).
 * @param {object} options
 * @param {number} [options.offset=0]      Starting index (`_start` query param).
 * @param {number} [options.limit=10]      Number of records per page (`_limit` query param).
 * @param {number} [options.timeoutMs=10000] Request timeout in milliseconds.
 * @returns {Promise<object[]>} Parsed JSON array of records.
 * @throws On HTTP error, timeout, or malformed response.
 */
export async function fetchPage(
  baseUrl,
  { offset = 0, limit = 10, timeoutMs = 10000 } = {},
) {
  const url = `${baseUrl}?_start=${offset}&_limit=${limit}`;
  const maxRetries = 2;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, {
        signal: AbortSignal.timeout(timeoutMs),
      });

      if (!response.ok) {
        // Retry on 5xx server errors
        if (response.status >= 500 && attempt < maxRetries) {
          console.warn(
            `[fetchPage] Server error ${response.status} on attempt ${attempt + 1}, retrying...`,
          );
          await sleep(1000);
          continue;
        }
        throw new Error(
          `HTTP error: ${response.status} ${response.statusText}`,
        );
      }

      const data = await response.json();

      if (!Array.isArray(data)) {
        throw new Error("Malformed response: expected a JSON array");
      }

      return data;
    } catch (error) {
      // Retry on network / timeout errors (but not on our own thrown HTTP errors)
      const isRetryable =
        error.name === "TimeoutError" ||
        error.name === "AbortError" ||
        error.message === "fetch failed" ||
        error.cause?.code === "ECONNREFUSED" ||
        error.cause?.code === "ENOTFOUND";

      if (isRetryable && attempt < maxRetries) {
        console.warn(
          `[fetchPage] ${error.message} on attempt ${attempt + 1}, retrying...`,
        );
        await sleep(1000);
        continue;
      }

      throw error;
    }
  }
}

// ---------------------------------------------------------------------------
// fetchAllPages
// ---------------------------------------------------------------------------

/**
 * Fetch multiple pages from the API with automatic pagination.
 *
 * Stops when a page returns fewer records than `pageSize` or when
 * `maxPages` pages have been fetched.
 *
 * @param {string} baseUrl  Full URL to the API resource.
 * @param {object} options
 * @param {number} [options.pageSize=10]    Records per page.
 * @param {number} [options.maxPages=5]     Maximum number of pages to fetch.
 * @param {number} [options.timeoutMs=10000] Per-request timeout in milliseconds.
 * @returns {Promise<{ records: object[], pagesFetched: number }>}
 */
export async function fetchAllPages(
  baseUrl,
  { pageSize = 10, maxPages = 5, timeoutMs = 10000 } = {},
) {
  const records = [];
  let pagesFetched = 0;

  for (let page = 0; page < maxPages; page++) {
    const offset = page * pageSize;
    console.info(`[fetchAllPages] Fetching page ${page + 1} (offset=${offset}, limit=${pageSize})`);

    const pageRecords = await fetchPage(baseUrl, {
      offset,
      limit: pageSize,
      timeoutMs,
    });

    records.push(...pageRecords);
    pagesFetched++;

    if (pageRecords.length < pageSize) {
      console.info("[fetchAllPages] Received partial page — pagination complete.");
      break;
    }
  }

  return { records, pagesFetched };
}

// ---------------------------------------------------------------------------
// transformPosts
// ---------------------------------------------------------------------------

/**
 * Transform raw post records into a canonical schema.
 *
 * Output fields: `id`, `user_id`, `title`, `title_length`, `body_preview` (first
 * 100 characters of the body), and `word_count` (words in the body).
 *
 * Records missing any of the required fields (`id`, `userId`, `title`, `body`)
 * are silently skipped.
 *
 * @param {object[]} rawRecords  Array of raw post objects from the API.
 * @returns {object[]} Transformed records.
 */
export function transformPosts(rawRecords) {
  const requiredFields = ["id", "userId", "title", "body"];

  return rawRecords
    .filter((record) => {
      const missing = requiredFields.filter(
        (field) => record[field] === undefined || record[field] === null,
      );
      if (missing.length > 0) {
        console.warn(
          `[transformPosts] Skipping record — missing fields: ${missing.join(", ")}`,
        );
        return false;
      }
      return true;
    })
    .map((record) => ({
      id: record.id,
      user_id: record.userId,
      title: record.title,
      title_length: record.title.length,
      body_preview: String(record.body).slice(0, 100),
      word_count: String(record.body)
        .trim()
        .split(/\s+/)
        .filter(Boolean).length,
    }));
}

// ---------------------------------------------------------------------------
// saveRaw
// ---------------------------------------------------------------------------

/**
 * Save raw records to a JSON file.
 *
 * @param {object[]} records   Records to persist.
 * @param {string}   outputDir Directory to write into (created if absent).
 * @param {string}   runId     Unique run identifier used in the filename.
 * @returns {Promise<string>} Absolute path to the saved file.
 */
export async function saveRaw(records, outputDir, runId) {
  await mkdir(outputDir, { recursive: true });
  const filePath = join(outputDir, `${runId}_raw.json`);
  await writeFile(filePath, JSON.stringify(records, null, 2), "utf-8");
  console.info(`[saveRaw] Wrote ${records.length} records to ${filePath}`);
  return filePath;
}

// ---------------------------------------------------------------------------
// saveProcessed
// ---------------------------------------------------------------------------

/**
 * Save processed (transformed) records to a JSON file.
 *
 * @param {object[]} records   Records to persist.
 * @param {string}   outputDir Directory to write into (created if absent).
 * @param {string}   runId     Unique run identifier used in the filename.
 * @returns {Promise<string>} Absolute path to the saved file.
 */
export async function saveProcessed(records, outputDir, runId) {
  await mkdir(outputDir, { recursive: true });
  const filePath = join(outputDir, `${runId}_processed.json`);
  await writeFile(filePath, JSON.stringify(records, null, 2), "utf-8");
  console.info(`[saveProcessed] Wrote ${records.length} records to ${filePath}`);
  return filePath;
}

// ---------------------------------------------------------------------------
// runApiPipeline
// ---------------------------------------------------------------------------

/**
 * Run the full API ingestion pipeline: fetch, transform, and save.
 *
 * @param {object} options
 * @param {string} [options.baseUrl="https://jsonplaceholder.typicode.com/posts"]
 * @param {string} [options.rawDir="data/raw/api_posts"]
 * @param {string} [options.processedDir="data/bronze/api_posts"]
 * @param {number} [options.pageSize=10]
 * @param {number} [options.maxPages=5]
 * @param {number} [options.timeoutMs=10000]
 * @returns {Promise<{
 *   run_id: string,
 *   api_url: string,
 *   pages_fetched: number,
 *   total_records: number,
 *   records_written: number,
 *   raw_path: string,
 *   processed_path: string,
 *   errors: string[],
 *   duration_seconds: number
 * }>}
 */
export async function runApiPipeline({
  baseUrl = "https://jsonplaceholder.typicode.com/posts",
  rawDir = "data/raw/api_posts",
  processedDir = "data/bronze/api_posts",
  pageSize = 10,
  maxPages = 5,
  timeoutMs = 10000,
} = {}) {
  const startTime = performance.now();
  const runId = generateRunId();
  const errors = [];

  console.info(`[runApiPipeline] Starting run ${runId}`);
  console.info(`[runApiPipeline] API URL: ${baseUrl}`);

  // -- Fetch ------------------------------------------------------------------
  let records = [];
  let pagesFetched = 0;

  try {
    const result = await fetchAllPages(baseUrl, { pageSize, maxPages, timeoutMs });
    records = result.records;
    pagesFetched = result.pagesFetched;
  } catch (error) {
    const msg = `Fetch failed: ${error.message}`;
    console.warn(`[runApiPipeline] ${msg}`);
    errors.push(msg);
  }

  // -- Transform --------------------------------------------------------------
  const transformed = transformPosts(records);

  // -- Save -------------------------------------------------------------------
  let rawPath = "";
  let processedPath = "";

  try {
    rawPath = await saveRaw(records, rawDir, runId);
  } catch (error) {
    const msg = `Failed to save raw data: ${error.message}`;
    console.warn(`[runApiPipeline] ${msg}`);
    errors.push(msg);
  }

  try {
    processedPath = await saveProcessed(transformed, processedDir, runId);
  } catch (error) {
    const msg = `Failed to save processed data: ${error.message}`;
    console.warn(`[runApiPipeline] ${msg}`);
    errors.push(msg);
  }

  const durationSeconds =
    Math.round(((performance.now() - startTime) / 1000) * 1000) / 1000;

  let manifestPath = "";
  try {
    manifestPath = writeManifest({
      pipeline_name: "api_ingestion",
      run_id: runId,
      source: baseUrl,
      output: processedPath,
      row_count: transformed.length,
      status: errors.length > 0 ? "failed" : "success",
      warnings: errors.length > 0 ? errors : undefined,
      extras: {
        pages_fetched: pagesFetched,
        total_records: records.length,
        raw_path: rawPath,
      },
    });
  } catch {
    // Manifest writing is best-effort — skip in test environments
  }

  const summary = {
    run_id: runId,
    api_url: baseUrl,
    pages_fetched: pagesFetched,
    total_records: records.length,
    records_written: transformed.length,
    raw_path: rawPath,
    processed_path: processedPath,
    errors,
    duration_seconds: durationSeconds,
    manifest_path: manifestPath,
  };

  console.info("[runApiPipeline] Pipeline complete:", summary);
  return summary;
}
