/**
 * Ingestion — read data from files, APIs, archives, and external sources.
 *
 * Covers flat-file parsing (CSV, JSON), HTTP API consumption with pagination
 * and retries, compressed archive extraction, and log file readers.
 */

export {
  readCsvFile,
  validateColumns,
  standardizeHeaders,
  trimFields,
  deduplicate,
  runPipeline,
} from "./csv-pipeline.js";

export {
  fetchPage,
  fetchAllPages,
  transformPosts,
  saveRaw,
  saveProcessed,
  runApiPipeline,
} from "./api-pipeline.js";
