/**
 * Transform — clean, reshape, enrich, and aggregate datasets.
 *
 * Covers column mapping, type coercion, deduplication, filtering, derived
 * fields, and bronze-to-silver-to-gold promotion logic.
 */

export {
  loadCheckpoint,
  saveCheckpoint,
  readEvents,
  transformEvent,
  runIncrementalEtl,
} from "./incremental-etl.js";
