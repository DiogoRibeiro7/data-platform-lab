/**
 * Streaming — process event data with validation, deduplication, and aggregation.
 *
 * Simulates near-real-time event processing locally using JSONL input files.
 * Events are validated, deduplicated, and routed to accepted or dead-letter
 * outputs with per-sensor aggregate statistics.
 */

export {
  validateEvent,
  deduplicateKey,
  computeAggregates,
  processStream,
  parseEventTime,
  classifyLateness,
} from "./processor.js";
