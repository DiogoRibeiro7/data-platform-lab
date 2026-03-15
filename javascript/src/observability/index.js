/**
 * Observability — instrument pipelines with logging, metrics, and lineage.
 *
 * Covers structured logging, execution timing, row-count metrics, data
 * lineage tracking, and pipeline health monitoring.
 */

export {
  Timer,
  RunTracker,
  generateRunId,
  formatRunMetadata,
} from "./tracker.js";
