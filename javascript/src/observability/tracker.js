/**
 * Execution tracking utilities for pipeline observability.
 *
 * Provides a simple timer, a run tracker that collects pipeline metadata
 * (timing, row/file counts, warnings, errors), and formatting helpers.
 */

/**
 * Simple execution timer.
 *
 * @example
 * const timer = new Timer();
 * timer.start();
 * // ... work ...
 * timer.stop();
 * console.log(timer.elapsed); // seconds
 */
export class Timer {
  constructor() {
    this._startMs = null;
    this._endMs = null;
  }

  /** Start the timer. Returns this. */
  start() {
    this._startMs = Date.now();
    this._endMs = null;
    return this;
  }

  /** Stop the timer. Returns this. */
  stop() {
    this._endMs = Date.now();
    return this;
  }

  /**
   * @returns {number} Elapsed seconds. If running, time since start.
   *   If stopped, start-to-stop. If not started, 0.
   */
  get elapsed() {
    if (this._startMs === null) return 0;
    const end = this._endMs !== null ? this._endMs : Date.now();
    return (end - this._startMs) / 1000;
  }

  /** @returns {boolean} */
  get running() {
    return this._startMs !== null && this._endMs === null;
  }
}

/**
 * Collects run metadata for a pipeline execution.
 * Tracks timing, row counts, file counts, warnings, and errors.
 *
 * @example
 * const tracker = new RunTracker("my_pipeline");
 * tracker.start();
 * tracker.incRowsRead(100);
 * tracker.incRowsWritten(95);
 * tracker.incRowsRejected(5);
 * tracker.addWarning("5 rows had null emails");
 * tracker.finish();
 *
 * console.log(formatRunMetadata(tracker.metadata));
 */
export class RunTracker {
  /**
   * @param {string} pipelineName
   * @param {string} [runId] - If not provided, generated from current UTC timestamp
   */
  constructor(pipelineName, runId = null) {
    this._pipelineName = pipelineName;
    this._runId = runId || generateRunId();
    this._timer = new Timer();
    this._status = "pending";
    this._startedAt = null;
    this._endedAt = null;
    this._rowsRead = 0;
    this._rowsWritten = 0;
    this._rowsRejected = 0;
    this._filesProcessed = 0;
    this._filesRejected = 0;
    this._warnings = [];
    this._errors = [];
    this._extra = {};
  }

  /** Start tracking. Returns this. */
  start() {
    this._timer.start();
    this._startedAt = new Date().toISOString();
    this._status = "running";
    return this;
  }

  /**
   * Stop tracking and set final status.
   * @param {string} [status="success"]
   * @returns {RunTracker}
   */
  finish(status = "success") {
    this._timer.stop();
    this._endedAt = new Date().toISOString();
    this._status = status;
    return this;
  }

  /** @param {number} [count=1] */
  incRowsRead(count = 1) { this._rowsRead += count; }

  /** @param {number} [count=1] */
  incRowsWritten(count = 1) { this._rowsWritten += count; }

  /** @param {number} [count=1] */
  incRowsRejected(count = 1) { this._rowsRejected += count; }

  /** @param {number} [count=1] */
  incFilesProcessed(count = 1) { this._filesProcessed += count; }

  /** @param {number} [count=1] */
  incFilesRejected(count = 1) { this._filesRejected += count; }

  /** @param {string} message */
  addWarning(message) { this._warnings.push(message); }

  /** @param {string} message */
  addError(message) { this._errors.push(message); }

  /**
   * Store custom metadata.
   * @param {string} key
   * @param {*} value
   */
  setExtra(key, value) { this._extra[key] = value; }

  /**
   * Build and return the current run metadata snapshot.
   * @returns {{
   *   pipeline_name: string,
   *   run_id: string,
   *   status: string,
   *   started_at: string|null,
   *   ended_at: string|null,
   *   duration_seconds: number,
   *   rows_read: number,
   *   rows_written: number,
   *   rows_rejected: number,
   *   files_processed: number,
   *   files_rejected: number,
   *   warnings: string[],
   *   errors: string[],
   *   extra: Object
   * }}
   */
  get metadata() {
    return {
      pipeline_name: this._pipelineName,
      run_id: this._runId,
      status: this._status,
      started_at: this._startedAt,
      ended_at: this._endedAt,
      duration_seconds: this._timer.elapsed,
      rows_read: this._rowsRead,
      rows_written: this._rowsWritten,
      rows_rejected: this._rowsRejected,
      files_processed: this._filesProcessed,
      files_rejected: this._filesRejected,
      warnings: [...this._warnings],
      errors: [...this._errors],
      extra: { ...this._extra },
    };
  }
}

/**
 * Generate a run ID from the current UTC timestamp: YYYYMMDD_HHMMSS.
 * @returns {string}
 */
export function generateRunId() {
  const d = new Date();
  const yyyy = String(d.getUTCFullYear());
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const min = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}_${hh}${min}${ss}`;
}

/**
 * Format run metadata as a human-readable summary string.
 * @param {object} meta - RunMetadata from tracker.metadata
 * @returns {string}
 */
export function formatRunMetadata(meta) {
  const lines = [
    `=== Run: ${meta.pipeline_name} (${meta.run_id}) ===`,
    `Status: ${meta.status}`,
    `Started: ${meta.started_at || "N/A"}`,
  ];
  if (meta.ended_at) {
    lines.push(`Ended:   ${meta.ended_at}`);
  }
  lines.push(`Duration: ${meta.duration_seconds.toFixed(2)}s`);
  lines.push("");
  lines.push(`Rows read:     ${meta.rows_read}`);
  lines.push(`Rows written:  ${meta.rows_written}`);
  lines.push(`Rows rejected: ${meta.rows_rejected}`);
  if (meta.files_processed || meta.files_rejected) {
    lines.push(`Files processed: ${meta.files_processed}`);
    lines.push(`Files rejected:  ${meta.files_rejected}`);
  }
  if (meta.warnings.length > 0) {
    lines.push("");
    lines.push(`Warnings (${meta.warnings.length}):`);
    for (const w of meta.warnings) {
      lines.push(`  - ${w}`);
    }
  }
  if (meta.errors.length > 0) {
    lines.push("");
    lines.push(`Errors (${meta.errors.length}):`);
    for (const e of meta.errors) {
      lines.push(`  - ${e}`);
    }
  }
  const extraKeys = Object.keys(meta.extra);
  if (extraKeys.length > 0) {
    lines.push("");
    lines.push("Extra:");
    for (const k of extraKeys) {
      lines.push(`  ${k}: ${meta.extra[k]}`);
    }
  }
  return lines.join("\n");
}
