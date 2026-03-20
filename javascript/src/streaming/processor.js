import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";

/**
 * @typedef {object} SensorEvent
 * @property {string} sensor_id
 * @property {string} type
 * @property {*}      value
 * @property {string} unit
 * @property {string} location
 * @property {string} timestamp
 */

/**
 * @typedef {object} ValidationResult
 * @property {SensorEvent} event
 * @property {"accepted"|"rejected"|"duplicate"} status
 * @property {string|null} reason
 */

/**
 * @typedef {object} SensorAggregates
 * @property {Object<string, {count: number, min_value: number, max_value: number, avg_value: number}>} by_sensor
 * @property {Object<string, number>} by_type
 * @property {Object<string, number>} by_location
 */

/**
 * @typedef {object} PipelineSummary
 * @property {string}  pipeline_name
 * @property {string}  run_at
 * @property {number}  duration_seconds
 * @property {string}  status
 * @property {number}  events_seen
 * @property {number}  events_accepted
 * @property {number}  events_rejected
 * @property {number}  events_duplicate
 * @property {number}  dead_letter_count
 * @property {number}  events_late
 * @property {number}  max_lateness_seconds
 * @property {string}  watermark
 * @property {number}  lateness_threshold_seconds
 * @property {SensorAggregates} aggregates
 * @property {Object<string, number>} rejection_reasons
 */

/**
 * Check whether a timestamp string is parseable as a valid date.
 *
 * @param {string} ts - Timestamp string to validate.
 * @returns {boolean} True if the timestamp is valid.
 */
function isValidTimestamp(ts) {
  try {
    const d = new Date(ts);
    const iso = d.toISOString();
    return iso !== "Invalid Date";
  } catch {
    return false;
  }
}

/**
 * Parse an ISO 8601 timestamp string into a Date object.
 *
 * @param {string} ts - ISO timestamp string.
 * @returns {Date} Parsed Date object.
 */
export function parseEventTime(ts) {
  return new Date(ts);
}

/**
 * Determine if an event is late relative to the current watermark.
 *
 * @param {Date} eventTime - The event's timestamp as a Date.
 * @param {Date|null} watermark - The current watermark (max event time seen).
 * @param {number} thresholdSeconds - Allowed lateness in seconds.
 * @returns {{is_late: boolean, lateness_seconds: number}}
 */
export function classifyLateness(eventTime, watermark, thresholdSeconds) {
  if (watermark === null) {
    return { is_late: false, lateness_seconds: 0 };
  }
  const latenessMs = watermark.getTime() - eventTime.getTime();
  const latenessSeconds = Math.max(latenessMs / 1000, 0);
  const isLate = latenessSeconds > thresholdSeconds;
  return { is_late: isLate, lateness_seconds: latenessSeconds };
}

/**
 * Validate a single sensor event against the required-field schema.
 *
 * Checks that all required fields are present and well-typed:
 * - sensor_id, type, unit, location, timestamp must be non-empty strings
 * - value must be a finite number (not null, undefined, NaN, or Infinity)
 * - timestamp must be parseable as an ISO date
 *
 * @param {SensorEvent} event - The event object to validate.
 * @returns {ValidationResult} The event with an accepted/rejected status.
 */
export function validateEvent(event) {
  // Check string fields
  const stringFields = ["sensor_id", "type", "unit", "location", "timestamp"];
  for (const field of stringFields) {
    const val = event[field];
    if (typeof val !== "string" || val.trim() === "") {
      return {
        event,
        status: "rejected",
        reason: `missing or empty field: ${field}`,
      };
    }
  }

  // Check value is a number (not null, undefined, NaN, etc.)
  if (event.value === null || event.value === undefined) {
    return { event, status: "rejected", reason: "null value" };
  }
  if (typeof event.value !== "number" || !Number.isFinite(event.value)) {
    return { event, status: "rejected", reason: "value is not a valid number" };
  }

  // Check timestamp is parseable
  if (!isValidTimestamp(event.timestamp)) {
    return { event, status: "rejected", reason: "unparseable timestamp" };
  }

  return { event, status: "accepted", reason: null };
}

/**
 * Compute a deduplication key for a sensor event.
 *
 * The key is formed as `"{sensor_id}::{timestamp}"`, which identifies
 * logically identical readings from the same sensor at the same instant.
 *
 * @param {SensorEvent} event - The event to derive a key from.
 * @returns {string} A composite deduplication key.
 */
export function deduplicateKey(event) {
  return `${event.sensor_id}::${event.timestamp}`;
}

/**
 * Compute aggregate statistics over a set of accepted sensor events.
 *
 * Returns breakdowns by sensor (with min/max/avg value), by event type
 * (count), and by location (count).
 *
 * @param {SensorEvent[]} events - Array of accepted events.
 * @returns {SensorAggregates} Aggregated statistics.
 */
export function computeAggregates(events) {
  /** @type {Object<string, {count: number, sum: number, min_value: number, max_value: number}>} */
  const bySensor = {};
  /** @type {Object<string, number>} */
  const byType = {};
  /** @type {Object<string, number>} */
  const byLocation = {};

  for (const evt of events) {
    // by_sensor
    if (!bySensor[evt.sensor_id]) {
      bySensor[evt.sensor_id] = {
        count: 0,
        sum: 0,
        min_value: evt.value,
        max_value: evt.value,
      };
    }
    const s = bySensor[evt.sensor_id];
    s.count += 1;
    s.sum += evt.value;
    if (evt.value < s.min_value) s.min_value = evt.value;
    if (evt.value > s.max_value) s.max_value = evt.value;

    // by_type
    byType[evt.type] = (byType[evt.type] || 0) + 1;

    // by_location
    byLocation[evt.location] = (byLocation[evt.location] || 0) + 1;
  }

  // Build the final by_sensor with avg_value rounded to 2 decimals
  /** @type {Object<string, {count: number, min_value: number, max_value: number, avg_value: number}>} */
  const bySensorFinal = {};
  for (const [sensorId, data] of Object.entries(bySensor)) {
    bySensorFinal[sensorId] = {
      count: data.count,
      min_value: data.min_value,
      max_value: data.max_value,
      avg_value: Math.round((data.sum / data.count) * 100) / 100,
    };
  }

  return {
    by_sensor: bySensorFinal,
    by_type: byType,
    by_location: byLocation,
  };
}

/**
 * Process a JSONL stream of sensor events end-to-end.
 *
 * Reads events line-by-line from the input file, validates them, deduplicates
 * (first occurrence wins), routes accepted events to `accepted.jsonl` and
 * rejected/duplicate events to `dead_letter.jsonl`, computes aggregates over
 * accepted events, and writes a summary to `summary.json`.
 *
 * @param {string} inputPath - Path to the JSONL input file.
 * @param {string} outputDir - Directory to write output files into.
 * @param {object} [options]
 * @param {string} [options.pipelineName="sensor_stream"] - Name for the pipeline run.
 * @returns {Promise<PipelineSummary>} The pipeline run summary.
 */
export async function processStream(inputPath, outputDir, { pipelineName = "sensor_stream", latenessThresholdSeconds = 0 } = {}) {
  const startTime = performance.now();
  const runAt = new Date().toISOString();

  console.info(`[${pipelineName}] Starting stream processing from ${inputPath}`);

  // Ensure output directory exists
  await mkdir(outputDir, { recursive: true });

  /** @type {SensorEvent[]} */
  const acceptedEvents = [];
  /** @type {Array<{event: *, status: string, reason: string}>} */
  const deadLetterEvents = [];
  /** @type {Object<string, number>} */
  const rejectionReasons = {};
  const seenKeys = new Set();

  let eventsSeen = 0;
  let eventsAccepted = 0;
  let eventsRejected = 0;
  let eventsDuplicate = 0;

  let status = "success";
  /** @type {Date|null} */
  let watermark = null;
  /** @type {SensorEvent[]} */
  const lateEvents = [];
  let maxLateness = 0;

  try {
    const raw = await readFile(inputPath, "utf-8");
    const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);

    for (const line of lines) {
      eventsSeen += 1;

      // Parse JSON
      let event;
      try {
        event = JSON.parse(line);
      } catch {
        const reason = "malformed JSON";
        eventsRejected += 1;
        rejectionReasons[reason] = (rejectionReasons[reason] || 0) + 1;
        deadLetterEvents.push({ event: line, status: "rejected", reason });
        console.warn(`[${pipelineName}] Rejected line ${eventsSeen}: ${reason}`);
        continue;
      }

      // Validate
      let result;
      try {
        result = validateEvent(event);
      } catch (err) {
        const reason = `validation error: ${err.message}`;
        eventsRejected += 1;
        rejectionReasons[reason] = (rejectionReasons[reason] || 0) + 1;
        deadLetterEvents.push({ event, status: "rejected", reason });
        console.warn(`[${pipelineName}] Rejected line ${eventsSeen}: ${reason}`);
        continue;
      }

      if (result.status === "rejected") {
        eventsRejected += 1;
        rejectionReasons[result.reason] = (rejectionReasons[result.reason] || 0) + 1;
        deadLetterEvents.push({ event: result.event, status: "rejected", reason: result.reason });
        console.warn(`[${pipelineName}] Rejected line ${eventsSeen}: ${result.reason}`);
        continue;
      }

      // Deduplicate
      const key = deduplicateKey(event);
      if (seenKeys.has(key)) {
        const reason = "duplicate event";
        eventsDuplicate += 1;
        rejectionReasons[reason] = (rejectionReasons[reason] || 0) + 1;
        deadLetterEvents.push({ event, status: "duplicate", reason });
        console.warn(`[${pipelineName}] Duplicate line ${eventsSeen}: ${key}`);
        continue;
      }
      seenKeys.add(key);

      // Accept
      eventsAccepted += 1;
      acceptedEvents.push(event);

      // Lateness check
      const eventTime = parseEventTime(event.timestamp);
      const { is_late: isLate, lateness_seconds: lateness } = classifyLateness(
        eventTime, watermark, latenessThresholdSeconds,
      );
      if (isLate) {
        lateEvents.push(event);
        if (lateness > maxLateness) maxLateness = lateness;
      }
      // Advance watermark
      if (watermark === null || eventTime > watermark) {
        watermark = eventTime;
      }
    }
  } catch (err) {
    status = "failed";
    console.error(`[${pipelineName}] Fatal error: ${err.message}`);
  }

  // Write accepted events
  const acceptedPath = join(outputDir, "accepted.jsonl");
  const acceptedContent = acceptedEvents.map((e) => JSON.stringify(e)).join("\n");
  await writeFile(acceptedPath, acceptedContent.length > 0 ? acceptedContent + "\n" : "", "utf-8");

  // Write dead-letter events
  const deadLetterPath = join(outputDir, "dead_letter.jsonl");
  const deadLetterContent = deadLetterEvents.map((e) => JSON.stringify(e)).join("\n");
  await writeFile(deadLetterPath, deadLetterContent.length > 0 ? deadLetterContent + "\n" : "", "utf-8");

  // Write late events
  const lateEventsPath = join(outputDir, "late_events.jsonl");
  const lateEventsContent = lateEvents.map((e) => JSON.stringify(e)).join("\n");
  await writeFile(lateEventsPath, lateEventsContent.length > 0 ? lateEventsContent + "\n" : "", "utf-8");

  // Compute aggregates
  const aggregates = computeAggregates(acceptedEvents);

  const endTime = performance.now();
  const durationSeconds = Math.round(((endTime - startTime) / 1000) * 1000) / 1000;

  /** @type {PipelineSummary} */
  const summary = {
    pipeline_name: pipelineName,
    run_at: runAt,
    duration_seconds: durationSeconds,
    status,
    events_seen: eventsSeen,
    events_accepted: eventsAccepted,
    events_rejected: eventsRejected,
    events_duplicate: eventsDuplicate,
    dead_letter_count: eventsRejected + eventsDuplicate,
    events_late: lateEvents.length,
    max_lateness_seconds: Math.round(maxLateness * 100) / 100,
    watermark: watermark ? watermark.toISOString() : "",
    lateness_threshold_seconds: latenessThresholdSeconds,
    aggregates,
    rejection_reasons: rejectionReasons,
  };

  // Write summary
  const summaryPath = join(outputDir, "summary.json");
  await writeFile(summaryPath, JSON.stringify(summary, null, 2) + "\n", "utf-8");

  console.info(
    `[${pipelineName}] Complete: ${eventsAccepted} accepted (${lateEvents.length} late), ` +
    `${eventsRejected} rejected, ${eventsDuplicate} duplicate ` +
    `(${durationSeconds}s)`,
  );

  return summary;
}
