import { readdir, readFile, writeFile, mkdir } from "node:fs/promises";
import { join, dirname } from "node:path";

/**
 * Load checkpoint from file, or return empty checkpoint if not found.
 * @param {string} checkpointPath
 * @param {string} pipelineName
 * @returns {Promise<{pipeline_name: string, last_run_at: string|null, processed_ids: string[], total_runs: number}>}
 */
export async function loadCheckpoint(checkpointPath, pipelineName) {
  try {
    const content = await readFile(checkpointPath, "utf-8");
    return JSON.parse(content);
  } catch (err) {
    if (err.code === "ENOENT") {
      return {
        pipeline_name: pipelineName,
        last_run_at: null,
        processed_ids: [],
        total_runs: 0,
      };
    }
    throw err;
  }
}

/**
 * Write checkpoint to JSON file. Creates parent directories if needed.
 * @param {string} checkpointPath
 * @param {object} checkpoint
 * @returns {Promise<void>}
 */
export async function saveCheckpoint(checkpointPath, checkpoint) {
  await mkdir(dirname(checkpointPath), { recursive: true });
  await writeFile(checkpointPath, JSON.stringify(checkpoint, null, 2), "utf-8");
}

/**
 * Read all JSONL files from inputDir, return list of parsed events.
 * Each line is a JSON object. Skip blank lines. Sort files alphabetically.
 * @param {string} inputDir
 * @returns {Promise<object[]>}
 */
export async function readEvents(inputDir) {
  let entries;
  try {
    entries = await readdir(inputDir);
  } catch {
    return [];
  }

  const jsonlFiles = entries
    .filter((f) => f.toLowerCase().endsWith(".jsonl"))
    .sort();

  const events = [];

  for (const fileName of jsonlFiles) {
    const filePath = join(inputDir, fileName);
    const content = await readFile(filePath, "utf-8");
    const lines = content.split(/\r?\n/);

    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.length === 0) continue;
      events.push(JSON.parse(trimmed));
    }
  }

  return events;
}

/**
 * Transform a single event. Returns enriched event or null if missing required fields.
 * Required fields: event_id, timestamp, type.
 * Enrichment: event_date, hour, is_purchase, has_user, processed_at.
 * @param {object} event
 * @returns {object|null}
 */
export function transformEvent(event) {
  if (!event.event_id || !event.timestamp || !event.type) {
    return null;
  }

  const date = new Date(event.timestamp);

  return {
    ...event,
    event_date: date.toISOString().slice(0, 10),
    hour: date.getUTCHours(),
    is_purchase: event.type === "checkout",
    has_user: event.user_id != null && event.user_id !== "",
    processed_at: new Date().toISOString(),
  };
}

/**
 * Format a Date as YYYYMMDD_HHMMSS_mmm for output filenames.
 * Includes milliseconds to avoid collisions on fast successive runs.
 * @param {Date} date
 * @returns {string}
 */
function formatRunTimestamp(date) {
  const yyyy = String(date.getUTCFullYear());
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const min = String(date.getUTCMinutes()).padStart(2, "0");
  const ss = String(date.getUTCSeconds()).padStart(2, "0");
  const ms = String(date.getUTCMilliseconds()).padStart(3, "0");
  return `${yyyy}${mm}${dd}_${hh}${min}${ss}_${ms}`;
}

/**
 * Run the full incremental ETL pipeline.
 * @param {object} options
 * @param {string} options.inputDir
 * @param {string} options.outputDir
 * @param {string} options.checkpointPath
 * @param {string} [options.pipelineName="events_etl"]
 * @param {Function} [options._saveCheckpointFn=saveCheckpoint] - Internal, for testing.
 * @returns {Promise<{pipeline_name: string, run_at: string, records_seen: number, records_skipped: number, records_processed: number, records_failed: number, checkpoint_updated: boolean}>}
 */
export async function runIncrementalEtl({
  inputDir,
  outputDir,
  checkpointPath,
  pipelineName = "events_etl",
  _saveCheckpointFn = saveCheckpoint,
}) {
  const runAt = new Date();
  const runAtIso = runAt.toISOString();

  // 1. Load checkpoint
  const checkpoint = await loadCheckpoint(checkpointPath, pipelineName);

  // 2. Read all events
  const allEvents = await readEvents(inputDir);

  // 3. Build a Set from checkpoint.processed_ids for O(1) lookup
  const processedSet = new Set(checkpoint.processed_ids);

  // 4. Filter to new events (deduplicate within the batch as well)
  const seenInBatch = new Set();
  const newEvents = [];

  for (const event of allEvents) {
    if (!event.event_id) continue;
    if (processedSet.has(event.event_id)) continue;
    if (seenInBatch.has(event.event_id)) continue;
    seenInBatch.add(event.event_id);
    newEvents.push(event);
  }

  const recordsSeen = allEvents.length;
  const recordsSkipped = recordsSeen - newEvents.length;

  // 5. Transform new events, collecting successes and counting failures
  const transformed = [];
  let recordsFailed = 0;

  for (const event of newEvents) {
    const result = transformEvent(event);
    if (result !== null) {
      transformed.push(result);
    } else {
      recordsFailed++;
    }
  }

  const recordsProcessed = transformed.length;

  // 6. If there are processed events, write output JSONL file
  if (transformed.length > 0) {
    await mkdir(outputDir, { recursive: true });
    const outputFileName = `${formatRunTimestamp(runAt)}.jsonl`;
    const outputPath = join(outputDir, outputFileName);
    const content =
      transformed.map((e) => JSON.stringify(e)).join("\n") + "\n";
    await writeFile(outputPath, content, "utf-8");
  }

  // 7. Update checkpoint only if new events were processed
  let checkpointUpdated = false;

  if (transformed.length > 0) {
    const newIds = transformed.map((e) => e.event_id);
    const updatedCheckpoint = {
      pipeline_name: pipelineName,
      last_run_at: runAtIso,
      processed_ids: [...checkpoint.processed_ids, ...newIds],
      total_runs: checkpoint.total_runs + 1,
    };
    await _saveCheckpointFn(checkpointPath, updatedCheckpoint);
    checkpointUpdated = true;
  }

  // 8. Return run summary
  return {
    pipeline_name: pipelineName,
    run_at: runAtIso,
    records_seen: recordsSeen,
    records_skipped: recordsSkipped,
    records_processed: recordsProcessed,
    records_failed: recordsFailed,
    checkpoint_updated: checkpointUpdated,
  };
}
