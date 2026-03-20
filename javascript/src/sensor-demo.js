/**
 * Sensor pipeline demo — orchestrated 5-step workflow.
 *
 * Processes data/sample/sensor_events.json through ingest, validate,
 * deduplicate, aggregate, and output steps with observability,
 * dead-letter routing, hourly aggregation, and a manifest.
 *
 * Run from the javascript/ directory:
 *   node src/sensor-demo.js
 *
 * Or with custom paths:
 *   node src/sensor-demo.js --data-dir ../data/sample --output-dir ../data/silver/sensor_demo
 */

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { Pipeline, formatResult } from "./orchestration/runner.js";
import { writeManifest, generateRunId } from "./manifest.js";

// ---------------------------------------------------------------------------
// Step functions
// ---------------------------------------------------------------------------

/**
 * Step 1: Ingest — read sensor_events.json (JSONL), parse each line.
 */
async function ingest(ctx) {
  const filePath = join(ctx.dataDir, "sensor_events.json");
  const content = await readFile(filePath, "utf-8");
  const lines = content
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0);

  const events = lines.map((line, idx) => {
    try {
      return JSON.parse(line);
    } catch {
      return { _parse_error: true, _line: idx + 1, _raw: line };
    }
  });

  ctx.raw_events = events;
  return { events_read: events.length };
}

/**
 * Step 2: Validate — check required fields, numeric value, parseable timestamp.
 * Split into accepted / rejected (dead-letter).
 */
function validate(ctx) {
  const required = ["sensor_id", "type", "value", "unit", "location", "timestamp"];
  const accepted = [];
  const rejected = [];

  for (const event of ctx.raw_events) {
    const reasons = [];

    // Parse errors from ingest
    if (event._parse_error) {
      reasons.push(`JSON parse error on line ${event._line}`);
      rejected.push({ event, reasons });
      continue;
    }

    // Required fields
    for (const field of required) {
      if (!(field in event) || event[field] === undefined) {
        reasons.push(`missing field: ${field}`);
      }
    }

    // Numeric value (null counts as invalid)
    if (event.value === null || event.value === undefined || typeof event.value !== "number" || !Number.isFinite(event.value)) {
      reasons.push("value is not a finite number");
    }

    // Parseable timestamp
    if (event.timestamp) {
      const ts = Date.parse(event.timestamp);
      if (Number.isNaN(ts)) {
        reasons.push("timestamp is not parseable");
      }
    }

    if (reasons.length > 0) {
      rejected.push({ event, reasons });
    } else {
      accepted.push(event);
    }
  }

  ctx.accepted = accepted;
  ctx.rejected = rejected;

  return {
    accepted: accepted.length,
    rejected: rejected.length,
    rejection_reasons: rejected.map((r) => ({
      sensor_id: r.event.sensor_id ?? "unknown",
      reasons: r.reasons,
    })),
  };
}

/**
 * Step 3: Deduplicate — by sensor_id::timestamp key. First occurrence wins.
 */
function deduplicate(ctx) {
  const seen = new Set();
  const unique = [];
  let duplicateCount = 0;

  for (const event of ctx.accepted) {
    const key = `${event.sensor_id}::${event.timestamp}`;
    if (seen.has(key)) {
      duplicateCount++;
      continue;
    }
    seen.add(key);
    unique.push(event);
  }

  ctx.accepted = unique;

  return {
    before: unique.length + duplicateCount,
    after: unique.length,
    duplicates_removed: duplicateCount,
  };
}

/**
 * Step 4: Aggregate — hourly aggregates (sensor_id + hour bucket)
 * and per-location summary.
 */
function aggregate(ctx) {
  // Hourly aggregates: group by sensor_id + hour bucket
  /** @type {Map<string, {sensor_id: string, hour: string, type: string, unit: string, location: string, values: number[]}>} */
  const hourlyMap = new Map();

  for (const event of ctx.accepted) {
    const dt = new Date(event.timestamp);
    const hour = `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}T${String(dt.getUTCHours()).padStart(2, "0")}:00:00Z`;
    const key = `${event.sensor_id}::${hour}`;

    if (!hourlyMap.has(key)) {
      hourlyMap.set(key, {
        sensor_id: event.sensor_id,
        hour,
        type: event.type,
        unit: event.unit,
        location: event.location,
        values: [],
      });
    }
    hourlyMap.get(key).values.push(event.value);
  }

  const hourlyAggregates = [];
  for (const bucket of hourlyMap.values()) {
    const vals = bucket.values;
    const sum = vals.reduce((a, b) => a + b, 0);
    hourlyAggregates.push({
      sensor_id: bucket.sensor_id,
      hour: bucket.hour,
      type: bucket.type,
      unit: bucket.unit,
      location: bucket.location,
      count: vals.length,
      min: Math.min(...vals),
      max: Math.max(...vals),
      avg: Math.round((sum / vals.length) * 100) / 100,
      sum: Math.round(sum * 100) / 100,
    });
  }

  // Per-location summary
  /** @type {Map<string, {location: string, event_count: number, sensors: Set<string>, types: Set<string>}>} */
  const locationMap = new Map();

  for (const event of ctx.accepted) {
    if (!locationMap.has(event.location)) {
      locationMap.set(event.location, {
        location: event.location,
        event_count: 0,
        sensors: new Set(),
        types: new Set(),
      });
    }
    const loc = locationMap.get(event.location);
    loc.event_count++;
    loc.sensors.add(event.sensor_id);
    loc.types.add(event.type);
  }

  const locationSummary = [];
  for (const loc of locationMap.values()) {
    locationSummary.push({
      location: loc.location,
      event_count: loc.event_count,
      sensor_count: loc.sensors.size,
      sensors: [...loc.sensors].sort().join(";"),
      type_count: loc.types.size,
      types: [...loc.types].sort().join(";"),
    });
  }

  ctx.hourly_aggregates = hourlyAggregates;
  ctx.location_summary = locationSummary;

  return {
    hourly_buckets: hourlyAggregates.length,
    locations: locationSummary.length,
    sensors: new Set(ctx.accepted.map((e) => e.sensor_id)).size,
  };
}

/**
 * Step 5: Output — write accepted.jsonl, dead_letter.jsonl,
 * hourly_aggregates.csv, location_summary.csv, summary.json.
 */
async function output(ctx) {
  const outDir = ctx.outputDir;
  await mkdir(outDir, { recursive: true });

  const files = [];

  // accepted.jsonl
  const acceptedPath = join(outDir, "accepted.jsonl");
  const acceptedLines = ctx.accepted.map((e) => JSON.stringify(e));
  await writeFile(acceptedPath, acceptedLines.join("\n") + "\n", "utf-8");
  files.push(acceptedPath);

  // dead_letter.jsonl
  const deadLetterPath = join(outDir, "dead_letter.jsonl");
  const deadLetterLines = ctx.rejected.map((r) =>
    JSON.stringify({ event: r.event, reasons: r.reasons }),
  );
  const deadLetterContent =
    deadLetterLines.length > 0 ? deadLetterLines.join("\n") + "\n" : "";
  await writeFile(deadLetterPath, deadLetterContent, "utf-8");
  files.push(deadLetterPath);

  // hourly_aggregates.csv
  const aggPath = join(outDir, "hourly_aggregates.csv");
  const aggHeaders = [
    "sensor_id",
    "hour",
    "type",
    "unit",
    "location",
    "count",
    "min",
    "max",
    "avg",
    "sum",
  ];
  const aggRows = ctx.hourly_aggregates.map((row) =>
    aggHeaders.map((h) => row[h]).join(","),
  );
  await writeFile(
    aggPath,
    aggHeaders.join(",") + "\n" + aggRows.join("\n") + "\n",
    "utf-8",
  );
  files.push(aggPath);

  // location_summary.csv
  const locPath = join(outDir, "location_summary.csv");
  const locHeaders = [
    "location",
    "event_count",
    "sensor_count",
    "sensors",
    "type_count",
    "types",
  ];
  const locRows = ctx.location_summary.map((row) =>
    locHeaders.map((h) => row[h]).join(","),
  );
  await writeFile(
    locPath,
    locHeaders.join(",") + "\n" + locRows.join("\n") + "\n",
    "utf-8",
  );
  files.push(locPath);

  // summary.json
  const summaryPath = join(outDir, "summary.json");
  const summary = {
    total_events_read: ctx.raw_events.length,
    accepted: ctx.accepted.length,
    rejected: ctx.rejected.length,
    duplicates_removed: ctx.step_results.deduplicate.duplicates_removed,
    hourly_buckets: ctx.hourly_aggregates.length,
    locations: ctx.location_summary.length,
    sensors: [...new Set(ctx.accepted.map((e) => e.sensor_id))].sort(),
  };
  await writeFile(summaryPath, JSON.stringify(summary, null, 2) + "\n", "utf-8");
  files.push(summaryPath);

  ctx.output_files = files;

  return {
    files_written: files.length,
    output_dir: outDir,
  };
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

/**
 * Run the sensor demo pipeline end-to-end.
 *
 * @param {object} [options]
 * @param {string} [options.dataDir="data/sample"]
 * @param {string} [options.outputDir="data/silver/sensor_demo"]
 * @param {string} [options.manifestDir="data/manifests"]
 * @returns {Promise<{ pipeline_result: object, output_dir: string, manifest_path: string }>}
 */
export async function runSensorDemo({
  dataDir = "data/sample",
  outputDir = "data/silver/sensor_demo",
  manifestDir = "data/manifests",
} = {}) {
  const pipeline = new Pipeline("sensor_demo");

  pipeline
    .addStep("ingest", ingest)
    .addStep("validate", validate)
    .addStep("deduplicate", deduplicate)
    .addStep("aggregate", aggregate)
    .addStep("output", output);

  const context = { dataDir, outputDir };
  const result = await pipeline.run(context);

  // Write manifest after pipeline completes
  const runId = generateRunId();
  const manifestPath = writeManifest({
    pipeline_name: "sensor_demo",
    run_id: runId,
    source: join(dataDir, "sensor_events.json"),
    output: context.output_files ?? [],
    row_count: context.accepted ? context.accepted.length : 0,
    status: result.status,
    warnings: result.steps
      .filter((s) => s.status !== "success")
      .map((s) => `${s.name}: ${s.error}`),
    extras: {
      total_events_read: context.raw_events ? context.raw_events.length : 0,
      rejected_count: context.rejected ? context.rejected.length : 0,
      duplicates_removed: result.steps.find((s) => s.name === "deduplicate")
        ?.result?.duplicates_removed ?? 0,
    },
    manifestDir,
  });

  return {
    pipeline_result: result,
    output_dir: outputDir,
    manifest_path: manifestPath,
  };
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

async function main() {
  const { values } = parseArgs({
    options: {
      "data-dir": { type: "string", default: "../data/sample" },
      "output-dir": { type: "string", default: "../data/silver/sensor_demo" },
      "manifest-dir": { type: "string", default: "../data/manifests" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });

  if (values.help) {
    console.log(
      `Usage: node src/sensor-demo.js [options]

Options:
  --data-dir <path>      Directory with sensor_events.json (default: ../data/sample)
  --output-dir <path>    Directory for pipeline output (default: ../data/silver/sensor_demo)
  --manifest-dir <path>  Directory for run manifest (default: ../data/manifests)
  -h, --help             Show this help message`,
    );
    process.exit(0);
  }

  const result = await runSensorDemo({
    dataDir: values["data-dir"],
    outputDir: values["output-dir"],
    manifestDir: values["manifest-dir"],
  });

  console.log();
  console.log(formatResult(result.pipeline_result));
  console.log();
  console.log(`Output directory: ${result.output_dir}`);
  console.log(`Manifest: ${result.manifest_path}`);
}

// Only run CLI when executed directly (not when imported)
const isMain = process.argv[1] && new URL(process.argv[1], "file://").pathname
  === new URL(import.meta.url).pathname;

if (isMain) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
