#!/usr/bin/env node

import { parseArgs } from "node:util";
import { resolve } from "node:path";
import { processStream } from "./processor.js";
import { loadConfig, validateConfig } from "../config.js";

const { values } = parseArgs({
  options: {
    input: { type: "string", short: "i" },
    "output-dir": { type: "string", short: "o" },
    "pipeline-name": { type: "string", short: "n" },
    "lateness-threshold": { type: "string", short: "l" },
    config: { type: "string", short: "c" },
    help: { type: "boolean", short: "h" },
  },
  strict: true,
});

if (values.help) {
  console.log(
    `Usage: node cli.js --input <path> --output-dir <path> [options]

Options:
  -i, --input              Path to the JSONL input file
  -o, --output-dir         Directory for output files
  -n, --pipeline-name      Name for this pipeline run (default: sensor_stream)
  -l, --lateness-threshold Allowed lateness in seconds (default: 0)
  -c, --config             Path to a JSON config file
  -h, --help               Show this help message`,
  );
  process.exit(0);
}

// Load config if provided
let configData = {};
if (values.config) {
  try {
    configData = loadConfig(values.config);
  } catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  }
  const errors = validateConfig(configData, {
    known: ["input", "output_dir", "pipeline_name", "lateness_threshold"],
  });
  if (errors.length > 0) {
    for (const e of errors) console.error(`Config error: ${e}`);
    process.exit(1);
  }
}

// Merge: defaults < config < CLI flags
const inputPath = values.input || configData.input;
const outputDir = values["output-dir"] || configData.output_dir;
const pipelineName = values["pipeline-name"] || configData.pipeline_name || "sensor_stream";
const latenessThresholdSeconds = values["lateness-threshold"]
  ? parseFloat(values["lateness-threshold"])
  : configData.lateness_threshold ?? 0;

if (!inputPath) {
  console.error("Error: --input is required (provide via CLI or config file)");
  process.exit(1);
}
if (!outputDir) {
  console.error("Error: --output-dir is required (provide via CLI or config file)");
  process.exit(1);
}

const summary = await processStream(resolve(inputPath), resolve(outputDir), {
  pipelineName,
  latenessThresholdSeconds,
});

console.log("\n=== Stream Processing Summary ===");
console.log(`Pipeline        : ${summary.pipeline_name}`);
console.log(`Status          : ${summary.status}`);
console.log(`Events seen     : ${summary.events_seen}`);
console.log(`Events accepted : ${summary.events_accepted}`);
console.log(`Events rejected : ${summary.events_rejected}`);
console.log(`Events duplicate: ${summary.events_duplicate}`);
console.log(`Events late     : ${summary.events_late}`);
console.log(`Dead letter     : ${summary.dead_letter_count}`);
console.log(`Duration        : ${summary.duration_seconds}s`);
if (summary.events_late > 0) {
  console.log(`Max lateness    : ${summary.max_lateness_seconds}s`);
  console.log(`Watermark       : ${summary.watermark}`);
}

if (Object.keys(summary.rejection_reasons).length > 0) {
  console.log("\nRejection reasons:");
  for (const [reason, count] of Object.entries(summary.rejection_reasons)) {
    console.log(`  ${reason}: ${count}`);
  }
}

const sensorCount = Object.keys(summary.aggregates.by_sensor || {}).length;
if (sensorCount > 0) {
  console.log(`\nSensors tracked : ${sensorCount}`);
}

console.log(`\nFull summary written to ${resolve(outputDir)}/summary.json`);
