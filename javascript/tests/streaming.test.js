import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFile, writeFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

import {
  validateEvent,
  deduplicateKey,
  computeAggregates,
  processStream,
} from "../src/streaming/processor.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SAMPLE_DIR = join(__dirname, "..", "..", "data", "sample");

function makeEvent(overrides = {}) {
  return {
    sensor_id: "sensor-01",
    type: "temperature",
    value: 22.5,
    unit: "celsius",
    location: "warehouse-A",
    timestamp: "2024-06-01T08:00:00Z",
    ...overrides,
  };
}

async function writeJsonl(filePath, events) {
  const content =
    events.map((e) => (typeof e === "string" ? e : JSON.stringify(e))).join("\n") + "\n";
  await writeFile(filePath, content, "utf-8");
}

// ---------------------------------------------------------------------------
// validateEvent
// ---------------------------------------------------------------------------
describe("validateEvent", () => {
  it("valid event returns accepted with null reason", () => {
    const result = validateEvent(makeEvent());
    assert.equal(result.status, "accepted");
    assert.equal(result.reason, null);
  });

  it("missing field returns rejected with reason containing missing or empty", () => {
    const result = validateEvent(makeEvent({ sensor_id: undefined }));
    assert.equal(result.status, "rejected");
    assert.ok(/missing|empty/.test(result.reason));
  });

  it("null value returns rejected with reason null value", () => {
    const result = validateEvent(makeEvent({ value: null }));
    assert.equal(result.status, "rejected");
    assert.equal(result.reason, "null value");
  });

  it("empty string field returns rejected", () => {
    const result = validateEvent(makeEvent({ unit: "" }));
    assert.equal(result.status, "rejected");
  });

  it("non-numeric value string returns rejected with reason containing number", () => {
    const result = validateEvent(makeEvent({ value: "hello" }));
    assert.equal(result.status, "rejected");
    assert.ok(/number/.test(result.reason));
  });

  it("unparseable timestamp returns rejected with reason unparseable timestamp", () => {
    const result = validateEvent(makeEvent({ timestamp: "not-a-date" }));
    assert.equal(result.status, "rejected");
    assert.equal(result.reason, "unparseable timestamp");
  });

  it("boolean value true is rejected as not a valid number", () => {
    const result = validateEvent(makeEvent({ value: true }));
    assert.equal(result.status, "rejected");
    assert.ok(/number/.test(result.reason));
  });
});

// ---------------------------------------------------------------------------
// deduplicateKey
// ---------------------------------------------------------------------------
describe("deduplicateKey", () => {
  it("returns correct format sensor_id::timestamp", () => {
    const key = deduplicateKey(makeEvent());
    assert.equal(key, "sensor-01::2024-06-01T08:00:00Z");
  });

  it("different sensors produce different keys", () => {
    const key1 = deduplicateKey(makeEvent({ sensor_id: "sensor-01" }));
    const key2 = deduplicateKey(makeEvent({ sensor_id: "sensor-02" }));
    assert.notEqual(key1, key2);
  });
});

// ---------------------------------------------------------------------------
// computeAggregates
// ---------------------------------------------------------------------------
describe("computeAggregates", () => {
  it("single sensor with 3 readings computes correct count, min, max, avg", () => {
    const events = [
      makeEvent({ value: 10 }),
      makeEvent({ value: 20 }),
      makeEvent({ value: 30 }),
    ];
    const agg = computeAggregates(events);
    const sensor = agg.by_sensor["sensor-01"];
    assert.equal(sensor.count, 3);
    assert.equal(sensor.min_value, 10);
    assert.equal(sensor.max_value, 30);
    assert.equal(sensor.avg_value, 20.0);
  });

  it("multiple sensors produce correct by_sensor, by_type, by_location", () => {
    const events = [
      makeEvent({ sensor_id: "s1", type: "temperature", location: "locA", value: 10 }),
      makeEvent({ sensor_id: "s1", type: "temperature", location: "locA", value: 20 }),
      makeEvent({ sensor_id: "s2", type: "humidity", location: "locB", value: 50 }),
    ];
    const agg = computeAggregates(events);

    assert.equal(Object.keys(agg.by_sensor).length, 2);
    assert.equal(agg.by_sensor["s1"].count, 2);
    assert.equal(agg.by_sensor["s2"].count, 1);

    assert.equal(agg.by_type["temperature"], 2);
    assert.equal(agg.by_type["humidity"], 1);

    assert.equal(agg.by_location["locA"], 2);
    assert.equal(agg.by_location["locB"], 1);
  });

  it("empty array returns empty objects", () => {
    const agg = computeAggregates([]);
    assert.deepEqual(agg.by_sensor, {});
    assert.deepEqual(agg.by_type, {});
    assert.deepEqual(agg.by_location, {});
  });
});

// ---------------------------------------------------------------------------
// processStream (end-to-end)
// ---------------------------------------------------------------------------
describe("processStream", () => {
  let outDir;

  beforeEach(async () => {
    outDir = await mkdtemp(join(tmpdir(), "stream-test-"));
  });

  afterEach(async () => {
    await rm(outDir, { recursive: true, force: true });
  });

  it("sample data produces expected counts and output files", async () => {
    const inputPath = join(SAMPLE_DIR, "sensor_events.json");
    const result = await processStream(inputPath, outDir);

    assert.equal(result.events_seen, 16);
    assert.equal(result.events_accepted, 14);
    assert.equal(result.events_rejected, 1);
    assert.equal(result.events_duplicate, 1);
    assert.equal(result.dead_letter_count, 2);
    assert.equal(result.status, "success");

    // accepted.jsonl has 14 lines
    const acceptedRaw = await readFile(join(outDir, "accepted.jsonl"), "utf-8");
    const acceptedLines = acceptedRaw.split("\n").filter((l) => l.length > 0);
    assert.equal(acceptedLines.length, 14);

    // dead_letter.jsonl has 2 lines
    const dlRaw = await readFile(join(outDir, "dead_letter.jsonl"), "utf-8");
    const dlLines = dlRaw.split("\n").filter((l) => l.length > 0);
    assert.equal(dlLines.length, 2);

    // summary.json exists, is valid JSON, and has all required keys
    const summaryRaw = await readFile(join(outDir, "summary.json"), "utf-8");
    const summary = JSON.parse(summaryRaw);
    const requiredKeys = [
      "pipeline_name",
      "run_at",
      "duration_seconds",
      "status",
      "events_seen",
      "events_accepted",
      "events_rejected",
      "events_duplicate",
      "dead_letter_count",
      "aggregates",
      "rejection_reasons",
    ];
    for (const key of requiredKeys) {
      assert.ok(key in summary, `summary.json missing key: ${key}`);
    }
  });

  it("all valid events are accepted with zero rejects and duplicates", async () => {
    const inputPath = join(outDir, "input.jsonl");
    await writeJsonl(inputPath, [
      makeEvent({ sensor_id: "s1", timestamp: "2024-06-01T08:00:00Z" }),
      makeEvent({ sensor_id: "s2", timestamp: "2024-06-01T08:00:00Z" }),
      makeEvent({ sensor_id: "s3", timestamp: "2024-06-01T08:00:00Z" }),
    ]);

    const result = await processStream(inputPath, outDir);
    assert.equal(result.events_accepted, 3);
    assert.equal(result.events_rejected, 0);
    assert.equal(result.events_duplicate, 0);
  });

  it("duplicate events are detected", async () => {
    const inputPath = join(outDir, "input.jsonl");
    const evt = makeEvent();
    await writeJsonl(inputPath, [evt, evt]);

    const result = await processStream(inputPath, outDir);
    assert.equal(result.events_accepted, 1);
    assert.equal(result.events_duplicate, 1);
  });

  it("malformed JSON lines are rejected", async () => {
    const inputPath = join(outDir, "input.jsonl");
    await writeJsonl(inputPath, ["not json", makeEvent({ sensor_id: "s1" })]);

    const result = await processStream(inputPath, outDir);
    assert.equal(result.events_rejected, 1);
    assert.equal(result.events_accepted, 1);
  });

  it("empty file produces zero counts", async () => {
    const inputPath = join(outDir, "input.jsonl");
    await writeFile(inputPath, "", "utf-8");

    const result = await processStream(inputPath, outDir);
    assert.equal(result.events_seen, 0);
    assert.equal(result.events_accepted, 0);
  });

  it("dead letter contains correct status and reason for rejected events", async () => {
    const inputPath = join(outDir, "input.jsonl");
    await writeJsonl(inputPath, [
      makeEvent({ value: null }),
      makeEvent({ sensor_id: "s2" }),
    ]);

    await processStream(inputPath, outDir);

    const dlRaw = await readFile(join(outDir, "dead_letter.jsonl"), "utf-8");
    const firstEntry = JSON.parse(dlRaw.split("\n").filter((l) => l.length > 0)[0]);
    assert.equal(firstEntry.status, "rejected");
    assert.ok(firstEntry.reason, "dead letter entry should have a reason");
  });

  it("summary.json has all required keys", async () => {
    const inputPath = join(SAMPLE_DIR, "sensor_events.json");
    await processStream(inputPath, outDir);

    const summaryRaw = await readFile(join(outDir, "summary.json"), "utf-8");
    const summary = JSON.parse(summaryRaw);

    const requiredKeys = [
      "pipeline_name",
      "run_at",
      "duration_seconds",
      "status",
      "events_seen",
      "events_accepted",
      "events_rejected",
      "events_duplicate",
      "dead_letter_count",
      "aggregates",
      "rejection_reasons",
    ];
    for (const key of requiredKeys) {
      assert.ok(key in summary, `summary.json missing key: ${key}`);
    }
  });

  it("rerun to the same output dir is idempotent", async () => {
    const inputPath = join(outDir, "input.jsonl");
    await writeJsonl(inputPath, [
      makeEvent({ sensor_id: "s1", timestamp: "2024-06-01T08:00:00Z" }),
      makeEvent({ sensor_id: "s2", timestamp: "2024-06-01T08:01:00Z" }),
    ]);

    await processStream(inputPath, outDir);
    await processStream(inputPath, outDir);

    const acceptedRaw = await readFile(join(outDir, "accepted.jsonl"), "utf-8");
    const acceptedLines = acceptedRaw.split("\n").filter((l) => l.length > 0);
    assert.equal(acceptedLines.length, 2, "rerun should overwrite, not append");
  });
});
