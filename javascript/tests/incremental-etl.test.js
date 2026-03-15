import { describe, test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  rmSync,
  readFileSync,
  readdirSync,
  existsSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  loadCheckpoint,
  saveCheckpoint,
  readEvents,
  transformEvent,
  runIncrementalEtl,
} from "../src/transform/incremental-etl.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTempDir() {
  return mkdtempSync(join(tmpdir(), "incremental-etl-test-"));
}

function writeJsonl(filePath, events) {
  const content = events.map((e) => JSON.stringify(e)).join("\n") + "\n";
  writeFileSync(filePath, content, "utf-8");
}

const SAMPLE_EVENTS = [
  {
    event_id: "evt-001",
    type: "page_view",
    user_id: "U001",
    page: "/home",
    timestamp: "2024-06-01T10:00:00Z",
  },
  {
    event_id: "evt-002",
    type: "checkout",
    user_id: "U002",
    order_id: "ORD-001",
    timestamp: "2024-06-01T11:30:00Z",
  },
  {
    event_id: "evt-003",
    type: "add_to_cart",
    user_id: null,
    product_id: "P001",
    timestamp: "2024-06-01T12:00:00Z",
  },
];

// ---------------------------------------------------------------------------
// loadCheckpoint
// ---------------------------------------------------------------------------

describe("loadCheckpoint", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("returns default when no file", async () => {
    const cp = await loadCheckpoint(
      join(tempDir, "nonexistent.json"),
      "events_etl",
    );

    assert.equal(cp.pipeline_name, "events_etl");
    assert.equal(cp.last_run_at, null);
    assert.deepEqual(cp.processed_ids, []);
    assert.equal(cp.total_runs, 0);
  });

  test("saveCheckpoint and loadCheckpoint round-trip", async () => {
    const cpPath = join(tempDir, "sub", "checkpoint.json");
    const checkpoint = {
      pipeline_name: "events_etl",
      last_run_at: "2024-06-01T10:00:00.000Z",
      processed_ids: ["evt-001", "evt-002"],
      total_runs: 3,
    };

    await saveCheckpoint(cpPath, checkpoint);
    const loaded = await loadCheckpoint(cpPath, "events_etl");

    assert.deepEqual(loaded, checkpoint);
  });
});

// ---------------------------------------------------------------------------
// readEvents
// ---------------------------------------------------------------------------

describe("readEvents", () => {
  let tempDir;

  beforeEach(() => {
    tempDir = makeTempDir();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("reads JSONL files", async () => {
    writeJsonl(join(tempDir, "batch-a.jsonl"), [
      SAMPLE_EVENTS[0],
      SAMPLE_EVENTS[1],
    ]);
    writeJsonl(join(tempDir, "batch-b.jsonl"), [SAMPLE_EVENTS[2]]);

    // Also add a blank line in the middle of a file
    writeFileSync(
      join(tempDir, "batch-c.jsonl"),
      JSON.stringify({ event_id: "evt-004", type: "click", timestamp: "2024-06-01T13:00:00Z" }) +
        "\n\n" +
        JSON.stringify({ event_id: "evt-005", type: "click", timestamp: "2024-06-01T14:00:00Z" }) +
        "\n",
      "utf-8",
    );

    const events = await readEvents(tempDir);

    assert.equal(events.length, 5);
    // Files sorted alphabetically: batch-a, batch-b, batch-c
    assert.equal(events[0].event_id, "evt-001");
    assert.equal(events[1].event_id, "evt-002");
    assert.equal(events[2].event_id, "evt-003");
    assert.equal(events[3].event_id, "evt-004");
    assert.equal(events[4].event_id, "evt-005");
  });

  test("empty directory", async () => {
    const events = await readEvents(tempDir);
    assert.deepEqual(events, []);
  });
});

// ---------------------------------------------------------------------------
// transformEvent
// ---------------------------------------------------------------------------

describe("transformEvent", () => {
  test("valid event", () => {
    const result = transformEvent(SAMPLE_EVENTS[1]);

    assert.equal(result.event_id, "evt-002");
    assert.equal(result.event_date, "2024-06-01");
    assert.equal(result.hour, 11);
    assert.equal(result.is_purchase, true);
    assert.equal(result.has_user, true);
    assert.ok(result.processed_at);
    // Original fields preserved
    assert.equal(result.order_id, "ORD-001");
    assert.equal(result.type, "checkout");
  });

  test("missing required fields", () => {
    assert.equal(transformEvent({ event_id: "evt-100" }), null);
    assert.equal(transformEvent({ event_id: "evt-100", timestamp: "2024-06-01T00:00:00Z" }), null);
    assert.equal(transformEvent({ type: "click", timestamp: "2024-06-01T00:00:00Z" }), null);
    assert.equal(transformEvent({}), null);
  });

  test("null user_id", () => {
    const result = transformEvent(SAMPLE_EVENTS[2]);

    assert.equal(result.has_user, false);
    assert.equal(result.is_purchase, false);
    assert.equal(result.event_date, "2024-06-01");
    assert.equal(result.hour, 12);
  });
});

// ---------------------------------------------------------------------------
// runIncrementalEtl — integration tests
// ---------------------------------------------------------------------------

describe("runIncrementalEtl", () => {
  let tempDir;
  let inputDir;
  let outputDir;
  let checkpointPath;

  beforeEach(() => {
    tempDir = makeTempDir();
    inputDir = join(tempDir, "input");
    outputDir = join(tempDir, "output");
    checkpointPath = join(tempDir, "checkpoint.json");

    mkdirSync(inputDir, { recursive: true });
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  test("first run processes all events", async () => {
    writeJsonl(join(inputDir, "events.jsonl"), SAMPLE_EVENTS);

    const result = await runIncrementalEtl({
      inputDir,
      outputDir,
      checkpointPath,
    });

    assert.equal(result.pipeline_name, "events_etl");
    assert.equal(result.records_seen, 3);
    assert.equal(result.records_skipped, 0);
    assert.equal(result.records_processed, 3);
    assert.equal(result.records_failed, 0);
    assert.equal(result.checkpoint_updated, true);

    // Output file should exist
    const outputFiles = readdirSync(outputDir).filter((f) =>
      f.endsWith(".jsonl"),
    );
    assert.equal(outputFiles.length, 1);

    // Read output and verify enrichment
    const outputContent = readFileSync(
      join(outputDir, outputFiles[0]),
      "utf-8",
    );
    const outputEvents = outputContent
      .trim()
      .split("\n")
      .map((l) => JSON.parse(l));
    assert.equal(outputEvents.length, 3);
    assert.equal(outputEvents[0].event_date, "2024-06-01");
    assert.equal(outputEvents[1].is_purchase, true);
    assert.equal(outputEvents[2].has_user, false);

    // Checkpoint should be updated
    const cp = JSON.parse(readFileSync(checkpointPath, "utf-8"));
    assert.equal(cp.total_runs, 1);
    assert.equal(cp.processed_ids.length, 3);
    assert.ok(cp.processed_ids.includes("evt-001"));
    assert.ok(cp.processed_ids.includes("evt-002"));
    assert.ok(cp.processed_ids.includes("evt-003"));
  });

  test("second run with no new data", async () => {
    writeJsonl(join(inputDir, "events.jsonl"), SAMPLE_EVENTS);

    // First run
    await runIncrementalEtl({ inputDir, outputDir, checkpointPath });

    // Second run — same data
    const result = await runIncrementalEtl({
      inputDir,
      outputDir,
      checkpointPath,
    });

    assert.equal(result.records_seen, 3);
    assert.equal(result.records_skipped, 3);
    assert.equal(result.records_processed, 0);
    assert.equal(result.records_failed, 0);
    assert.equal(result.checkpoint_updated, false);

    // Should still be only 1 output file from the first run
    const outputFiles = readdirSync(outputDir).filter((f) =>
      f.endsWith(".jsonl"),
    );
    assert.equal(outputFiles.length, 1);

    // Checkpoint unchanged — still from first run only
    const cp = JSON.parse(readFileSync(checkpointPath, "utf-8"));
    assert.equal(cp.total_runs, 1);
    assert.equal(cp.processed_ids.length, 3);
  });

  test("new data after first run", async () => {
    writeJsonl(join(inputDir, "batch-01.jsonl"), [SAMPLE_EVENTS[0]]);

    // First run
    await runIncrementalEtl({ inputDir, outputDir, checkpointPath });

    // Add new data
    writeJsonl(join(inputDir, "batch-02.jsonl"), [
      SAMPLE_EVENTS[1],
      SAMPLE_EVENTS[2],
    ]);

    // Second run
    const result = await runIncrementalEtl({
      inputDir,
      outputDir,
      checkpointPath,
    });

    assert.equal(result.records_seen, 3); // all 3 events visible
    assert.equal(result.records_skipped, 1); // evt-001 skipped
    assert.equal(result.records_processed, 2); // evt-002 and evt-003 new
    assert.equal(result.records_failed, 0);

    // Should be 2 output files now
    const outputFiles = readdirSync(outputDir).filter((f) =>
      f.endsWith(".jsonl"),
    );
    assert.equal(outputFiles.length, 2);

    // Checkpoint should have all 3 IDs
    const cp = JSON.parse(readFileSync(checkpointPath, "utf-8"));
    assert.equal(cp.total_runs, 2);
    assert.equal(cp.processed_ids.length, 3);
  });

  test("failure before checkpoint update", async () => {
    writeJsonl(join(inputDir, "events.jsonl"), SAMPLE_EVENTS);

    // First successful run
    await runIncrementalEtl({ inputDir, outputDir, checkpointPath });

    const cpBefore = JSON.parse(readFileSync(checkpointPath, "utf-8"));

    // Add new events
    writeJsonl(join(inputDir, "new-events.jsonl"), [
      {
        event_id: "evt-004",
        type: "click",
        user_id: "U003",
        timestamp: "2024-06-02T09:00:00Z",
      },
    ]);

    // Second run with a failing saveCheckpoint
    const failingSave = async () => {
      throw new Error("simulated checkpoint write failure");
    };

    await assert.rejects(
      () =>
        runIncrementalEtl({
          inputDir,
          outputDir,
          checkpointPath,
          _saveCheckpointFn: failingSave,
        }),
      { message: "simulated checkpoint write failure" },
    );

    // Checkpoint should NOT have been updated
    const cpAfter = JSON.parse(readFileSync(checkpointPath, "utf-8"));
    assert.deepEqual(cpAfter, cpBefore);
  });

  test("rerun after failure", async () => {
    writeJsonl(join(inputDir, "events.jsonl"), SAMPLE_EVENTS);

    // First run fails at checkpoint save
    const failingSave = async () => {
      throw new Error("simulated failure");
    };

    await assert.rejects(() =>
      runIncrementalEtl({
        inputDir,
        outputDir,
        checkpointPath,
        _saveCheckpointFn: failingSave,
      }),
    );

    // No checkpoint file should exist
    assert.equal(existsSync(checkpointPath), false);

    // Rerun with real saveCheckpoint — should process all events
    const result = await runIncrementalEtl({
      inputDir,
      outputDir,
      checkpointPath,
    });

    assert.equal(result.records_seen, 3);
    assert.equal(result.records_skipped, 0);
    assert.equal(result.records_processed, 3);
    assert.equal(result.checkpoint_updated, true);

    const cp = JSON.parse(readFileSync(checkpointPath, "utf-8"));
    assert.equal(cp.processed_ids.length, 3);
    assert.equal(cp.total_runs, 1);
  });

  test("duplicate event_ids in input", async () => {
    // Write a file with a duplicate event_id
    writeJsonl(join(inputDir, "events.jsonl"), [
      SAMPLE_EVENTS[0],
      SAMPLE_EVENTS[1],
      SAMPLE_EVENTS[0], // duplicate of evt-001
    ]);

    const result = await runIncrementalEtl({
      inputDir,
      outputDir,
      checkpointPath,
    });

    assert.equal(result.records_seen, 3);
    assert.equal(result.records_skipped, 1); // 1 duplicate skipped
    assert.equal(result.records_processed, 2);
    assert.equal(result.records_failed, 0);

    // Output should contain only 2 events
    const outputFiles = readdirSync(outputDir).filter((f) =>
      f.endsWith(".jsonl"),
    );
    assert.equal(outputFiles.length, 1);
    const outputContent = readFileSync(
      join(outputDir, outputFiles[0]),
      "utf-8",
    );
    const outputEvents = outputContent
      .trim()
      .split("\n")
      .map((l) => JSON.parse(l));
    assert.equal(outputEvents.length, 2);

    // Checkpoint should have only 2 IDs
    const cp = JSON.parse(readFileSync(checkpointPath, "utf-8"));
    assert.equal(cp.processed_ids.length, 2);
  });
});
