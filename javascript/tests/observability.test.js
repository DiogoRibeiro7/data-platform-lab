import { describe, test } from "node:test";
import assert from "node:assert/strict";

import {
  Timer,
  RunTracker,
  generateRunId,
  formatRunMetadata,
} from "../src/observability/index.js";

/**
 * Synchronous busy-wait for the given number of milliseconds.
 * @param {number} ms
 */
function busyWait(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) { /* spin */ }
}

// ---------------------------------------------------------------------------
// Timer
// ---------------------------------------------------------------------------
describe("Timer", () => {
  test("start and stop measures elapsed", () => {
    const timer = new Timer();
    timer.start();
    busyWait(15);
    timer.stop();
    assert.ok(timer.elapsed > 0.01, `expected elapsed > 0.01, got ${timer.elapsed}`);
  });

  test("elapsed is 0 before start", () => {
    const timer = new Timer();
    assert.equal(timer.elapsed, 0);
  });

  test("running property", () => {
    const timer = new Timer();
    assert.equal(timer.running, false, "before start");
    timer.start();
    assert.equal(timer.running, true, "after start");
    timer.stop();
    assert.equal(timer.running, false, "after stop");
  });

  test("elapsed while running", () => {
    const timer = new Timer();
    timer.start();
    busyWait(10);
    assert.ok(timer.elapsed > 0, `expected elapsed > 0 while running, got ${timer.elapsed}`);
  });
});

// ---------------------------------------------------------------------------
// RunTracker
// ---------------------------------------------------------------------------
describe("RunTracker", () => {
  test("start and finish sets timing and status", () => {
    const tracker = new RunTracker("pipeline_a");
    tracker.start();
    tracker.finish();
    const meta = tracker.metadata;
    assert.equal(meta.status, "success");
    assert.ok(meta.started_at !== null, "started_at should be set");
    assert.ok(meta.ended_at !== null, "ended_at should be set");
    assert.ok(meta.duration_seconds >= 0, "duration_seconds should be >= 0");
  });

  test("finish with custom status", () => {
    const tracker = new RunTracker("pipeline_b");
    tracker.start();
    tracker.finish("failed");
    assert.equal(tracker.metadata.status, "failed");
  });

  test("row counting", () => {
    const tracker = new RunTracker("pipeline_c");
    tracker.incRowsRead(100);
    tracker.incRowsWritten(95);
    tracker.incRowsRejected(5);
    const meta = tracker.metadata;
    assert.equal(meta.rows_read, 100);
    assert.equal(meta.rows_written, 95);
    assert.equal(meta.rows_rejected, 5);
  });

  test("file counting", () => {
    const tracker = new RunTracker("pipeline_d");
    tracker.incFilesProcessed(3);
    tracker.incFilesRejected(1);
    const meta = tracker.metadata;
    assert.equal(meta.files_processed, 3);
    assert.equal(meta.files_rejected, 1);
  });

  test("warnings and errors", () => {
    const tracker = new RunTracker("pipeline_e");
    tracker.addWarning("w1");
    tracker.addWarning("w2");
    tracker.addError("e1");
    const meta = tracker.metadata;
    assert.deepEqual(meta.warnings, ["w1", "w2"]);
    assert.deepEqual(meta.errors, ["e1"]);
  });

  test("extra metadata", () => {
    const tracker = new RunTracker("pipeline_f");
    tracker.setExtra("key", "value");
    assert.equal(tracker.metadata.extra.key, "value");
  });

  test("custom run_id", () => {
    const tracker = new RunTracker("test", "my-run-123");
    assert.equal(tracker.metadata.run_id, "my-run-123");
  });

  test("default run_id format", () => {
    const tracker = new RunTracker("test");
    assert.match(tracker.metadata.run_id, /^\d{8}_\d{6}$/);
  });

  test("multiple increments accumulate", () => {
    const tracker = new RunTracker("pipeline_g");
    tracker.incRowsRead(10);
    tracker.incRowsRead(20);
    assert.equal(tracker.metadata.rows_read, 30);
  });

  test("metadata is a snapshot", () => {
    const tracker = new RunTracker("pipeline_h");
    tracker.incRowsRead(10);
    const snapshot = tracker.metadata;

    // Mutate the tracker after taking the snapshot
    tracker.incRowsRead(50);
    tracker.addWarning("new warning");
    tracker.setExtra("added", true);

    // The original snapshot should be unchanged
    assert.equal(snapshot.rows_read, 10);
    assert.deepEqual(snapshot.warnings, []);
    assert.deepEqual(snapshot.extra, {});
  });
});

// ---------------------------------------------------------------------------
// formatRunMetadata
// ---------------------------------------------------------------------------
describe("formatRunMetadata", () => {
  test("basic format", () => {
    const meta = {
      pipeline_name: "test_pipeline",
      run_id: "20240101_120000",
      status: "success",
      started_at: "2024-01-01T12:00:00.000Z",
      ended_at: "2024-01-01T12:00:05.000Z",
      duration_seconds: 5.0,
      rows_read: 100,
      rows_written: 95,
      rows_rejected: 5,
      files_processed: 0,
      files_rejected: 0,
      warnings: [],
      errors: [],
      extra: {},
    };
    const output = formatRunMetadata(meta);
    assert.ok(output.includes("test_pipeline"), "should contain pipeline_name");
    assert.ok(output.includes("success"), "should contain status");
    assert.ok(output.includes("Rows read"), "should contain 'Rows read'");
  });

  test("format with warnings", () => {
    const meta = {
      pipeline_name: "warn_pipeline",
      run_id: "20240101_120000",
      status: "success",
      started_at: "2024-01-01T12:00:00.000Z",
      ended_at: "2024-01-01T12:00:01.000Z",
      duration_seconds: 1.0,
      rows_read: 50,
      rows_written: 48,
      rows_rejected: 2,
      files_processed: 0,
      files_rejected: 0,
      warnings: ["missing emails", "bad dates"],
      errors: [],
      extra: {},
    };
    const output = formatRunMetadata(meta);
    assert.ok(output.includes("Warnings"), "should contain 'Warnings'");
    assert.ok(output.includes("missing emails"), "should contain warning text");
    assert.ok(output.includes("bad dates"), "should contain second warning text");
  });

  test("format with extras", () => {
    const meta = {
      pipeline_name: "extra_pipeline",
      run_id: "20240101_120000",
      status: "success",
      started_at: "2024-01-01T12:00:00.000Z",
      ended_at: "2024-01-01T12:00:02.000Z",
      duration_seconds: 2.0,
      rows_read: 200,
      rows_written: 200,
      rows_rejected: 0,
      files_processed: 0,
      files_rejected: 0,
      warnings: [],
      errors: [],
      extra: { source: "api", version: "2.1" },
    };
    const output = formatRunMetadata(meta);
    assert.ok(output.includes("Extra"), "should contain 'Extra'");
    assert.ok(output.includes("source"), "should contain extra key");
    assert.ok(output.includes("api"), "should contain extra value");
  });
});

// ---------------------------------------------------------------------------
// generateRunId
// ---------------------------------------------------------------------------
describe("generateRunId", () => {
  test("format matches pattern", () => {
    const id = generateRunId();
    assert.match(id, /^\d{8}_\d{6}$/);
  });
});
