import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync, mkdirSync, readFileSync, writeFileSync, readdirSync, rmSync, statSync,
} from "node:fs";
import { join, resolve, dirname, basename } from "node:path";
import { tmpdir } from "node:os";
import { DatabaseSync } from "node:sqlite";

import { runPipeline } from "../src/ingestion/csv-pipeline.js";
import { runApiPipeline } from "../src/ingestion/api-pipeline.js";
import { runIncrementalEtl } from "../src/transform/incremental-etl.js";
import { processStream } from "../src/streaming/processor.js";
import { runSensorDemo } from "../src/sensor-demo.js";
import { runWarehousePipeline } from "../src/warehouse/loader.js";
import { readManifest, validateManifest } from "../src/manifest.js";
import { SAMPLE_DIR, SQL_DIR, writeJsonl } from "./helpers.js";

const PIPELINES = [
  "csv_ingestion", "api_ingestion", "events_etl", "sensor_stream", "sensor_demo", "warehouse",
];
const API_URL = "https://example.invalid/posts";
const POSTS = [
  { id: 1, userId: 1, title: "First post", body: "Hello world" },
  { id: 2, userId: 2, title: "Second post", body: "Another post" },
];
const readJson = (path) => JSON.parse(readFileSync(path, "utf-8"));
const jsonlCount = (path) => readFileSync(path, "utf-8")
  .split(/\r?\n/)
  .filter((line) => line.trim())
  .map((line) => JSON.parse(line)).length;

/** Run real pipeline I/O, replacing only the API network boundary. */
async function run(name, root, context) {
  const output = join(root, "output");
  const input = join(root, "input");
  mkdirSync(output);
  mkdirSync(input);
  switch (name) {
    case "csv_ingestion":
      writeFileSync(join(input, "rows.csv"), "id,name\n1,Ada\n1,Ada\n2,Bob\n");
      return runPipeline({
        inputDir: input,
        outputPath: join(output, "merged.csv"),
        requiredColumns: ["id", "name"],
      });
    case "api_ingestion":
      context.mock.method(globalThis, "fetch", async () => ({ ok: true, json: async () => POSTS }));
      return runApiPipeline({
        baseUrl: API_URL,
        rawDir: join(output, "raw"),
        processedDir: join(output, "processed"),
        maxPages: 1,
      });
    case "events_etl":
      await writeJsonl(join(input, "events.jsonl"), [
        { event_id: "e1", type: "page_view", timestamp: "2024-06-01T10:00:00Z" },
        { event_id: "e2", type: "checkout", timestamp: "2024-06-01T11:00:00Z" },
      ]);
      return runIncrementalEtl({
        inputDir: input,
        outputDir: join(output, "events"),
        checkpointPath: join(root, "checkpoint.json"),
      });
    case "sensor_stream":
      return processStream(join(SAMPLE_DIR, "sensor_events.json"), output);
    case "sensor_demo":
      return runSensorDemo({ dataDir: SAMPLE_DIR, outputDir: output });
    case "warehouse":
      return runWarehousePipeline({
        dataDir: SAMPLE_DIR,
        dbPath: join(output, "warehouse.db"),
        reportDir: join(output, "reports"),
        sqlDir: SQL_DIR,
      });
    default:
      throw new Error(`Unknown test pipeline: ${name}`);
  }
}

// The test runner isolates this file in a process. Its cases run serially so
// default manifest paths can be exercised without writing into the checkout.
describe("pipeline manifest contracts", { concurrency: false }, () => {
  let root;
  let previousDirectory;
  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "pipeline-manifests-"));
    previousDirectory = process.cwd();
    process.chdir(root);
  });
  afterEach(() => {
    process.chdir(previousDirectory);
    rmSync(root, { recursive: true, force: true });
  });

  for (const name of PIPELINES) {
    it(`${name}: manifest matches persisted output`, async (context) => {
      const result = await run(name, root, context);
      const path = resolve(result.manifest_path);
      assert.equal(dirname(path), join(root, "data", "manifests"));
      assert.deepEqual(readdirSync(dirname(path)), [basename(path)]);
      const manifest = readManifest(path);
      assert.deepEqual(validateManifest(manifest), []);
      assert.equal(manifest.pipeline_name, name);
      assert.equal(manifest.status, "success");
      assert.match(manifest.run_id, /^\d{8}_\d{6}$/);
      assert.equal(basename(path), `${name}_${manifest.run_id}.json`);
      assert.ok(Number.isFinite(Date.parse(manifest.created_at)));

      const output = join(root, "output");
      if (name === "csv_ingestion") {
        assert.deepEqual(manifest.source, [join(root, "input", "rows.csv")]);
        assert.equal(manifest.output, join(output, "merged.csv"));
        assert.equal(readFileSync(manifest.output, "utf-8"), "id,name\n1,Ada\n2,Bob\n");
        assert.equal(manifest.row_count, 2);
        assert.deepEqual(manifest.schema_hint, ["id", "name"]);
        assert.equal(manifest.rows_read, 3);
        assert.equal(manifest.duplicates_removed, 1);
      } else if (name === "api_ingestion") {
        assert.equal(manifest.source, API_URL);
        assert.equal(manifest.run_id, result.run_id);
        assert.equal(manifest.output, result.processed_path);
        assert.equal(manifest.row_count, readJson(manifest.output).length);
        assert.equal(manifest.row_count, 2);
        assert.deepEqual(readJson(manifest.raw_path), POSTS);
        assert.equal(manifest.pages_fetched, 1);
      } else if (name === "events_etl") {
        assert.equal(manifest.source, join(root, "input"));
        assert.equal(manifest.output, join(output, "events"));
        const files = readdirSync(manifest.output);
        assert.equal(files.length, 1);
        assert.equal(manifest.row_count, jsonlCount(join(manifest.output, files[0])));
        assert.equal(manifest.row_count, 2);
        assert.equal(manifest.checkpoint_updated, true);
        assert.deepEqual(readJson(join(root, "checkpoint.json")).processed_ids.sort(), ["e1", "e2"]);
      } else if (["sensor_stream", "sensor_demo"].includes(name)) {
        assert.equal(manifest.source, join(SAMPLE_DIR, "sensor_events.json"));
        assert.equal(manifest.row_count, jsonlCount(join(output, "accepted.jsonl")));
        assert.equal(manifest.row_count, 14);
        if (name === "sensor_stream") {
          assert.equal(manifest.output, join(output, "accepted.jsonl"));
          assert.equal(manifest.events_seen, 16);
          assert.equal(manifest.events_rejected, 1);
          assert.equal(manifest.events_duplicate, 1);
          assert.equal(jsonlCount(manifest.dead_letter_path), 2);
        } else {
          assert.ok(manifest.output.includes(join(output, "accepted.jsonl")));
          assert.ok(manifest.output.every((file) => statSync(file).isFile()));
          assert.equal(result.pipeline_result.status, "success");
        }
      } else {
        assert.equal(manifest.source, SAMPLE_DIR);
        assert.equal(manifest.output, join(output, "reports"));
        assert.equal(manifest.db_path, join(output, "warehouse.db"));
        const db = new DatabaseSync(manifest.db_path, { readOnly: true });
        const counts = {};
        try {
          for (const table of [
            "dim_customer", "dim_product", "dim_date", "fact_order", "fact_order_item", "fact_event",
          ]) {
            counts[table] = db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count;
          }
        } finally {
          db.close();
        }
        assert.deepEqual(manifest.warehouse_tables, counts);
        assert.equal(manifest.row_count, Object.values(counts).reduce((a, b) => a + b, 0));
        assert.ok(manifest.row_count > 0);
        assert.equal(readJson(join(output, "reports", "warehouse_summary.json")).status, "success");
      }
    });

    it(`${name}: manifest write failure is visible`, async (context) => {
      const manifestDirectory = join(root, "data", "manifests");
      mkdirSync(dirname(manifestDirectory));
      writeFileSync(manifestDirectory, "blocked");
      await assert.rejects(() => run(name, root, context), (error) => {
        assert.ok(["EEXIST", "ENOTDIR"].includes(error.code));
        assert.match(error.message, /manifests/);
        return true;
      });
      assert.equal(readFileSync(manifestDirectory, "utf-8"), "blocked");
      const outputs = readdirSync(join(root, "output"), { recursive: true, withFileTypes: true });
      assert.ok(outputs.some((entry) => entry.isFile()));
    });
  }

  it("failed sensor run records failure", async () => {
    const result = await runSensorDemo({
      dataDir: join(root, "missing"),
      outputDir: join(root, "output"),
    });
    const manifest = readManifest(result.manifest_path);
    assert.equal(result.pipeline_result.status, "failed");
    assert.equal(manifest.status, "failed");
    assert.equal(manifest.row_count, 0);
  });
});
