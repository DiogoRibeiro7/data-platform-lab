import assert from "node:assert/strict";
import test from "node:test";

import { PostgresRunStore } from "../src/metadata/index.js";

class FakeClient {
  constructor() { this.calls = []; this.rows = []; }
  async query(text, values) {
    this.calls.push({ text, values });
    if (text.startsWith("SELECT")) return { rows: this.rows };
    return { rows: [] };
  }
}

const metadata = {
  pipeline_name: "demo", run_id: "r1", status: "success",
  started_at: "2026-09-01T00:00:00Z", ended_at: "2026-09-01T00:00:01Z",
  duration_seconds: 1, rows_read: 10, rows_written: 9, rows_rejected: 1,
  files_processed: 1, files_rejected: 0, warnings: [], errors: [], extra: { source: "test" },
};

test("PostgresRunStore upserts the shared run metadata shape", async () => {
  const client = new FakeClient();
  await new PostgresRunStore(client).save(metadata);
  assert.match(client.calls[0].text, /ON CONFLICT/);
  assert.equal(client.calls[0].values[0], "demo");
  assert.equal(client.calls[0].values[13], JSON.stringify({ source: "test" }));
});

test("PostgresRunStore reads and lists rows through injected client", async () => {
  const client = new FakeClient();
  client.rows = [metadata];
  const store = new PostgresRunStore(client);
  assert.deepEqual(await store.get("demo", "r1"), metadata);
  assert.deepEqual(await store.listRecent(5), [metadata]);
});
