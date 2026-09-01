import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  LocalBlobStore,
  StorageKeyError,
  normalizeKey,
} from "../src/storage/index.js";

test("normalizeKey rejects traversal", () => {
  assert.throws(() => normalizeKey("../outside.txt"), StorageKeyError);
});

test("normalizeKey converts backslashes", () => {
  assert.equal(
    normalizeKey("silver\\orders\\part-000.csv"),
    "silver/orders/part-000.csv",
  );
});

test("LocalBlobStore implements put, get, exists, and list", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "data-platform-lab-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const store = new LocalBlobStore(root);

  const first = store.putBytes("bronze/events/part-001.jsonl", Buffer.from("one\n"));
  store.putBytes("bronze/events/part-002.jsonl", Buffer.from("two\n"));
  store.putBytes("silver/events.csv", Buffer.from("id\n1\n"));

  assert.equal(first.size_bytes, 4);
  assert.equal(store.exists(first.key), true);
  assert.equal(store.getBytes(first.key).toString("utf8"), "one\n");
  assert.deepEqual(
    (await store.listObjects("bronze/events")).map((item) => item.key),
    ["bronze/events/part-001.jsonl", "bronze/events/part-002.jsonl"],
  );
});

test("LocalBlobStore replaces a complete object", (t) => {
  const root = mkdtempSync(join(tmpdir(), "data-platform-lab-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const store = new LocalBlobStore(root);

  store.putBytes("gold/report.json", Buffer.from("old"));
  const stored = store.putBytes("gold/report.json", Buffer.from("new-value"));

  assert.equal(stored.size_bytes, 9);
  assert.equal(store.getBytes("gold/report.json").toString("utf8"), "new-value");
});
