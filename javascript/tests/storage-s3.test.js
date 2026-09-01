import assert from "node:assert/strict";
import test from "node:test";

import { runStorageSmoke } from "../src/storage/cli.js";
import { S3BlobStore } from "../src/storage/index.js";

class FakeS3Error extends Error {
  constructor(name, status) {
    super(name);
    this.name = name;
    this.$metadata = { httpStatusCode: status };
  }
}

class FakeS3Client {
  constructor() {
    this.objects = new Map();
    this.listCalls = [];
  }

  async putObject(input) {
    this.objects.set(`${input.Bucket}:${input.Key}`, Buffer.from(input.Body));
    return {};
  }

  async getObject(input) {
    const object = this.objects.get(`${input.Bucket}:${input.Key}`);
    if (!object) throw new FakeS3Error("NoSuchKey", 404);
    return { Body: object };
  }

  async headObject(input) {
    if (!this.objects.has(`${input.Bucket}:${input.Key}`)) {
      throw new FakeS3Error("NotFound", 404);
    }
    return {};
  }

  async listObjectsV2(input) {
    this.listCalls.push(input);
    const namespace = `${input.Bucket}:`;
    const keys = [...this.objects.keys()]
      .filter((stored) => stored.startsWith(namespace))
      .map((stored) => stored.slice(namespace.length))
      .filter((key) => key.startsWith(input.Prefix))
      .sort();

    const start = input.ContinuationToken === "page-2" ? 1 : 0;
    const page = keys.slice(start, start + 1);
    const truncated = start + 1 < keys.length;
    return {
      Contents: page.map((Key) => ({
        Key,
        Size: this.objects.get(`${input.Bucket}:${Key}`).length,
      })),
      IsTruncated: truncated,
      NextContinuationToken: truncated ? "page-2" : undefined,
    };
  }
}

test("S3BlobStore preserves logical keys and paginates", async () => {
  const client = new FakeS3Client();
  const store = new S3BlobStore({ client, bucket: "platform", keyPrefix: "lab" });

  await store.putBytes("bronze/events/b.jsonl", Buffer.from("bb"));
  await store.putBytes("bronze/events/a.jsonl", Buffer.from("a"));

  assert.equal(
    client.objects.get("platform:lab/bronze/events/b.jsonl").toString("utf8"),
    "bb",
  );
  assert.equal((await store.getBytes("bronze/events/a.jsonl")).toString("utf8"), "a");
  assert.equal(await store.exists("bronze/events/a.jsonl"), true);
  assert.equal(await store.exists("bronze/events/missing.jsonl"), false);
  assert.deepEqual(
    (await store.listObjects("bronze/events")).map((item) => item.key),
    ["bronze/events/a.jsonl", "bronze/events/b.jsonl"],
  );
  assert.equal(client.listCalls.length, 2);
});

test("S3BlobStore reraises non-missing head errors", async () => {
  const client = new FakeS3Client();
  client.headObject = async () => {
    throw new FakeS3Error("AccessDenied", 403);
  };
  const store = new S3BlobStore({ client, bucket: "platform" });

  await assert.rejects(() => store.exists("private/object"), /AccessDenied/);
});

test("runStorageSmoke consumes the async-compatible blob contract", async () => {
  const store = new S3BlobStore({ client: new FakeS3Client(), bucket: "platform" });
  const report = await runStorageSmoke(store);

  assert.equal(report.round_trip, true);
  assert.equal(report.listed, true);
  assert.equal(report.key, "_platform/smoke.txt");
});
