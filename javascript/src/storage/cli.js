#!/usr/bin/env node

import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";

import { LocalBlobStore } from "./local-store.js";
import { createAwsS3BlobStore } from "./s3-store.js";

const SMOKE_PAYLOAD = Buffer.from("data-platform-lab-storage-smoke\n");

export function resolveS3Region(cliRegion, env = process.env) {
  return cliRegion ?? env.AWS_DEFAULT_REGION;
}

export async function runStorageSmoke(store, key = "_platform/smoke.txt") {
  const stored = await store.putBytes(key, SMOKE_PAYLOAD);
  const roundTrip = await store.getBytes(key);
  const prefix = stored.key.includes("/")
    ? stored.key.slice(0, stored.key.lastIndexOf("/"))
    : "";
  const listed = await store.listObjects(prefix);
  const exists = await store.exists(stored.key);

  if (!Buffer.from(roundTrip).equals(SMOKE_PAYLOAD)) {
    throw new Error("storage smoke check failed: round-trip payload changed");
  }
  if (!listed.some((item) => item.key === stored.key)) {
    throw new Error("storage smoke check failed: stored key was not listed");
  }
  if (!exists) throw new Error("storage smoke check failed: stored key does not exist");

  return { key: stored.key, size_bytes: stored.size_bytes, round_trip: true, listed: true };
}

export async function main(argv = process.argv.slice(2)) {
  const { values } = parseArgs({
    args: argv,
    options: {
      backend: { type: "string", default: "local" },
      root: { type: "string", default: "../data/object-store" },
      bucket: { type: "string", default: process.env.DPL_S3_BUCKET || "data-platform-lab" },
      "endpoint-url": { type: "string", default: process.env.DPL_S3_ENDPOINT_URL },
      region: { type: "string" },
      "key-prefix": { type: "string", default: process.env.DPL_S3_KEY_PREFIX || "" },
      "smoke-key": { type: "string", default: "_platform/smoke.txt" },
      help: { type: "boolean", short: "h" },
    },
    strict: true,
  });

  if (values.help) {
    console.log("Usage: data-platform-lab storage [--backend local|s3] [storage options]");
    return 0;
  }
  if (!new Set(["local", "s3"]).has(values.backend)) {
    throw new Error(`Unknown storage backend: ${values.backend}`);
  }

  const store = values.backend === "local"
    ? new LocalBlobStore(values.root)
    : await createAwsS3BlobStore({
        bucket: values.bucket,
        endpointUrl: values["endpoint-url"],
        region: resolveS3Region(values.region),
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        keyPrefix: values["key-prefix"],
      });

  try {
    const report = await runStorageSmoke(store, values["smoke-key"]);
    console.log("=== Storage Smoke Check ===");
    console.log(`Backend    : ${values.backend}`);
    console.log(`Key        : ${report.key}`);
    console.log(`Bytes      : ${report.size_bytes}`);
    console.log("Round trip : ok");
    console.log("Listing    : ok");
    return 0;
  } finally {
    if (typeof store.destroy === "function") await store.destroy();
  }
}

const isDirect = process.argv[1]
  ? import.meta.url === pathToFileURL(process.argv[1]).href
  : false;

if (isDirect) process.exitCode = await main();
