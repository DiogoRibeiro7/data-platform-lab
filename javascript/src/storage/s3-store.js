import { normalizeKey } from "./local-store.js";

function isNotFound(error) {
  if (!error || typeof error !== "object") return false;
  if ("name" in error && ["NotFound", "NoSuchKey"].includes(String(error.name))) return true;
  if ("$metadata" in error && error.$metadata && typeof error.$metadata === "object") {
    return error.$metadata.httpStatusCode === 404;
  }
  return false;
}

async function bodyToBuffer(body) {
  if (Buffer.isBuffer(body)) return body;
  if (body instanceof Uint8Array) return Buffer.from(body);
  if (body && typeof body === "object" && "transformToByteArray" in body) {
    const transform = body.transformToByteArray;
    if (typeof transform === "function") return Buffer.from(await transform.call(body));
  }
  throw new TypeError("S3 getObject response Body must be bytes-like");
}

export class S3BlobStore {
  constructor({ client, bucket, keyPrefix = "" }) {
    if (!client || typeof client !== "object") throw new TypeError("client must be an object");
    if (typeof bucket !== "string") throw new TypeError("bucket must be a string");
    if (!bucket.trim()) throw new Error("bucket must not be empty");
    if (typeof keyPrefix !== "string") throw new TypeError("keyPrefix must be a string");

    for (const method of ["putObject", "getObject", "headObject", "listObjectsV2"]) {
      if (typeof client[method] !== "function") {
        throw new TypeError(`client must implement ${method}()`);
      }
    }

    const normalizedPrefix = keyPrefix.trim().replaceAll("\\", "/").replace(/\/+$/, "");
    this.client = client;
    this.bucket = bucket.trim();
    this.keyPrefix = normalizedPrefix ? normalizeKey(normalizedPrefix) : "";
  }

  _remoteKey(key) {
    const normalized = normalizeKey(key);
    return this.keyPrefix ? `${this.keyPrefix}/${normalized}` : normalized;
  }

  _logicalKey(remoteKey) {
    if (!this.keyPrefix) return remoteKey;
    const namespace = `${this.keyPrefix}/`;
    if (!remoteKey.startsWith(namespace)) return null;
    return remoteKey.slice(namespace.length) || null;
  }

  async putBytes(key, payload) {
    if (!Buffer.isBuffer(payload)) throw new TypeError("payload must be a Buffer");
    const normalized = normalizeKey(key);
    await this.client.putObject({
      Bucket: this.bucket,
      Key: this._remoteKey(normalized),
      Body: payload,
    });
    return { key: normalized, size_bytes: payload.length };
  }

  async getBytes(key) {
    const response = await this.client.getObject({
      Bucket: this.bucket,
      Key: this._remoteKey(key),
    });
    if (!response || typeof response !== "object" || !("Body" in response)) {
      throw new Error("S3 getObject response did not contain Body");
    }
    return bodyToBuffer(response.Body);
  }

  async exists(key) {
    try {
      await this.client.headObject({ Bucket: this.bucket, Key: this._remoteKey(key) });
      return true;
    } catch (error) {
      if (isNotFound(error)) return false;
      throw error;
    }
  }

  async listObjects(prefix = "") {
    if (typeof prefix !== "string") throw new TypeError("prefix must be a string");
    const logicalPrefix = prefix.trim() ? normalizeKey(prefix) : "";
    let remotePrefix = logicalPrefix;
    if (this.keyPrefix && logicalPrefix) remotePrefix = `${this.keyPrefix}/${logicalPrefix}`;
    else if (this.keyPrefix) remotePrefix = `${this.keyPrefix}/`;

    const objects = [];
    let continuationToken;
    do {
      const request = { Bucket: this.bucket, Prefix: remotePrefix };
      if (continuationToken) request.ContinuationToken = continuationToken;
      const response = await this.client.listObjectsV2(request);
      const contents = Array.isArray(response?.Contents) ? response.Contents : [];
      for (const item of contents) {
        if (!item || typeof item.Key !== "string" || !Number.isInteger(item.Size)) continue;
        const logicalKey = this._logicalKey(item.Key);
        if (logicalKey !== null) objects.push({ key: logicalKey, size_bytes: item.Size });
      }
      continuationToken = response?.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);

    return objects.sort((left, right) => left.key.localeCompare(right.key));
  }
}

export async function createAwsS3BlobStore({
  bucket,
  endpointUrl,
  region = "us-east-1",
  accessKeyId,
  secretAccessKey,
  keyPrefix = "",
  forcePathStyle,
}) {
  if ((accessKeyId === undefined) !== (secretAccessKey === undefined)) {
    throw new Error("accessKeyId and secretAccessKey must be provided together");
  }

  const sdk = await import("@aws-sdk/client-s3");
  const clientOptions = {
    region,
    forcePathStyle: forcePathStyle ?? Boolean(endpointUrl),
  };
  if (endpointUrl) clientOptions.endpoint = endpointUrl;
  if (accessKeyId !== undefined && secretAccessKey !== undefined) {
    clientOptions.credentials = { accessKeyId, secretAccessKey };
  }

  const sdkClient = new sdk.S3Client(clientOptions);
  const client = {
    putObject: (input) => sdkClient.send(new sdk.PutObjectCommand(input)),
    getObject: (input) => sdkClient.send(new sdk.GetObjectCommand(input)),
    headObject: (input) => sdkClient.send(new sdk.HeadObjectCommand(input)),
    listObjectsV2: (input) => sdkClient.send(new sdk.ListObjectsV2Command(input)),
  };

  return new S3BlobStore({ client, bucket, keyPrefix });
}
