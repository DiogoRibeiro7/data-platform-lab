import { mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { readdir } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";

/** Error raised when an object key is unsafe or invalid. */
export class StorageKeyError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message);
    this.name = "StorageKeyError";
  }
}

/**
 * Validate and normalize a portable object key.
 * @param {string} key
 * @returns {string}
 */
export function normalizeKey(key) {
  if (typeof key !== "string") {
    throw new TypeError("key must be a string");
  }

  const candidate = key.trim().replaceAll("\\", "/");
  const parts = candidate.split("/");

  if (!candidate || candidate === "." || candidate === "/") {
    throw new StorageKeyError("key must not be empty");
  }
  if (candidate.startsWith("/")) {
    throw new StorageKeyError("key must be relative");
  }
  if (parts.some((part) => part === "" || part === "." || part === "..")) {
    throw new StorageKeyError("key must not contain traversal components");
  }

  return parts.join("/");
}

/** Filesystem-backed object store for local development and tests. */
export class LocalBlobStore {
  /** @param {string} root */
  constructor(root) {
    if (typeof root !== "string") {
      throw new TypeError("root must be a string");
    }
    this.root = resolve(root);
    mkdirSync(this.root, { recursive: true });
  }

  /**
   * Resolve a validated key below the storage root.
   * @param {string} key
   * @returns {string}
   */
  _resolveKey(key) {
    const normalized = normalizeKey(key);
    const target = resolve(this.root, ...normalized.split("/"));
    const rel = relative(this.root, target);

    if (rel.startsWith(`..${sep}`) || rel === "..") {
      throw new StorageKeyError("key resolves outside storage root");
    }
    return target;
  }

  /**
   * Persist a buffer atomically under a key.
   * @param {string} key
   * @param {Buffer} payload
   * @returns {{key: string, size_bytes: number}}
   */
  putBytes(key, payload) {
    if (!Buffer.isBuffer(payload)) {
      throw new TypeError("payload must be a Buffer");
    }

    const normalized = normalizeKey(key);
    const target = this._resolveKey(normalized);
    mkdirSync(dirname(target), { recursive: true });

    const temporary = join(dirname(target), `.${target.split(sep).at(-1)}.tmp`);
    writeFileSync(temporary, payload);
    renameSync(temporary, target);

    return { key: normalized, size_bytes: payload.length };
  }

  /** @param {string} key @returns {Buffer} */
  getBytes(key) {
    return readFileSync(this._resolveKey(key));
  }

  /** @param {string} key @returns {boolean} */
  exists(key) {
    try {
      return statSync(this._resolveKey(key)).isFile();
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        return false;
      }
      throw error;
    }
  }

  /**
   * List objects in deterministic key order.
   * @param {string} [prefix=""]
   * @returns {Promise<Array<{key: string, size_bytes: number}>>}
   */
  async listObjects(prefix = "") {
    if (typeof prefix !== "string") {
      throw new TypeError("prefix must be a string");
    }
    const normalizedPrefix = prefix.trim() ? normalizeKey(prefix) : "";
    const objects = [];

    /** @param {string} directory */
    const visit = async (directory) => {
      const entries = await readdir(directory, { withFileTypes: true });
      for (const entry of entries) {
        const path = join(directory, entry.name);
        if (entry.isDirectory()) {
          await visit(path);
        } else if (entry.isFile()) {
          const key = relative(this.root, path).split(sep).join("/");
          if (!normalizedPrefix || key.startsWith(normalizedPrefix)) {
            objects.push({ key, size_bytes: statSync(path).size });
          }
        }
      }
    };

    await visit(this.root);
    return objects.sort((left, right) => left.key.localeCompare(right.key));
  }
}
