import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { writeFileSync, mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { loadConfig, validateConfig, mergeConfig } from "../src/config.js";

/** Create a fresh temp directory for each test that needs files. */
function makeTmpDir() {
  const dir = join(tmpdir(), `config-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

// ---------------------------------------------------------------------------
// loadConfig
// ---------------------------------------------------------------------------

describe("loadConfig", () => {
  it("valid JSON file returns correct object", () => {
    const dir = makeTmpDir();
    const filePath = join(dir, "config.json");
    const cfg = { input_dir: "/data/raw", output_dir: "/data/out", batch_size: 100 };
    writeFileSync(filePath, JSON.stringify(cfg), "utf-8");

    const result = loadConfig(filePath);

    assert.deepStrictEqual(result, cfg);
    rmSync(dir, { recursive: true, force: true });
  });

  it("file not found throws with 'not found'", () => {
    assert.throws(() => loadConfig("/tmp/does_not_exist_config.json"), {
      message: /not found/,
    });
  });

  it("invalid JSON throws with 'Invalid JSON'", () => {
    const dir = makeTmpDir();
    const filePath = join(dir, "bad.json");
    writeFileSync(filePath, "{not valid json!!!", "utf-8");

    assert.throws(() => loadConfig(filePath), {
      message: /Invalid JSON/,
    });
    rmSync(dir, { recursive: true, force: true });
  });

  it("non-object JSON throws with 'JSON object'", () => {
    const dir = makeTmpDir();
    const filePath = join(dir, "array.json");
    writeFileSync(filePath, JSON.stringify([1, 2, 3]), "utf-8");

    assert.throws(() => loadConfig(filePath), {
      message: /JSON object/,
    });
    rmSync(dir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// validateConfig
// ---------------------------------------------------------------------------

describe("validateConfig", () => {
  it("no errors for valid config with all required keys", () => {
    const data = { input_dir: "/data", output_dir: "/out" };
    const errors = validateConfig(data, { required: ["input_dir", "output_dir"] });

    assert.deepStrictEqual(errors, []);
  });

  it("missing required keys appear in error messages", () => {
    const data = { input_dir: "/data" };
    const errors = validateConfig(data, {
      required: ["input_dir", "output_dir", "batch_size"],
    });

    assert.equal(errors.length, 2);
    assert.ok(errors.some((e) => e.includes("output_dir")));
    assert.ok(errors.some((e) => e.includes("batch_size")));
  });

  it("unknown keys produce no errors", () => {
    const data = { input_dir: "/data", mystery_key: 42 };
    const errors = validateConfig(data, { known: ["input_dir"] });

    assert.deepStrictEqual(errors, []);
  });
});

// ---------------------------------------------------------------------------
// mergeConfig
// ---------------------------------------------------------------------------

describe("mergeConfig", () => {
  it("defaults only returns the defaults", () => {
    const defaults = { input_dir: "/default", batch_size: 50 };

    const result = mergeConfig(defaults);

    assert.deepStrictEqual(result, defaults);
    assert.notEqual(result, defaults); // must be a new object
  });

  it("config values override defaults", () => {
    const defaults = { input_dir: "/default", batch_size: 50 };
    const config = { batch_size: 200 };

    const result = mergeConfig(defaults, config);

    assert.equal(result.input_dir, "/default");
    assert.equal(result.batch_size, 200);
  });

  it("CLI overrides override config values", () => {
    const defaults = { input_dir: "/default", batch_size: 50 };
    const config = { batch_size: 200 };
    const cli = { batch_size: 999 };

    const result = mergeConfig(defaults, config, cli);

    assert.equal(result.batch_size, 999);
  });

  it("undefined override does not replace config value", () => {
    const defaults = { input_dir: "/default", batch_size: 50 };
    const config = { batch_size: 200 };
    const cli = { batch_size: undefined };

    const result = mergeConfig(defaults, config, cli);

    assert.equal(result.batch_size, 200);
  });

  it("full precedence: defaults < config < CLI", () => {
    const defaults = { a: 1, b: 2, c: 3 };
    const config = { a: 10, b: 20 };
    const cli = { a: 100 };

    const result = mergeConfig(defaults, config, cli);

    assert.equal(result.a, 100); // CLI wins
    assert.equal(result.b, 20); // config wins over default
    assert.equal(result.c, 3); // default survives
  });
});
