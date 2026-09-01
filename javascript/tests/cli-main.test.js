import assert from "node:assert/strict";
import test from "node:test";

import { helpText, resolveCommand } from "../src/cli/main.js";

test("helpText exposes all unified commands", () => {
  const help = helpText();
  assert.match(help, /benchmark/);
  assert.match(help, /stream/);
  assert.match(help, /warehouse/);
});

test("resolveCommand forwards child arguments unchanged", () => {
  const resolved = resolveCommand([
    "stream",
    "--input",
    "events.jsonl",
    "--output-dir",
    "out",
  ]);

  assert.ok(resolved);
  assert.match(resolved.script, /streaming[\\/]cli\.js$/);
  assert.deepEqual(resolved.args, [
    "--input",
    "events.jsonl",
    "--output-dir",
    "out",
  ]);
});

test("resolveCommand rejects unknown commands", () => {
  assert.throws(() => resolveCommand(["missing"]), /Unknown command/);
});
