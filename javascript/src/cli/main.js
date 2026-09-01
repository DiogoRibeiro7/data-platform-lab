#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const COMMANDS = new Map([
  ["benchmark", new URL("../benchmark/cli.js", import.meta.url)],
  ["stream", new URL("../streaming/cli.js", import.meta.url)],
  ["warehouse", new URL("../warehouse/cli.js", import.meta.url)],
]);

/** @returns {string} */
export function helpText() {
  return `Usage: data-platform-lab <command> [arguments]\n\nCommands:\n  benchmark   Run ingestion benchmarks\n  stream      Process streaming sensor events\n  warehouse   Run the warehouse pipeline`;
}

/**
 * Resolve one platform command into a Node child process invocation.
 * @param {string[]} argv
 * @returns {{script: string, args: string[]} | null}
 */
export function resolveCommand(argv) {
  if (!Array.isArray(argv)) {
    throw new TypeError("argv must be an array");
  }
  if (argv.length === 0) return null;

  const [command, ...args] = argv;
  const target = COMMANDS.get(command);
  if (!target) {
    throw new Error(`Unknown command: ${command}`);
  }
  return { script: fileURLToPath(target), args };
}

/** @param {string[]} [argv=process.argv.slice(2)] */
export function main(argv = process.argv.slice(2)) {
  const resolved = resolveCommand(argv);
  if (!resolved) {
    console.log(helpText());
    return 0;
  }

  const result = spawnSync(process.execPath, [resolved.script, ...resolved.args], {
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  return result.status ?? 1;
}

const isDirect = process.argv[1]
  ? import.meta.url === pathToFileURL(process.argv[1]).href
  : false;

if (isDirect) {
  process.exitCode = main();
}
