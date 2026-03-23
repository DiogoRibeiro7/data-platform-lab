/**
 * Lightweight JSON config loader for pipeline workflows.
 *
 * Supports a simple precedence model:
 *   defaults < config file < CLI flags
 *
 * @module config
 */

import { readFileSync, existsSync } from "node:fs";

/**
 * Load and parse a JSON config file.
 *
 * @param {string} filePath - Path to the JSON config file.
 * @returns {object} Parsed config object.
 * @throws {Error} If the file doesn't exist or contains invalid JSON.
 */
export function loadConfig(filePath) {
  if (!existsSync(filePath)) {
    throw new Error(`Config file not found: ${filePath}`);
  }
  let raw;
  try {
    raw = readFileSync(filePath, "utf-8");
  } catch (err) {
    throw new Error(`Cannot read config file ${filePath}: ${err.message}`);
  }
  let data;
  try {
    data = JSON.parse(raw);
  } catch (err) {
    throw new Error(`Invalid JSON in config file ${filePath}: ${err.message}`);
  }
  if (typeof data !== "object" || data === null || Array.isArray(data)) {
    throw new Error(`Config file must contain a JSON object, got ${typeof data}`);
  }
  return data;
}

/**
 * Validate a config object.
 *
 * @param {object} data - The loaded config.
 * @param {object} [options]
 * @param {string[]} [options.required] - Keys that must be present.
 * @param {string[]} [options.known] - Keys that are recognised (warns on unknown).
 * @returns {string[]} List of error messages (empty if valid).
 */
export function validateConfig(data, { required, known } = {}) {
  const errors = [];
  if (required) {
    for (const key of required) {
      if (!(key in data)) {
        errors.push(`Missing required config key: ${key}`);
      }
    }
  }
  if (known) {
    for (const key of Object.keys(data)) {
      if (!known.includes(key)) {
        console.warn(`Unknown config key: ${key} (will be ignored)`);
      }
    }
  }
  return errors;
}

/**
 * Merge config with standard precedence: defaults < config < CLI overrides.
 *
 * Only non-undefined override values replace config values.
 *
 * @param {object} defaults
 * @param {object} [config]
 * @param {object} [overrides]
 * @returns {object}
 */
export function mergeConfig(defaults, config, overrides) {
  const result = { ...defaults };
  if (config) {
    for (const [k, v] of Object.entries(config)) {
      if (k in result) result[k] = v;
    }
  }
  if (overrides) {
    for (const [k, v] of Object.entries(overrides)) {
      if (v !== undefined && k in result) result[k] = v;
    }
  }
  return result;
}
