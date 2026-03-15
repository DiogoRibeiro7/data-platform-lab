/**
 * Sequential pipeline runner — executes ordered steps with retry,
 * skip-on-failure, and structured run summaries.
 */

/**
 * Sequential pipeline runner.
 *
 * @example
 * const pipeline = new Pipeline("my_etl");
 * pipeline.addStep("extract", extractFn);
 * pipeline.addStep("validate", validateFn, { retries: 2 });
 * pipeline.addStep("transform", transformFn);
 * pipeline.addStep("load", loadFn, { allowSkip: true });
 * pipeline.addStep("report", reportFn);
 *
 * const result = await pipeline.run();
 * console.log(formatResult(result));
 */
export class Pipeline {
  /**
   * @param {string} name - Pipeline name
   */
  constructor(name) {
    this._name = name;
    /** @type {Array<{name: string, fn: Function, retries: number, allowSkip: boolean}>} */
    this._steps = [];
  }

  /** @returns {string} */
  get name() {
    return this._name;
  }

  /** @returns {Array<{name: string, fn: Function, retries: number, allowSkip: boolean}>} */
  get steps() {
    return this._steps;
  }

  /**
   * Register a step. Returns this for chaining.
   * @param {string} name
   * @param {Function} fn - async function receiving context object
   * @param {object} [options]
   * @param {number} [options.retries=0] - max retry attempts
   * @param {boolean} [options.allowSkip=false] - if true, failure is non-fatal
   * @returns {Pipeline}
   */
  addStep(name, fn, options = {}) {
    const { retries = 0, allowSkip = false } = options;
    this._steps.push({ name, fn, retries, allowSkip });
    return this;
  }

  /**
   * Execute all registered steps in order.
   * @param {object} [context={}] - shared context passed to every step
   * @returns {Promise<{
   *   pipeline_name: string,
   *   status: "success"|"failed",
   *   started_at: string,
   *   ended_at: string,
   *   duration_seconds: number,
   *   steps: Array<{
   *     name: string,
   *     status: "success"|"failed"|"skipped",
   *     started_at: string,
   *     ended_at: string,
   *     duration_seconds: number,
   *     result: any,
   *     error: string|null,
   *     attempts: number
   *   }>,
   *   steps_passed: number,
   *   steps_failed: number,
   *   steps_skipped: number
   * }>}
   */
  async run(context = {}) {
    const pipelineStartedAt = new Date().toISOString();
    const pipelineStartMs = Date.now();

    context.pipeline_name = this._name;
    context.step_results = {};

    const stepResults = [];
    let pipelineFailed = false;

    for (const step of this._steps) {
      const stepStartedAt = new Date().toISOString();
      const stepStartMs = Date.now();

      let result = undefined;
      let error = null;
      let status = "success";
      let attempts = 0;
      const maxAttempts = 1 + step.retries;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        attempts = attempt;
        try {
          result = await step.fn(context);
          error = null;
          break;
        } catch (err) {
          error = err.message ?? String(err);
          result = undefined;
        }
      }

      const stepEndedAt = new Date().toISOString();
      const stepDuration = (Date.now() - stepStartMs) / 1000;

      if (error !== null) {
        if (step.allowSkip) {
          status = "skipped";
        } else {
          status = "failed";
          pipelineFailed = true;
        }
      } else {
        status = "success";
        context.step_results[step.name] = result;
      }

      stepResults.push({
        name: step.name,
        status,
        started_at: stepStartedAt,
        ended_at: stepEndedAt,
        duration_seconds: stepDuration,
        result: status === "success" ? result : undefined,
        error,
        attempts,
      });

      if (pipelineFailed) {
        break;
      }
    }

    const pipelineEndedAt = new Date().toISOString();
    const pipelineDuration = (Date.now() - pipelineStartMs) / 1000;

    let stepsPassed = 0;
    let stepsFailed = 0;
    let stepsSkipped = 0;

    for (const sr of stepResults) {
      if (sr.status === "success") stepsPassed++;
      else if (sr.status === "failed") stepsFailed++;
      else if (sr.status === "skipped") stepsSkipped++;
    }

    return {
      pipeline_name: this._name,
      status: pipelineFailed ? "failed" : "success",
      started_at: pipelineStartedAt,
      ended_at: pipelineEndedAt,
      duration_seconds: pipelineDuration,
      steps: stepResults,
      steps_passed: stepsPassed,
      steps_failed: stepsFailed,
      steps_skipped: stepsSkipped,
    };
  }
}

/**
 * Format a pipeline result as a human-readable string.
 * @param {object} result - PipelineResult from pipeline.run()
 * @returns {string}
 */
export function formatResult(result) {
  const lines = [];
  const total = result.steps.length;

  lines.push(`=== Pipeline: ${result.pipeline_name} ===`);
  lines.push(`Status: ${result.status}`);
  lines.push(`Duration: ${result.duration_seconds.toFixed(2)}s`);
  lines.push(
    `Steps: ${total} total | ${result.steps_passed} passed | ${result.steps_failed} failed | ${result.steps_skipped} skipped`,
  );
  lines.push("");

  for (const step of result.steps) {
    let tag;
    if (step.status === "success") tag = "PASS";
    else if (step.status === "skipped") tag = "SKIP";
    else tag = "FAIL";

    let detail = `${step.duration_seconds.toFixed(2)}s`;
    if (step.attempts > 1) {
      detail += `, ${step.attempts} attempts`;
    }

    let line = `  [${tag}] ${step.name} (${detail})`;
    if (step.error !== null) {
      line += ` — ${step.error}`;
    }
    lines.push(line);
  }

  return lines.join("\n");
}
