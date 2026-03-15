import { describe, test } from "node:test";
import assert from "node:assert/strict";

import { Pipeline, formatResult } from "../src/orchestration/index.js";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeStep(returnValue = "ok") {
  return async (ctx) => returnValue;
}

function makeFailingStep(errorMsg = "boom") {
  return async (ctx) => {
    throw new Error(errorMsg);
  };
}

function makeFlakyStep(failCount, returnValue = "recovered") {
  let calls = 0;
  return async (ctx) => {
    calls++;
    if (calls <= failCount) throw new Error(`flaky failure #${calls}`);
    return returnValue;
  };
}

// ---------------------------------------------------------------------------
// Pipeline.addStep
// ---------------------------------------------------------------------------
describe("Pipeline.addStep", () => {
  test("registers steps in order", () => {
    const p = new Pipeline("test");
    p.addStep("a", makeStep());
    p.addStep("b", makeStep());
    p.addStep("c", makeStep());

    assert.equal(p.steps.length, 3);
    assert.equal(p.steps[0].name, "a");
    assert.equal(p.steps[1].name, "b");
    assert.equal(p.steps[2].name, "c");
  });

  test("returns self for chaining", () => {
    const p = new Pipeline("test");
    const returned = p.addStep("a", makeStep());
    assert.equal(returned, p);
  });
});

// ---------------------------------------------------------------------------
// Pipeline.run — basic execution
// ---------------------------------------------------------------------------
describe("Pipeline.run — basic execution", () => {
  test("successful run", async () => {
    const p = new Pipeline("basic");
    p.addStep("s1", makeStep("one"));
    p.addStep("s2", makeStep("two"));
    p.addStep("s3", makeStep("three"));

    const result = await p.run();

    assert.equal(result.status, "success");
    assert.equal(result.pipeline_name, "basic");
    assert.equal(result.steps_passed, 3);
    assert.equal(result.steps_failed, 0);
    assert.equal(result.steps_skipped, 0);
    assert.equal(result.steps.length, 3);

    for (const step of result.steps) {
      assert.equal(step.status, "success");
    }

    assert.ok(result.started_at);
    assert.ok(result.ended_at);
    assert.ok(result.duration_seconds >= 0);
  });

  test("step failure stops pipeline", async () => {
    const p = new Pipeline("fail-fast");
    p.addStep("s1", makeStep("one"));
    p.addStep("s2", makeFailingStep("step2 broke"));
    p.addStep("s3", makeStep("three"));

    const result = await p.run();

    assert.equal(result.status, "failed");
    assert.equal(result.steps_passed, 1);
    assert.equal(result.steps_failed, 1);
    assert.equal(result.steps.length, 2);
    assert.equal(result.steps[0].status, "success");
    assert.equal(result.steps[1].status, "failed");
    assert.equal(result.steps[1].error, "step2 broke");
  });

  test("empty pipeline", async () => {
    const p = new Pipeline("empty");
    const result = await p.run();

    assert.equal(result.status, "success");
    assert.deepEqual(result.steps, []);
    assert.equal(result.steps_passed, 0);
    assert.equal(result.steps_failed, 0);
    assert.equal(result.steps_skipped, 0);
  });
});

// ---------------------------------------------------------------------------
// Pipeline.run — context
// ---------------------------------------------------------------------------
describe("Pipeline.run — context", () => {
  test("context passing between steps", async () => {
    let secondSawValue;

    const step1 = async (ctx) => {
      ctx.shared_data = "hello from step1";
      return "done";
    };

    const step2 = async (ctx) => {
      secondSawValue = ctx.shared_data;
      return "done";
    };

    const p = new Pipeline("ctx-test");
    p.addStep("writer", step1);
    p.addStep("reader", step2);

    await p.run();

    assert.equal(secondSawValue, "hello from step1");
  });

  test("step return values in context", async () => {
    const p = new Pipeline("results-test");
    p.addStep("alpha", makeStep("alpha_value"));
    p.addStep("beta", makeStep("beta_value"));

    const ctx = {};
    await p.run(ctx);

    assert.equal(ctx.step_results.alpha, "alpha_value");
    assert.equal(ctx.step_results.beta, "beta_value");
  });
});

// ---------------------------------------------------------------------------
// Pipeline.run — retries
// ---------------------------------------------------------------------------
describe("Pipeline.run — retries", () => {
  test("retry succeeds", async () => {
    const p = new Pipeline("retry-ok");
    p.addStep("flaky", makeFlakyStep(1, "recovered"), { retries: 2 });

    const result = await p.run();

    assert.equal(result.status, "success");
    assert.equal(result.steps[0].status, "success");
    assert.equal(result.steps[0].attempts, 2);
    assert.equal(result.steps[0].result, "recovered");
  });

  test("retry exhausted", async () => {
    const p = new Pipeline("retry-fail");
    p.addStep("always-fail", makeFailingStep("nope"), { retries: 1 });

    const result = await p.run();

    assert.equal(result.status, "failed");
    assert.equal(result.steps[0].status, "failed");
    assert.equal(result.steps[0].attempts, 2);
    assert.equal(result.steps[0].error, "nope");
  });
});

// ---------------------------------------------------------------------------
// Pipeline.run — allow skip
// ---------------------------------------------------------------------------
describe("Pipeline.run — allow skip", () => {
  test("allowSkip continues past failure", async () => {
    const p = new Pipeline("skip-test");
    p.addStep("s1", makeStep("one"));
    p.addStep("s2", makeFailingStep("optional broke"), { allowSkip: true });
    p.addStep("s3", makeStep("three"));

    const result = await p.run();

    assert.equal(result.status, "success");
    assert.equal(result.steps_passed, 2);
    assert.equal(result.steps_skipped, 1);
    assert.equal(result.steps.length, 3);
    assert.equal(result.steps[0].status, "success");
    assert.equal(result.steps[1].status, "skipped");
    assert.equal(result.steps[2].status, "success");
  });

  test("allowSkip step that passes", async () => {
    const p = new Pipeline("skip-pass");
    p.addStep("optional-ok", makeStep("fine"), { allowSkip: true });

    const result = await p.run();

    assert.equal(result.status, "success");
    assert.equal(result.steps[0].status, "success");
    assert.equal(result.steps_skipped, 0);
  });
});

// ---------------------------------------------------------------------------
// Pipeline.run — timing
// ---------------------------------------------------------------------------
describe("Pipeline.run — timing", () => {
  test("step timing recorded", async () => {
    const p = new Pipeline("timing-step");
    p.addStep("timed", makeStep("ok"));

    const result = await p.run();
    const step = result.steps[0];

    assert.ok(step.started_at);
    assert.ok(step.ended_at);
    assert.ok(step.duration_seconds >= 0);
  });

  test("pipeline timing recorded", async () => {
    const p = new Pipeline("timing-pipeline");
    p.addStep("a", makeStep());

    const result = await p.run();

    assert.ok(result.started_at);
    assert.ok(result.ended_at);
    assert.ok(result.duration_seconds >= 0);
  });
});

// ---------------------------------------------------------------------------
// formatResult
// ---------------------------------------------------------------------------
describe("formatResult", () => {
  test("produces readable output", async () => {
    const p = new Pipeline("format_test");
    p.addStep("extract", makeStep("data"));
    p.addStep("load", makeStep("loaded"));

    const result = await p.run();
    const output = formatResult(result);

    assert.ok(typeof output === "string");
    assert.ok(output.includes("format_test"));
    assert.ok(output.includes("success"));
    assert.ok(output.includes("extract"));
    assert.ok(output.includes("load"));
    assert.ok(output.includes("PASS"));
  });
});
