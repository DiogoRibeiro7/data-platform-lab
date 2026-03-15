"""Tests for the orchestration pipeline runner."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from data_platform_lab.orchestration.runner import (
    Pipeline,
    PipelineResult,
    StepResult,
    format_result,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def make_step(return_value: Any = "ok") -> Callable[[dict[str, Any]], Any]:
    """Return a step function that succeeds with the given return value."""

    def step(ctx: dict[str, Any]) -> Any:
        return return_value

    return step


def make_failing_step(error_msg: str = "boom") -> Callable[[dict[str, Any]], Any]:
    """Return a step function that always raises."""

    def step(ctx: dict[str, Any]) -> Any:
        raise RuntimeError(error_msg)

    return step


def make_flaky_step(
    fail_count: int, return_value: Any = "recovered"
) -> Callable[[dict[str, Any]], Any]:
    """Return a step that fails *fail_count* times then succeeds."""
    calls: dict[str, int] = {"count": 0}

    def step(ctx: dict[str, Any]) -> Any:
        calls["count"] += 1
        if calls["count"] <= fail_count:
            raise RuntimeError(f"flaky failure #{calls['count']}")
        return return_value

    return step


# ===================================================================
# Pipeline execution — basic behaviour
# ===================================================================


class TestSuccessfulRun:
    """Pipeline with all steps passing."""

    def test_successful_run(self) -> None:
        pipeline = Pipeline("basic")
        pipeline.add_step("a", make_step("val_a"))
        pipeline.add_step("b", make_step("val_b"))
        pipeline.add_step("c", make_step("val_c"))

        result = pipeline.run()

        assert result.status == "success"
        assert result.steps_passed == 3
        assert result.steps_failed == 0
        assert result.steps_skipped == 0
        assert len(result.steps) == 3
        assert all(s.status == "success" for s in result.steps)
        assert result.started_at
        assert result.ended_at
        assert result.duration_seconds >= 0


class TestStepFailureStopsPipeline:
    """Fail-fast: second step fails, third never runs."""

    def test_step_failure_stops_pipeline(self) -> None:
        pipeline = Pipeline("fail_fast")
        pipeline.add_step("first", make_step())
        pipeline.add_step("bad", make_failing_step("step 2 error"))
        pipeline.add_step("never", make_step())

        result = pipeline.run()

        assert result.status == "failed"
        assert result.steps_passed == 1
        assert result.steps_failed == 1
        assert len(result.steps) == 2  # third step never ran
        assert result.steps[0].status == "success"
        assert result.steps[1].status == "failed"
        assert result.steps[1].error == "step 2 error"


class TestEmptyPipeline:
    """A pipeline with no steps should succeed immediately."""

    def test_empty_pipeline(self) -> None:
        pipeline = Pipeline("empty")
        result = pipeline.run()

        assert result.status == "success"
        assert result.steps == []
        assert result.steps_passed == 0
        assert result.steps_failed == 0
        assert result.steps_skipped == 0


# ===================================================================
# Context sharing
# ===================================================================


class TestContextPassingBetweenSteps:
    """Steps can communicate through the shared context dict."""

    def test_context_passing_between_steps(self) -> None:
        def writer(ctx: dict[str, Any]) -> str:
            ctx["shared_key"] = "hello from writer"
            return "wrote"

        captured: dict[str, Any] = {}

        def reader(ctx: dict[str, Any]) -> str:
            captured["value"] = ctx["shared_key"]
            return "read"

        pipeline = Pipeline("ctx")
        pipeline.add_step("writer", writer)
        pipeline.add_step("reader", reader)

        result = pipeline.run()

        assert result.status == "success"
        assert captured["value"] == "hello from writer"


class TestStepReturnValuesInContext:
    """Return values are stored in context['step_results']."""

    def test_step_return_values_in_context(self) -> None:
        ctx: dict[str, Any] = {}
        pipeline = Pipeline("returns")
        pipeline.add_step("alpha", make_step(42))
        pipeline.add_step("beta", make_step([1, 2, 3]))

        pipeline.run(context=ctx)

        assert ctx["step_results"]["alpha"] == 42
        assert ctx["step_results"]["beta"] == [1, 2, 3]


# ===================================================================
# Retry logic
# ===================================================================


class TestRetrySucceeds:
    """A flaky step recovers within the allowed retry count."""

    def test_retry_succeeds(self) -> None:
        pipeline = Pipeline("retry_ok")
        pipeline.add_step("flaky", make_flaky_step(1, "recovered"), retries=2)

        result = pipeline.run()

        assert result.status == "success"
        assert len(result.steps) == 1
        step = result.steps[0]
        assert step.status == "success"
        assert step.attempts == 2  # 1 fail + 1 success
        assert step.result == "recovered"


class TestRetryExhausted:
    """A step that always fails exhausts its retries."""

    def test_retry_exhausted(self) -> None:
        pipeline = Pipeline("retry_fail")
        pipeline.add_step("always_bad", make_failing_step("nope"), retries=1)

        result = pipeline.run()

        assert result.status == "failed"
        assert len(result.steps) == 1
        step = result.steps[0]
        assert step.status == "failed"
        assert step.attempts == 2  # 1 initial + 1 retry
        assert step.error == "nope"


# ===================================================================
# allow_skip
# ===================================================================


class TestAllowSkip:
    """A skippable step that fails does not stop the pipeline."""

    def test_allow_skip(self) -> None:
        pipeline = Pipeline("skip")
        pipeline.add_step("first", make_step("a"))
        pipeline.add_step("optional", make_failing_step("not critical"), allow_skip=True)
        pipeline.add_step("last", make_step("c"))

        result = pipeline.run()

        assert result.status == "success"
        assert result.steps_passed == 2
        assert result.steps_skipped == 1
        assert result.steps_failed == 0
        assert len(result.steps) == 3
        assert result.steps[0].status == "success"
        assert result.steps[1].status == "skipped"
        assert result.steps[1].error == "not critical"
        assert result.steps[2].status == "success"


class TestAllowSkipWithAllPassing:
    """allow_skip does not force a skip when the step succeeds."""

    def test_allow_skip_with_all_passing(self) -> None:
        pipeline = Pipeline("skip_pass")
        pipeline.add_step("safe", make_step("fine"), allow_skip=True)

        result = pipeline.run()

        assert result.status == "success"
        assert result.steps[0].status == "success"
        assert result.steps_skipped == 0


# ===================================================================
# Timing
# ===================================================================


class TestStepTiming:
    """Individual step timing fields are populated."""

    def test_step_timing(self) -> None:
        pipeline = Pipeline("timing")
        pipeline.add_step("noop", make_step())

        result = pipeline.run()
        step = result.steps[0]

        assert step.started_at
        assert step.ended_at
        assert step.duration_seconds >= 0


class TestPipelineTiming:
    """Pipeline-level timing fields are populated."""

    def test_pipeline_timing(self) -> None:
        pipeline = Pipeline("timing")
        pipeline.add_step("noop", make_step())

        result = pipeline.run()

        assert result.started_at
        assert result.ended_at
        assert result.duration_seconds >= 0


# ===================================================================
# API — chaining
# ===================================================================


class TestAddStepReturnsSelf:
    """add_step returns the Pipeline instance for fluent chaining."""

    def test_add_step_returns_self(self) -> None:
        pipeline = Pipeline("chain")
        ret = pipeline.add_step("a", make_step())

        assert ret is pipeline


# ===================================================================
# format_result
# ===================================================================


class TestFormatResult:
    """format_result produces a readable summary string."""

    def test_format_result(self) -> None:
        pipeline = Pipeline("my_etl")
        pipeline.add_step("extract", make_step())
        pipeline.add_step("transform", make_step())
        pipeline.add_step("load", make_step())

        result = pipeline.run()
        text = format_result(result)

        assert "Pipeline: my_etl" in text
        assert "success" in text
        assert "extract" in text
        assert "transform" in text
        assert "load" in text
        assert "[PASS]" in text

    def test_format_result_with_failures(self) -> None:
        pipeline = Pipeline("mixed")
        pipeline.add_step("good", make_step())
        pipeline.add_step("bad", make_failing_step("oops"), allow_skip=True)
        pipeline.add_step("also_good", make_step())

        result = pipeline.run()
        text = format_result(result)

        assert "[PASS]" in text
        assert "[SKIP]" in text
        assert "oops" in text
