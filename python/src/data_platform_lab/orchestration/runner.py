"""Orchestration runner — sequential pipeline execution engine."""

from __future__ import annotations

import datetime
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


logger = logging.getLogger(__name__)


@dataclass
class StepDefinition:
    """A registered step in the pipeline."""

    name: str
    fn: Callable[[dict[str, Any]], Any]
    retries: int = 0  # max retry attempts (0 = no retries)
    allow_skip: bool = False  # if True, failure is non-fatal


@dataclass
class StepResult:
    """Result of executing a single step."""

    name: str
    status: str  # "success", "failed", "skipped"
    started_at: str  # ISO timestamp
    ended_at: str  # ISO timestamp
    duration_seconds: float
    result: Any = None  # return value from the step function
    error: str | None = None  # error message if failed
    attempts: int = 1  # total attempts (1 = no retries needed)


@dataclass
class PipelineResult:
    """Result of executing the full pipeline."""

    pipeline_name: str
    status: str  # "success", "failed"
    started_at: str
    ended_at: str
    duration_seconds: float
    steps: list[StepResult] = field(default_factory=list)
    steps_passed: int = 0
    steps_failed: int = 0
    steps_skipped: int = 0


class Pipeline:
    """Sequential pipeline runner.

    Usage::

        pipeline = Pipeline("my_etl")
        pipeline.add_step("extract", extract_fn)
        pipeline.add_step("validate", validate_fn, retries=2)
        pipeline.add_step("transform", transform_fn)
        pipeline.add_step("load", load_fn, allow_skip=True)
        pipeline.add_step("report", report_fn)

        result = pipeline.run()
        print(format_result(result))
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._steps: list[StepDefinition] = []
        self._logger = logging.getLogger(f"{__name__}.{name}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def steps(self) -> list[StepDefinition]:
        return list(self._steps)

    def add_step(
        self,
        name: str,
        fn: Callable[[dict[str, Any]], Any],
        *,
        retries: int = 0,
        allow_skip: bool = False,
    ) -> Pipeline:
        """Register a step. Returns self for chaining."""
        self._steps.append(
            StepDefinition(name=name, fn=fn, retries=retries, allow_skip=allow_skip)
        )
        return self

    def run(self, context: dict[str, Any] | None = None) -> PipelineResult:
        """Execute all registered steps in order.

        Each step receives the shared *context* dict.  A step's return value
        is stored in ``context["step_results"][step_name]``.

        If a step fails and ``allow_skip`` is ``False``, execution stops
        immediately (fail-fast).  If ``allow_skip`` is ``True``, the step is
        marked ``"skipped"`` and execution continues.

        When ``retries > 0``, the step is retried up to that many additional
        times before being considered failed.
        """
        pipeline_start = datetime.datetime.now(datetime.UTC)
        pipeline_clock = time.perf_counter()

        if context is None:
            context = {}
        context["pipeline_name"] = self._name
        context["step_results"] = {}

        step_results: list[StepResult] = []
        pipeline_failed = False

        for step in self._steps:
            step_start = datetime.datetime.now(datetime.UTC)
            step_clock = time.perf_counter()

            last_error: Exception | None = None
            result_value: Any = None
            max_attempts = 1 + step.retries
            attempts = 0
            succeeded = False

            for attempt in range(1, max_attempts + 1):
                attempts = attempt
                try:
                    self._logger.debug(
                        "Running step %r (attempt %d/%d)",
                        step.name,
                        attempt,
                        max_attempts,
                    )
                    result_value = step.fn(context)
                    succeeded = True
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    self._logger.debug(
                        "Step %r attempt %d failed: %s",
                        step.name,
                        attempt,
                        exc,
                    )

            step_end = datetime.datetime.now(datetime.UTC)
            step_duration = time.perf_counter() - step_clock

            if succeeded:
                context["step_results"][step.name] = result_value
                step_results.append(
                    StepResult(
                        name=step.name,
                        status="success",
                        started_at=step_start.isoformat(),
                        ended_at=step_end.isoformat(),
                        duration_seconds=step_duration,
                        result=result_value,
                        attempts=attempts,
                    )
                )
                self._logger.info("Step %r succeeded", step.name)
            else:
                error_msg = str(last_error)
                if step.allow_skip:
                    step_results.append(
                        StepResult(
                            name=step.name,
                            status="skipped",
                            started_at=step_start.isoformat(),
                            ended_at=step_end.isoformat(),
                            duration_seconds=step_duration,
                            error=error_msg,
                            attempts=attempts,
                        )
                    )
                    self._logger.warning(
                        "Step %r failed but is skippable: %s",
                        step.name,
                        error_msg,
                    )
                else:
                    step_results.append(
                        StepResult(
                            name=step.name,
                            status="failed",
                            started_at=step_start.isoformat(),
                            ended_at=step_end.isoformat(),
                            duration_seconds=step_duration,
                            error=error_msg,
                            attempts=attempts,
                        )
                    )
                    self._logger.error(
                        "Step %r failed: %s", step.name, error_msg
                    )
                    pipeline_failed = True
                    break

        pipeline_end = datetime.datetime.now(datetime.UTC)
        pipeline_duration = time.perf_counter() - pipeline_clock

        steps_passed = sum(1 for s in step_results if s.status == "success")
        steps_failed = sum(1 for s in step_results if s.status == "failed")
        steps_skipped = sum(1 for s in step_results if s.status == "skipped")

        return PipelineResult(
            pipeline_name=self._name,
            status="failed" if pipeline_failed else "success",
            started_at=pipeline_start.isoformat(),
            ended_at=pipeline_end.isoformat(),
            duration_seconds=pipeline_duration,
            steps=step_results,
            steps_passed=steps_passed,
            steps_failed=steps_failed,
            steps_skipped=steps_skipped,
        )


def format_result(result: PipelineResult) -> str:
    """Format a :class:`PipelineResult` as a human-readable string."""
    total = len(result.steps)
    lines: list[str] = [
        f"=== Pipeline: {result.pipeline_name} ===",
        f"Status: {result.status}",
        f"Duration: {result.duration_seconds:.2f}s",
        (
            f"Steps: {total} total | {result.steps_passed} passed"
            f" | {result.steps_failed} failed"
            f" | {result.steps_skipped} skipped"
        ),
        "",
    ]

    for step in result.steps:
        if step.status == "success":
            tag = "PASS"
        elif step.status == "skipped":
            tag = "SKIP"
        else:
            tag = "FAIL"

        detail = f"{step.duration_seconds:.2f}s"
        if step.attempts > 1:
            detail += f", {step.attempts} attempts"

        line = f"  [{tag}] {step.name} ({detail})"
        if step.error is not None:
            line += f" \u2014 {step.error}"
        lines.append(line)

    return "\n".join(lines)
