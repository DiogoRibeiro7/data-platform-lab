"""Run metadata persistence boundary."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from data_platform_lab.observability import RunMetadata


@runtime_checkable
class RunStore(Protocol):
    """Minimal durable history contract for pipeline runs."""

    def save(self, metadata: RunMetadata) -> None:
        """Insert or replace one run snapshot."""

    def get(self, pipeline_name: str, run_id: str) -> RunMetadata | None:
        """Return one run snapshot if present."""

    def list_recent(self, limit: int = 20) -> list[RunMetadata]:
        """Return recent runs ordered newest first."""
