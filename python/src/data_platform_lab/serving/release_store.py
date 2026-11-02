"""Operator-controlled, content-addressed ONS pay release decisions.

An immutable event is durably published before a single active-pointer rename.
An interrupted write can leave an unused event but cannot expose a pointer to
an absent event. The local filesystem is trusted; these are not signatures or
authenticated human approvals.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from data_platform_lab.curation.ons_pay import MODEL_VERSION
from data_platform_lab.serving.catalog import (
    CatalogError,
    CuratedCatalog,
    VintageKey,
    VintageNotFound,
)
from data_platform_lab.serving.ons_pay_compare import (
    ComparisonError,
    ComparisonReport,
    compare_ons_pay_vintages,
)

_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_POLICIES = frozenset(
    {
        "added_rows",
        "removed_rows",
        "revised_rows",
        "newly_missing_pay_rows",
        "missing_pay_increase_pp",
        "absolute_pay_revision_gbp",
    }
)

Action = Literal["bootstrap", "promote", "rollback"]


class ReleaseError(ValueError):
    """Invalid release evidence, expected generation, or local publication state."""


@dataclass(frozen=True, slots=True)
class ReleaseEvent:
    generation: int
    action: Action
    created_at_utc: str
    from_vintage: VintageKey | None
    to_vintage: VintageKey
    previous_event_sha256: str | None
    reason: str
    comparison: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        """Canonical event body (without its derived content-addressed hash)."""
        return {"schema_version": 1, "pipeline_name": "ons_pay_release", **asdict(self)}


@dataclass(frozen=True, slots=True)
class ReleaseState:
    generation: int
    active: VintageKey
    event_sha256: str
    events: tuple[ReleaseEvent, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReleasePlan:
    action: Action
    expected_generation: int
    baseline: VintageKey | None
    candidate: VintageKey
    eligible: bool
    applied: bool
    comparison: ComparisonReport | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _mapping(value: object, name: str, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ReleaseError(f"invalid {name} fields in release history")
    if not all(isinstance(field, str) for field in value):
        raise ReleaseError(f"invalid {name} keys in release history")
    return cast(dict[str, object], value)


def _key(value: object, name: str) -> VintageKey:
    raw = _mapping(value, name, {"model_version", "registry_commit", "source_sha256"})
    version = raw["model_version"]
    commit = raw["registry_commit"]
    sha = raw["source_sha256"]
    if (
        type(version) is not int
        or version != MODEL_VERSION
        or not isinstance(commit, str)
        or not _COMMIT.fullmatch(commit)
        or not isinstance(sha, str)
        or not _HASH.fullmatch(sha)
    ):
        raise ReleaseError(f"invalid exact {name} version/commit/hash")
    return VintageKey(version, commit, sha)


def _generation(value: object) -> int:
    if type(value) is not int or value < 1:
        raise ReleaseError("release generation must be a positive integer")
    return value


def _sha(value: object, name: str) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise ReleaseError(f"invalid {name} in release history")
    return value


def _event(value: object) -> ReleaseEvent:
    raw = _mapping(
        value,
        "release event",
        {
            "schema_version",
            "pipeline_name",
            "generation",
            "action",
            "created_at_utc",
            "from_vintage",
            "to_vintage",
            "previous_event_sha256",
            "reason",
            "comparison",
        },
    )
    if raw["schema_version"] != 1 or raw["pipeline_name"] != "ons_pay_release":
        raise ReleaseError("unknown release event schema")
    action = raw["action"]
    if action not in {"bootstrap", "promote", "rollback"}:
        raise ReleaseError("unknown release action")
    stamp = raw["created_at_utc"]
    if not isinstance(stamp, str):
        raise ReleaseError("invalid release timestamp")
    try:
        timestamp = datetime.fromisoformat(stamp)
    except ValueError as exc:
        raise ReleaseError("invalid release timestamp") from exc
    offset = timestamp.utcoffset()
    if timestamp.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ReleaseError("release timestamp must be UTC")
    reason = raw["reason"]
    if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
        raise ReleaseError("invalid recorded release reason")
    from_value = raw["from_vintage"]
    previous = raw["previous_event_sha256"]
    comparison = raw["comparison"]
    if previous is not None:
        previous = _sha(previous, "previous event SHA")
    if comparison is not None and not isinstance(comparison, dict):
        raise ReleaseError("invalid release comparison evidence")
    return ReleaseEvent(
        generation=_generation(raw["generation"]),
        action=action,
        created_at_utc=stamp,
        from_vintage=None if from_value is None else _key(from_value, "previous vintage"),
        to_vintage=_key(raw["to_vintage"], "active vintage"),
        previous_event_sha256=previous,
        reason=reason,
        comparison=cast(dict[str, Any] | None, comparison),
    )


def _check_evidence(event: ReleaseEvent) -> None:
    if event.action != "promote":
        if event.comparison is not None:
            raise ReleaseError("bootstrap/rollback must not claim comparison evidence")
        return
    if event.comparison is None or event.from_vintage is None:
        raise ReleaseError("promotion lacks comparison evidence")
    evidence = event.comparison
    if (
        evidence.get("status") != "pass"
        or evidence.get("baseline") != asdict(event.from_vintage)
        or evidence.get("candidate") != asdict(event.to_vintage)
    ):
        raise ReleaseError("promotion evidence contradicts release pins")
    checks = evidence.get("policy_checks")
    if not isinstance(checks, list) or len(checks) != len(_POLICIES):
        raise ReleaseError("promotion lacks six explicit quality policies")
    names: set[str] = set()
    for check in checks:
        if not isinstance(check, dict):
            raise ReleaseError("invalid promotion policy evidence")
        name = check.get("name")
        if (
            not isinstance(name, str)
            or name in names
            or name not in _POLICIES
            or check.get("state") != "pass"
            or type(check.get("maximum")) not in {int, float}
        ):
            raise ReleaseError("promotion policy is unconfigured or failed")
        names.add(name)
    if names != _POLICIES:
        raise ReleaseError("missing promotion policy evidence")


def _read_json(path: Path, name: str) -> tuple[dict[str, object], bytes]:
    try:
        if path.resolve(strict=True) != path or not path.is_file():
            raise ReleaseError(f"unsafe {name} path")
        raw = path.read_bytes()
        value = json.loads(raw)
        if raw != _canonical(value) or not isinstance(value, dict):
            raise ReleaseError(f"noncanonical {name} JSON")
    except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
        raise ReleaseError(f"cannot read valid {name}") from exc
    return cast(dict[str, object], value), raw


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ReleaseStore:
    """Single active pointer with verified SHA-linked, immutable release events."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root.resolve()
        self.root = self.data_root / "releases" / "ons-pay"
        self.events = self.root / "events"
        self.head = self.root / "active.json"

    def _check_paths(self) -> None:
        for path in (self.root.parent, self.root, self.events):
            if path.is_symlink() or (path.exists() and not path.is_dir()):
                raise ReleaseError("unsafe release directory")
            if path.exists() and path.resolve() != path:
                raise ReleaseError("release directory contains symlinks")
        if self.head.is_symlink():
            raise ReleaseError("symlinked active release pointer")

    def _read_event(self, sha: str) -> ReleaseEvent:
        path = self.events / f"{sha}.json"
        raw, payload = _read_json(path, "release event")
        if hashlib.sha256(payload).hexdigest() != sha:
            raise ReleaseError("content-addressed release event hash mismatch")
        return _event(raw)

    def _load(self) -> ReleaseState | None:
        self._check_paths()
        if not self.head.exists():
            return None
        raw, _ = _read_json(self.head, "active release pointer")
        pointer = _mapping(
            raw,
            "active release pointer",
            {"schema_version", "pipeline_name", "generation", "active", "event_sha256"},
        )
        if pointer["schema_version"] != 1 or pointer["pipeline_name"] != "ons_pay_release":
            raise ReleaseError("unknown active release schema")
        generation = _generation(pointer["generation"])
        active = _key(pointer["active"], "active vintage")
        head_sha = _sha(pointer["event_sha256"], "active event SHA")

        expected_generation = generation
        expected_vintage: VintageKey | None = active
        sha: str | None = head_sha
        newest_first: list[ReleaseEvent] = []
        while sha is not None:
            event = self._read_event(sha)
            if event.generation != expected_generation or event.to_vintage != expected_vintage:
                raise ReleaseError("release event chain contradicts the active pointer")
            _check_evidence(event)
            newest_first.append(event)
            expected_generation -= 1
            expected_vintage = event.from_vintage
            sha = event.previous_event_sha256
        if expected_generation != 0 or expected_vintage is not None:
            raise ReleaseError("release history has missing or disconnected events")

        events = tuple(reversed(newest_first))
        for index, event in enumerate(events):
            if index == 0:
                if (
                    event.action != "bootstrap"
                    or event.from_vintage is not None
                    or event.previous_event_sha256 is not None
                ):
                    raise ReleaseError("first release event must be a bootstrap")
            elif event.action == "bootstrap":
                raise ReleaseError("bootstrap cannot follow an existing release")
            elif event.action == "rollback" and event.to_vintage not in {
                previous.to_vintage for previous in events[:index]
            }:
                raise ReleaseError("rollback target was not previously active")
        return ReleaseState(generation, active, head_sha, events)

    def _verify_curated(self, key: VintageKey) -> None:
        try:
            vintage = CuratedCatalog(self.data_root).get_vintage(key)
            CuratedCatalog.verify_vintage_tables(vintage)
        except (CatalogError, VintageNotFound) as exc:
            raise ReleaseError("active release does not have trusted curated data") from exc

    def history(self) -> ReleaseState | None:
        """Validate release-event provenance without asserting curated-data health.

        Monitoring may need to inspect the active pin after its Parquet fails.
        This read-only method authenticates the local event hash chain, not the
        bytes or schema of the curated data selected by that chain.
        """
        return self._load()

    def status(self) -> ReleaseState | None:
        """Read and validate history plus currently active curated data; no writes."""
        state = self.history()
        if state is not None:
            self._verify_curated(state.active)
        return state

    @staticmethod
    def _expected(value: int) -> None:
        if type(value) is not int or value < 0:
            raise ReleaseError("expected_generation must be a non-negative integer")

    @staticmethod
    def _reason(value: str) -> None:
        if not isinstance(value, str) or not 1 <= len(value.strip()) <= 500:
            raise ReleaseError("reason must contain 1 to 500 nonblank characters")

    def _plan_activate(
        self,
        *,
        state: ReleaseState | None,
        candidate: VintageKey,
        expected_generation: int,
        bootstrap: bool,
        limits: dict[str, int | float | None],
    ) -> ReleasePlan:
        self._expected(expected_generation)
        current = state.generation if state is not None else 0
        if expected_generation != current:
            raise ReleaseError("stale expected release generation")
        if bootstrap:
            if state is not None or expected_generation != 0:
                raise ReleaseError("bootstrap is only valid before the first activation")
            if any(value is not None for value in limits.values()):
                raise ReleaseError("bootstrap has no baseline; omit comparison policy limits")
            self._verify_curated(candidate)
            return ReleasePlan("bootstrap", current, None, candidate, True, False, None)
        if state is None:
            raise ReleaseError("first activation requires explicit --bootstrap")
        if candidate == state.active:
            raise ReleaseError("requested candidate is already active")
        if any(value is None for value in limits.values()):
            raise ReleaseError("promotion requires all six explicit comparison limits")
        try:
            report = compare_ons_pay_vintages(
                data_root=self.data_root,
                baseline=state.active,
                candidate=candidate,
                max_added_rows=cast(int, limits["max_added_rows"]),
                max_removed_rows=cast(int, limits["max_removed_rows"]),
                max_revised_rows=cast(int, limits["max_revised_rows"]),
                max_newly_missing_pay_rows=cast(int, limits["max_newly_missing_pay_rows"]),
                max_missing_pay_increase_pp=cast(float, limits["max_missing_pay_increase_pp"]),
                max_absolute_pay_revision_gbp=cast(float, limits["max_absolute_pay_revision_gbp"]),
            )
        except ComparisonError as exc:
            raise ReleaseError("candidate release comparison cannot be trusted") from exc
        return ReleasePlan(
            "promote", current, state.active, candidate, report.status == "pass", False, report
        )

    def _plan_rollback(
        self, *, state: ReleaseState | None, to_generation: int, expected_generation: int
    ) -> ReleasePlan:
        self._expected(expected_generation)
        if state is None or expected_generation != state.generation:
            raise ReleaseError("no matching active generation for rollback")
        if type(to_generation) is not int or not 1 <= to_generation < state.generation:
            raise ReleaseError("rollback must target an earlier positive generation")
        target = state.events[to_generation - 1].to_vintage
        if target == state.active:
            raise ReleaseError("rollback target is already active")
        self._verify_curated(target)
        return ReleasePlan("rollback", state.generation, state.active, target, True, False, None)

    def plan_activate(
        self,
        *,
        candidate: VintageKey,
        expected_generation: int,
        bootstrap: bool = False,
        max_added_rows: int | None = None,
        max_removed_rows: int | None = None,
        max_revised_rows: int | None = None,
        max_newly_missing_pay_rows: int | None = None,
        max_missing_pay_increase_pp: float | None = None,
        max_absolute_pay_revision_gbp: float | None = None,
    ) -> ReleasePlan:
        """Preview a pinned activation without a lock file or filesystem write."""
        limits: dict[str, int | float | None] = {
            "max_added_rows": max_added_rows,
            "max_removed_rows": max_removed_rows,
            "max_revised_rows": max_revised_rows,
            "max_newly_missing_pay_rows": max_newly_missing_pay_rows,
            "max_missing_pay_increase_pp": max_missing_pay_increase_pp,
            "max_absolute_pay_revision_gbp": max_absolute_pay_revision_gbp,
        }
        state = self.status()
        return self._plan_activate(
            state=state,
            candidate=candidate,
            expected_generation=expected_generation,
            bootstrap=bootstrap,
            limits=limits,
        )

    def plan_rollback(self, *, to_generation: int, expected_generation: int) -> ReleasePlan:
        """Preview a rollback to a previously active, still-verified vintage."""
        return self._plan_rollback(
            state=self.status(),
            to_generation=to_generation,
            expected_generation=expected_generation,
        )

    def _ensure_directories(self) -> None:
        self._check_paths()
        for path in (self.root.parent, self.root, self.events):
            try:
                path.mkdir(exist_ok=True)
            except OSError as exc:
                raise ReleaseError("cannot create release metadata directories") from exc
        self._check_paths()

    @contextmanager
    def _locked(self) -> Iterator[None]:
        """Serialize local writers, including validation and head replacement."""
        try:
            import fcntl
        except ModuleNotFoundError as exc:
            raise ReleaseError("release activation requires a POSIX filesystem lock") from exc
        self._ensure_directories()
        path = self.root / ".release.lock"
        try:
            fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                os.close(fd)
                raise ReleaseError("release lock is not a regular file")
        except OSError as exc:
            raise ReleaseError("cannot open release lock") from exc
        with os.fdopen(fd, "rb") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                yield
            except OSError as exc:
                raise ReleaseError("release lock or publication failed") from exc
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def locked_history(self) -> Iterator[ReleaseState | None]:
        """Hold the release writer lock while inspecting history for an opt-in write.

        A recorded health assessment must not be published against a release
        pointer that another local writer switches during the publication.
        This guard creates a lock file and must not be used by read-only paths.
        """
        with self._locked():
            yield self.history()

    def _publish_event(self, payload: bytes, sha: str) -> None:
        target = self.events / f"{sha}.json"
        if target.exists() or target.is_symlink():
            existing, raw = _read_json(target, "existing release event")
            if hashlib.sha256(raw).hexdigest() != sha or _canonical(existing) != payload:
                raise ReleaseError("content-addressed event conflict")
            return
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.events, prefix=".release-", suffix=".tmp", delete=False
            ) as output:
                temporary = Path(output.name)
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            # Hard-link creation refuses to overwrite a preexisting immutable event.
            os.link(temporary, target)
            _sync_directory(self.events)
        except OSError as exc:
            raise ReleaseError("cannot durably publish release event") from exc
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _publish_head(self, state: ReleaseState) -> None:
        body = _canonical(
            {
                "schema_version": 1,
                "pipeline_name": "ons_pay_release",
                "generation": state.generation,
                "active": asdict(state.active),
                "event_sha256": state.event_sha256,
            }
        )
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.root, prefix=".active-", suffix=".tmp", delete=False
            ) as output:
                temporary = Path(output.name)
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            if self.head.is_symlink():
                raise ReleaseError("active pointer became a symlink")
            os.replace(temporary, self.head)
            _sync_directory(self.root)
        except OSError as exc:
            raise ReleaseError("cannot atomically publish active release") from exc
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _apply(self, plan: ReleasePlan, state: ReleaseState | None, reason: str) -> ReleasePlan:
        if not plan.eligible:
            return plan
        event = ReleaseEvent(
            generation=plan.expected_generation + 1,
            action=plan.action,
            created_at_utc=datetime.now(UTC).isoformat(),
            from_vintage=plan.baseline,
            to_vintage=plan.candidate,
            previous_event_sha256=state.event_sha256 if state is not None else None,
            reason=reason,
            comparison=plan.comparison.to_dict() if plan.comparison is not None else None,
        )
        body = _canonical(event.to_dict())
        sha = hashlib.sha256(body).hexdigest()
        self._publish_event(body, sha)
        new_state = ReleaseState(
            event.generation,
            event.to_vintage,
            sha,
            (*state.events, event) if state is not None else (event,),
        )
        self._publish_head(new_state)
        return ReleasePlan(
            plan.action,
            plan.expected_generation,
            plan.baseline,
            plan.candidate,
            True,
            True,
            plan.comparison,
        )

    def activate(
        self,
        *,
        candidate: VintageKey,
        expected_generation: int,
        reason: str,
        bootstrap: bool = False,
        max_added_rows: int | None = None,
        max_removed_rows: int | None = None,
        max_revised_rows: int | None = None,
        max_newly_missing_pay_rows: int | None = None,
        max_missing_pay_increase_pp: float | None = None,
        max_absolute_pay_revision_gbp: float | None = None,
    ) -> ReleasePlan:
        """Explicitly apply a fully rechecked release under an exclusive lock."""
        self._reason(reason)
        limits: dict[str, int | float | None] = {
            "max_added_rows": max_added_rows,
            "max_removed_rows": max_removed_rows,
            "max_revised_rows": max_revised_rows,
            "max_newly_missing_pay_rows": max_newly_missing_pay_rows,
            "max_missing_pay_increase_pp": max_missing_pay_increase_pp,
            "max_absolute_pay_revision_gbp": max_absolute_pay_revision_gbp,
        }
        preflight = self._plan_activate(
            state=self.status(),
            candidate=candidate,
            expected_generation=expected_generation,
            bootstrap=bootstrap,
            limits=limits,
        )
        if not preflight.eligible:
            return preflight
        with self._locked():
            state = self.status()
            plan = self._plan_activate(
                state=state,
                candidate=candidate,
                expected_generation=expected_generation,
                bootstrap=bootstrap,
                limits=limits,
            )
            return self._apply(plan, state, reason)

    def rollback(self, *, to_generation: int, expected_generation: int, reason: str) -> ReleasePlan:
        """Explicitly reactivate an earlier, still-verified release under lock."""
        self._reason(reason)
        self._plan_rollback(
            state=self.status(),
            to_generation=to_generation,
            expected_generation=expected_generation,
        )
        with self._locked():
            state = self.status()
            plan = self._plan_rollback(
                state=state, to_generation=to_generation, expected_generation=expected_generation
            )
            return self._apply(plan, state, reason)
