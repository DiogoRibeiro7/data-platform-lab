"""Versioned, opt-in acceptance gates for paired historical alert comparisons.

A gate evaluates observed checkpoint counts against explicit review budgets.
It neither selects a winning policy nor deploys a policy, and its pass/fail
status does not measure statistical alert accuracy or external incident truth.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast

if TYPE_CHECKING:
    from data_platform_lab.observability.ons_pay_alert_comparison import PolicyComparison

_GATE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_MAX_CONFIG_BYTES = 65_536
_REQUIRED = frozenset({"schema_version", "name"})
_FIELDS = frozenset(
    {
        "schema_version",
        "name",
        "min_evaluated",
        "max_alert_added",
        "max_became_unavailable",
        "max_alert_to_unavailable",
        "max_status_changed",
    }
)

GateStatus = Literal["pass", "fail"]
GateOperator = Literal["at_least", "at_most"]


class AlertComparisonGateError(ValueError):
    """A comparison acceptance gate file is unsafe, malformed or unsupported."""


@dataclass(frozen=True, slots=True)
class AlertComparisonGate:
    """Immutable, explicit acceptance budgets for a paired checkpoint report."""

    schema_version: int = 1
    name: str = "candidate-review"
    min_evaluated: int = 1
    max_alert_added: int | None = None
    max_became_unavailable: int | None = None
    max_alert_to_unavailable: int | None = None
    max_status_changed: int | None = None

    def __post_init__(self) -> None:
        """Reject booleans, unsupported versions and meaningless empty gates."""
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise AlertComparisonGateError("unsupported comparison gate schema_version; expected 1")
        if not isinstance(self.name, str) or _GATE_NAME.fullmatch(self.name) is None:
            raise AlertComparisonGateError("comparison gate name must be lowercase and hyphenated")
        if type(self.min_evaluated) is not int or not 1 <= self.min_evaluated <= 1000:
            raise AlertComparisonGateError("min_evaluated must be an integer from 1 to 1000")
        limits = (
            self.max_alert_added,
            self.max_became_unavailable,
            self.max_alert_to_unavailable,
            self.max_status_changed,
        )
        for name, limit in zip(
            (
                "max_alert_added",
                "max_became_unavailable",
                "max_alert_to_unavailable",
                "max_status_changed",
            ),
            limits,
            strict=True,
        ):
            if limit is not None and (type(limit) is not int or not 0 <= limit <= 1000):
                raise AlertComparisonGateError(f"{name} must be an integer from 0 to 1000")
        if all(limit is None for limit in limits) and self.min_evaluated == 1:
            raise AlertComparisonGateError("configure a maximum budget or min_evaluated above 1")

    def to_dict(self) -> dict[str, Any]:
        """Return all effective limits, including disabled (null) budgets."""
        return asdict(self)

    @property
    def sha256(self) -> str:
        """Hash canonical, effective policy-as-code independent of JSON layout."""
        payload = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class GateCheck:
    """An observed, explicit numeric budget and its advisory pass/fail state."""

    metric: str
    operator: GateOperator
    observed: int
    limit: int
    status: GateStatus


@dataclass(frozen=True, slots=True)
class GateEvaluation:
    """A reproducible gate outcome; no underlying release or archive mutation."""

    gate: AlertComparisonGate
    gate_sha256: str
    status: GateStatus
    checks: tuple[GateCheck, ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize exact configured budgets and their evaluated evidence."""
        return asdict(self)


def _check(metric: str, observed: int, limit: int, operator: GateOperator) -> GateCheck:
    """Evaluate one threshold without treating missing evidence as success."""
    passed = observed >= limit if operator == "at_least" else observed <= limit
    return GateCheck(metric, operator, observed, limit, "pass" if passed else "fail")


def evaluate_alert_comparison_gate(
    *, report: PolicyComparison, gate: AlertComparisonGate
) -> GateEvaluation:
    """Evaluate explicit review budgets on a previously verified comparison.

    The mandatory evidence check fails a zero-checkpoint comparison even if
    every configured maximum would otherwise report zero and appear to pass.
    """
    summary = report.summary
    checks = [_check("evaluated", summary.evaluated, gate.min_evaluated, "at_least")]
    optional_limits = (
        ("alert_added", summary.alert_added, gate.max_alert_added),
        ("became_unavailable", summary.became_unavailable, gate.max_became_unavailable),
        ("alert_to_unavailable", summary.alert_to_unavailable, gate.max_alert_to_unavailable),
        ("status_changed", summary.status_changed, gate.max_status_changed),
    )
    for metric, observed, maximum in optional_limits:
        if maximum is not None:
            checks.append(_check(metric, observed, maximum, "at_most"))
    return GateEvaluation(
        gate=gate,
        gate_sha256=gate.sha256,
        status="fail" if any(item.status == "fail" for item in checks) else "pass",
        checks=tuple(checks),
    )


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Do not silently accept duplicate JSON keys in acceptance budgets."""
    found: dict[str, object] = {}
    for name, value in pairs:
        if name in found:
            raise AlertComparisonGateError(f"duplicate comparison gate key: {name}")
        found[name] = value
    return found


def _invalid_constant(value: str) -> NoReturn:
    raise AlertComparisonGateError(f"invalid non-finite comparison gate number: {value}")


def load_alert_comparison_gate(path: Path) -> AlertComparisonGate:
    """Load and validate one small UTF-8 gate JSON file without side effects."""
    if path.is_symlink():
        raise AlertComparisonGateError("comparison gate file must not be a symlink")
    try:
        with path.open("rb") as handle:
            payload = handle.read(_MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise AlertComparisonGateError(f"cannot read comparison gate file: {path}") from exc
    if len(payload) > _MAX_CONFIG_BYTES:
        raise AlertComparisonGateError("comparison gate file exceeds 64 KiB")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_invalid_constant,
        )
    except AlertComparisonGateError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise AlertComparisonGateError("comparison gate must contain valid UTF-8 JSON") from exc
    if not isinstance(raw, dict):
        raise AlertComparisonGateError("comparison gate must be a JSON object")
    fields = set(raw)
    unknown = fields - _FIELDS
    if unknown:
        raise AlertComparisonGateError(
            f"unknown comparison gate fields: {', '.join(sorted(unknown))}"
        )
    missing = _REQUIRED - fields
    if missing:
        raise AlertComparisonGateError(
            f"missing comparison gate fields: {', '.join(sorted(missing))}"
        )
    return AlertComparisonGate(**cast(dict[str, Any], raw))
