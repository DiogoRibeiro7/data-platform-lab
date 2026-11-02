"""Versioned, immutable quality-alert policies with canonical fingerprints.

Operators may pin a policy JSON file in Git and override selected thresholds
explicitly on the CLI. No configuration is implicitly loaded or written.
The final *effective* policy is fingerprinted after overrides are applied.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, NoReturn, cast

_POLICY_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_MAX_CONFIG_BYTES = 65_536
_REQUIRED = frozenset({"schema_version", "name"})
_FIELDS = frozenset(
    {
        "schema_version",
        "name",
        "last",
        "max_assessment_age_hours",
        "alert_after_unhealthy",
        "max_unhealthy_share_percent",
        "max_missing_pay_increase_pp",
        "max_cumulative_missing_pay_drift_pp",
    }
)


class AlertPolicyError(ValueError):
    """A release-history alert policy is malformed, unsafe, or unsupported."""


def _finite_maximum(value: int | float | None, name: str, ceiling: float | None = None) -> None:
    """Reject booleans, negative, non-finite, out-of-range and huge inputs."""
    if value is None:
        return
    if type(value) not in {int, float} or value < 0:
        raise AlertPolicyError(f"{name} requires a finite non-negative number")
    try:
        if not math.isfinite(value):
            raise AlertPolicyError(f"{name} requires a finite non-negative number")
    except OverflowError as exc:
        raise AlertPolicyError(f"{name} requires a finite non-negative number") from exc
    if ceiling is not None and value > ceiling:
        raise AlertPolicyError(f"{name} cannot exceed {ceiling}")


@dataclass(frozen=True, slots=True)
class AlertPolicy:
    """Validated rule configuration, independent of its source or storage."""

    schema_version: int = 1
    name: str = "cli"
    last: int = 50
    max_assessment_age_hours: float | None = None
    alert_after_unhealthy: int | None = None
    max_unhealthy_share_percent: float | None = None
    max_missing_pay_increase_pp: float | None = None
    max_cumulative_missing_pay_drift_pp: float | None = None

    def __post_init__(self) -> None:
        """Fail closed on invalid values, including Python's bool-as-int edge case."""
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise AlertPolicyError("unsupported alert policy schema_version; expected 1")
        if not isinstance(self.name, str) or _POLICY_NAME.fullmatch(self.name) is None:
            raise AlertPolicyError("policy name must be a lowercase, hyphenated identifier")
        if type(self.last) is not int or not 1 <= self.last <= 1000:
            raise AlertPolicyError("last must be an integer from 1 to 1000")
        if self.alert_after_unhealthy is not None and (
            type(self.alert_after_unhealthy) is not int or self.alert_after_unhealthy < 1
        ):
            raise AlertPolicyError("alert_after_unhealthy must be a positive integer")
        _finite_maximum(self.max_assessment_age_hours, "max_assessment_age_hours")
        _finite_maximum(self.max_unhealthy_share_percent, "max_unhealthy_share_percent", 100)
        _finite_maximum(self.max_missing_pay_increase_pp, "max_missing_pay_increase_pp", 100)
        _finite_maximum(
            self.max_cumulative_missing_pay_drift_pp, "max_cumulative_missing_pay_drift_pp"
        )

    def to_dict(self) -> dict[str, Any]:
        """Return all effective values, including disabled (null) thresholds."""
        return asdict(self)

    @property
    def sha256(self) -> str:
        """SHA-256 of canonical effective JSON, unaffected by file formatting."""
        canonical = json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def with_overrides(
        self,
        *,
        last: int | None = None,
        max_assessment_age_hours: float | None = None,
        alert_after_unhealthy: int | None = None,
        max_unhealthy_share_percent: float | None = None,
        max_missing_pay_increase_pp: float | None = None,
        max_cumulative_missing_pay_drift_pp: float | None = None,
    ) -> AlertPolicy:
        """Apply only supplied CLI values; validate the complete final policy."""
        return replace(
            self,
            last=self.last if last is None else last,
            max_assessment_age_hours=(
                self.max_assessment_age_hours
                if max_assessment_age_hours is None
                else max_assessment_age_hours
            ),
            alert_after_unhealthy=(
                self.alert_after_unhealthy
                if alert_after_unhealthy is None
                else alert_after_unhealthy
            ),
            max_unhealthy_share_percent=(
                self.max_unhealthy_share_percent
                if max_unhealthy_share_percent is None
                else max_unhealthy_share_percent
            ),
            max_missing_pay_increase_pp=(
                self.max_missing_pay_increase_pp
                if max_missing_pay_increase_pp is None
                else max_missing_pay_increase_pp
            ),
            max_cumulative_missing_pay_drift_pp=(
                self.max_cumulative_missing_pay_drift_pp
                if max_cumulative_missing_pay_drift_pp is None
                else max_cumulative_missing_pay_drift_pp
            ),
        )


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject ambiguous JSON keys before they can silently override a rule."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AlertPolicyError(f"duplicate alert policy key: {key}")
        result[key] = value
    return result


def _invalid_constant(value: str) -> NoReturn:
    raise AlertPolicyError(f"invalid non-finite JSON number: {value}")


def load_alert_policy(path: Path) -> AlertPolicy:
    """Read one small, strict, versioned UTF-8 JSON file without side effects.

    Unknown or duplicate fields are errors: silently ignoring a mistyped SLA
    would make an operational alert appear configured when it is not.
    """
    if path.is_symlink():
        raise AlertPolicyError("alert policy file must not be a symlink")
    try:
        with path.open("rb") as source:
            payload = source.read(_MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise AlertPolicyError(f"cannot read alert policy file: {path}") from exc
    if len(payload) > _MAX_CONFIG_BYTES:
        raise AlertPolicyError("alert policy file exceeds 64 KiB")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_invalid_constant,
        )
    except AlertPolicyError:
        # Keep precise duplicate-key and non-finite-number diagnostics.
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise AlertPolicyError("alert policy must contain valid UTF-8 JSON") from exc
    if not isinstance(raw, dict):
        raise AlertPolicyError("alert policy must be a JSON object")
    fields = set(raw)
    unknown = fields - _FIELDS
    if unknown:
        raise AlertPolicyError(f"unknown alert policy fields: {', '.join(sorted(unknown))}")
    missing = _REQUIRED - fields
    if missing:
        raise AlertPolicyError(f"missing alert policy fields: {', '.join(sorted(missing))}")
    return AlertPolicy(**cast(dict[str, Any], raw))
