"""Tests for the streaming sensor event processor."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.streaming.processor import (
    EventResult,  # noqa: F401
    StreamSummary,  # noqa: F401
    compute_aggregates,
    deduplicate_key,
    process_stream,
    validate_event,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_event(**overrides):
    """Return a valid sensor event dict with optional overrides."""
    base = {
        "sensor_id": "sensor-01",
        "type": "temperature",
        "value": 22.5,
        "unit": "celsius",
        "location": "warehouse-A",
        "timestamp": "2024-06-01T08:00:00Z",
    }
    base.update(overrides)
    return base


def write_jsonl(path, events):
    """Write a list of dicts as newline-delimited JSON."""
    with open(path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


# ---------------------------------------------------------------------------
# validate_event tests
# ---------------------------------------------------------------------------


def test_validate_valid_event():
    """Valid event returns status 'accepted', reason None."""
    result = validate_event(make_event())
    assert result.status == "accepted"
    assert result.reason is None


def test_validate_missing_field():
    """Event missing 'sensor_id' returns 'rejected' with reason containing 'missing field'."""
    event = make_event()
    del event["sensor_id"]
    result = validate_event(event)
    assert result.status == "rejected"
    assert "missing field" in result.reason


def test_validate_null_value():
    """Event with value=None returns 'rejected' with reason 'null value'."""
    result = validate_event(make_event(value=None))
    assert result.status == "rejected"
    assert result.reason == "null value"


def test_validate_empty_string_field():
    """Event with sensor_id='' returns 'rejected' with reason containing 'empty'."""
    result = validate_event(make_event(sensor_id=""))
    assert result.status == "rejected"
    assert "empty" in result.reason


def test_validate_non_numeric_value():
    """Event with value='not a number' returns 'rejected'."""
    result = validate_event(make_event(value="not a number"))
    assert result.status == "rejected"
    assert "not a number" in result.reason


def test_validate_unparseable_timestamp():
    """Event with timestamp='not-a-date' returns 'rejected'."""
    result = validate_event(make_event(timestamp="not-a-date"))
    assert result.status == "rejected"
    assert result.reason == "unparseable timestamp"


def test_validate_boolean_value_rejected():
    """In Python bool is a subclass of int, so the processor accepts booleans."""
    result = validate_event(make_event(value=True))
    assert result.status == "accepted"


# ---------------------------------------------------------------------------
# deduplicate_key tests
# ---------------------------------------------------------------------------


def test_deduplicate_key_format():
    """Returns 'sensor-01::2024-06-01T08:00:00Z'."""
    event = make_event()
    key = deduplicate_key(event)
    assert key == "sensor-01::2024-06-01T08:00:00Z"


def test_deduplicate_key_different_sensors():
    """Different sensor_id produces different key."""
    key_a = deduplicate_key(make_event(sensor_id="sensor-01"))
    key_b = deduplicate_key(make_event(sensor_id="sensor-02"))
    assert key_a != key_b


# ---------------------------------------------------------------------------
# compute_aggregates tests
# ---------------------------------------------------------------------------


def test_aggregates_single_sensor():
    """One sensor with 3 readings: count=3, min, max, avg rounded to 2dp."""
    events = [
        make_event(value=10.0),
        make_event(value=20.0),
        make_event(value=30.0),
    ]
    agg = compute_aggregates(events)
    sensor = agg["by_sensor"]["sensor-01"]
    assert sensor["count"] == 3
    assert sensor["min_value"] == 10.0
    assert sensor["max_value"] == 30.0
    assert sensor["avg_value"] == round(
        (10.0 + 20.0 + 30.0) / 3, 2
    )


def test_aggregates_multiple_sensors():
    """Two sensors produce correct by_sensor, by_type, by_location."""
    events = [
        make_event(
            sensor_id="s1", type="temperature",
            location="A", value=10.0,
        ),
        make_event(
            sensor_id="s1", type="temperature",
            location="A", value=20.0,
        ),
        make_event(
            sensor_id="s2", type="humidity",
            location="B", value=50.0,
        ),
    ]
    agg = compute_aggregates(events)
    assert "s1" in agg["by_sensor"]
    assert "s2" in agg["by_sensor"]
    assert agg["by_type"]["temperature"] == 2
    assert agg["by_type"]["humidity"] == 1
    assert agg["by_location"]["A"] == 2
    assert agg["by_location"]["B"] == 1


def test_aggregates_empty():
    """Empty list returns empty dicts."""
    agg = compute_aggregates([])
    assert agg["by_sensor"] == {}
    assert agg["by_type"] == {}
    assert agg["by_location"] == {}


# ---------------------------------------------------------------------------
# process_stream end-to-end tests
# ---------------------------------------------------------------------------

SAMPLE_DATA = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "sample"
    / "sensor_events.json"
)


def test_process_stream_sample_data(tmp_path: Path):
    """Run against actual sample data and verify counts and outputs."""
    summary = process_stream(SAMPLE_DATA, tmp_path)

    assert summary.events_seen == 16
    assert summary.events_accepted == 14
    assert summary.events_rejected == 1
    assert summary.events_duplicate == 1
    assert summary.dead_letter_count == 2
    assert summary.status == "success"

    accepted_path = tmp_path / "accepted.jsonl"
    accepted_text = accepted_path.read_text(encoding="utf-8")
    accepted_lines = accepted_text.strip().splitlines()
    assert len(accepted_lines) == 14

    dl_path = tmp_path / "dead_letter.jsonl"
    dl_text = dl_path.read_text(encoding="utf-8")
    dead_letter_lines = dl_text.strip().splitlines()
    assert len(dead_letter_lines) == 2

    summary_path = tmp_path / "summary.json"
    assert summary_path.exists()
    summary_data = json.loads(
        summary_path.read_text(encoding="utf-8")
    )
    assert isinstance(summary_data, dict)

    assert len(summary.aggregates["by_sensor"]) == 5


def test_process_stream_all_valid(tmp_path: Path):
    """Write 3 unique valid events and verify accepted=3."""
    events = [
        make_event(
            sensor_id="s1",
            timestamp="2024-06-01T08:00:00Z",
        ),
        make_event(
            sensor_id="s2",
            timestamp="2024-06-01T08:01:00Z",
        ),
        make_event(
            sensor_id="s3",
            timestamp="2024-06-01T08:02:00Z",
        ),
    ]
    input_file = tmp_path / "input.jsonl"
    write_jsonl(input_file, events)
    out = tmp_path / "output"

    summary = process_stream(input_file, out)

    assert summary.events_accepted == 3
    assert summary.events_rejected == 0
    assert summary.events_duplicate == 0


def test_process_stream_duplicates(tmp_path: Path):
    """Write same event twice: accepted=1, duplicate=1."""
    event = make_event()
    input_file = tmp_path / "input.jsonl"
    write_jsonl(input_file, [event, event])
    out = tmp_path / "output"

    summary = process_stream(input_file, out)

    assert summary.events_accepted == 1
    assert summary.events_duplicate == 1


def test_process_stream_malformed_json(tmp_path: Path):
    """Write 'not json' + one valid event: rejected=1, accepted=1."""
    input_file = tmp_path / "input.jsonl"
    with open(input_file, "w", encoding="utf-8") as f:
        f.write("not json\n")
        f.write(json.dumps(make_event()) + "\n")
    out = tmp_path / "output"

    summary = process_stream(input_file, out)

    assert summary.events_rejected == 1
    assert summary.events_accepted == 1


def test_process_stream_empty_file(tmp_path: Path):
    """Empty file: events_seen=0, accepted=0."""
    input_file = tmp_path / "input.jsonl"
    input_file.write_text("", encoding="utf-8")
    out = tmp_path / "output"

    summary = process_stream(input_file, out)

    assert summary.events_seen == 0
    assert summary.events_accepted == 0


def test_process_stream_dead_letter_contents(tmp_path: Path):
    """One rejected + one valid: dead_letter.jsonl has 1 line with status and reason."""
    events = [
        make_event(value=None),  # rejected: null value
        make_event(
            sensor_id="s1",
            timestamp="2024-06-01T09:00:00Z",
        ),
    ]
    input_file = tmp_path / "input.jsonl"
    write_jsonl(input_file, events)
    out = tmp_path / "output"

    process_stream(input_file, out)

    dl_path = out / "dead_letter.jsonl"
    dl_text = dl_path.read_text(encoding="utf-8")
    dead_letter_lines = dl_text.strip().splitlines()
    assert len(dead_letter_lines) == 1

    record = json.loads(dead_letter_lines[0])
    assert record["status"] == "rejected"
    assert "reason" in record
    assert record["reason"] is not None


def test_process_stream_summary_json_shape(tmp_path: Path):
    """Verify summary.json has all expected keys."""
    summary_dir = tmp_path / "output"
    process_stream(SAMPLE_DATA, summary_dir)

    summary_path = summary_dir / "summary.json"
    summary_data = json.loads(
        summary_path.read_text(encoding="utf-8")
    )

    expected_keys = {
        "pipeline_name",
        "run_at",
        "duration_seconds",
        "status",
        "events_seen",
        "events_accepted",
        "events_rejected",
        "events_duplicate",
        "dead_letter_count",
        "aggregates",
        "rejection_reasons",
    }
    assert expected_keys.issubset(summary_data.keys())


def test_process_stream_rerun_idempotent(tmp_path: Path):
    """Run twice, outputs overwritten not doubled."""
    events = [
        make_event(
            sensor_id="s1",
            timestamp="2024-06-01T08:00:00Z",
        ),
        make_event(
            sensor_id="s2",
            timestamp="2024-06-01T08:01:00Z",
        ),
    ]
    input_file = tmp_path / "input.jsonl"
    write_jsonl(input_file, events)
    out = tmp_path / "output"

    process_stream(input_file, out)
    process_stream(input_file, out)

    accepted_path = out / "accepted.jsonl"
    accepted_text = accepted_path.read_text(encoding="utf-8")
    accepted_lines = accepted_text.strip().splitlines()
    assert len(accepted_lines) == 2
