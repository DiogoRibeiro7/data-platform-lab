# Saved ONS job status

Inspect the completed runs of a [saved job](ons-pay-refresh-jobs.md) without
executing it. Both full refresh and `"load_only": true` jobs are supported:

```bash
cd python
poetry run data-platform-lab ons-pay-job-status --job ../jobs/ons-load.json

# Fail the check if the last complete success is more than 24 hours old.
poetry run data-platform-lab ons-pay-job-status \
  --job ../jobs/ons-load.json --max-success-age-hours 24
```

The command reads the job and batch audits under its resolved `output_root/manifests`.
It does not read handoffs or stored data, contact a source or database, acquire
destination guards, create directories, or import optional analytics dependencies.
Relative paths retain the saved job's directory as their base, regardless of the
working directory. Status remains available if a source handoff has been removed.

## Outcomes and exit codes

| Status | Meaning | Exit code |
| --- | --- | --- |
| `success` | The latest matching completed run succeeded and meets the optional age limit. | `0` |
| `failed` | The latest matching completed run failed, even if an older run succeeded. | `1` |
| `stale` | The latest run succeeded, but the last complete success exceeds the age limit. | `1` |
| `never_run` | No completed audit identifies these exact job bytes in this output root. | `1` |
| No JSON report | Invalid arguments, job configuration, or unreadable/inconsistent audit history. | `2` |

`--max-success-age-hours` must be finite and non-negative. Omitting it disables
the age check; equality with the limit passes. Age is elapsed UTC time since
completion of the last successful **whole batch**, not source retrieval age or
the time of a failed attempt. A later failure always takes precedence over age.

JSON includes `schema_version`, `job_sha256`, `pipeline_name`, `checked_at_utc`,
`status`, `matching_runs`, `last_success_age_hours`, `max_success_age_hours`,
`latest_run` and `latest_success`. Each non-null run contains its ID, receipt path,
start/completion times, duration, completed/total source counts, failure phase
and zero-based failed source index. Arbitrary exception text and source rows are
not included. With no successful run, `latest_success` and its age are null.
Runs are ordered by completion time, with run ID as a deterministic tie-breaker.

## Evidence and limits

Only audits with the job's exact raw-byte SHA-256 count. A whitespace edit or
change of mode starts a different configuration history. Restore the original
bytes to inspect the original history. Direct invocations and older audits with
no job digest are excluded, as are other jobs with valid different digests.
The digest identifies configuration; it is not an authenticity signature. Keep
job files and audit directories under trusted control.

Full jobs read `ons_pay_refresh_*.json`; load-only jobs read `ons_pay_load_*.json`.
A raw-load success means the raw batch completed, with no claim that data was
transformed. It cannot reset the freshness of a full refresh. For current curated
table integrity, source age and quality checks, use
[`ons-pay-monitor`](ons-pay-monitoring.md).

Status describes **completed, retained audit evidence**. It does not detect a
currently running process. Busy rejections, invalid configuration, a process crash,
or failure to publish the audit may leave no receipt; an older completed outcome
can therefore remain visible. `never_run` also covers removed history. Retain
invocation exit codes and scheduler/process logs to diagnose those cases. Inspection
can run while a job is busy, but does not promise to include audits published after
directory enumeration begins. No scheduler or alert delivery is installed.

Inspection accepts at most 10,000 files matching the selected pipeline, 4 MiB per
file and 64 MiB in total, including history for other jobs. Exceeding a bound fails
with exit `2`, never a partial result. Symlinked evidence, invalid JSON, duplicate
JSON keys, non-finite numbers and malformed job digests in this pipeline also fail
the check because their ownership cannot be safely established. For matching jobs,
all retained runs must have consistent identities, timestamps, counts, policies
and failure progress. Unknown schema/model versions are rejected. Unrelated
pipelines and unfinished temporary audit files are ignored.

The Python API `inspect_ons_pay_job` in
`data_platform_lab.observability.ons_pay_job_status` accepts `job_path`, optional
`max_success_age_hours` and an optional timezone-aware `now`. It returns an immutable
`JobStatusReport` with `to_dict()` for JSON output, or raises `JobStatusError` when
status cannot be established. It performs the same read-only inspection as the CLI.
