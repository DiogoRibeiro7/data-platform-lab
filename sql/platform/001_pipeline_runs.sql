CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','success','failed')),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (duration_seconds >= 0),
    rows_read BIGINT NOT NULL DEFAULT 0 CHECK (rows_read >= 0),
    rows_written BIGINT NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    rows_rejected BIGINT NOT NULL DEFAULT 0 CHECK (rows_rejected >= 0),
    files_processed BIGINT NOT NULL DEFAULT 0 CHECK (files_processed >= 0),
    files_rejected BIGINT NOT NULL DEFAULT 0 CHECK (files_rejected >= 0),
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, run_id)
);

CREATE INDEX IF NOT EXISTS pipeline_runs_started_at_idx
    ON pipeline_runs (started_at DESC NULLS LAST);
