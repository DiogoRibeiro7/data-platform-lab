-- Immutable migration: add a new numbered file for subsequent schema changes.
-- These public tables preserve the existing ONS publishing contract.
CREATE TABLE IF NOT EXISTS public.ons_pay_geography_year (
  record_id CHAR(64) NOT NULL,
  model_version INTEGER NOT NULL,
  geography TEXT NOT NULL,
  pay_year INTEGER NOT NULL,
  areas_total INTEGER NOT NULL,
  areas_with_pay INTEGER NOT NULL,
  areas_without_pay INTEGER NOT NULL,
  min_area_weekly_pay_gbp DOUBLE PRECISION,
  median_area_weekly_pay_gbp DOUBLE PRECISION,
  max_area_weekly_pay_gbp DOUBLE PRECISION,
  source_sha256 CHAR(64) NOT NULL,
  registry_commit CHAR(40) NOT NULL,
  PRIMARY KEY (record_id),
  CHECK (areas_total = areas_with_pay + areas_without_pay)
);
CREATE TABLE IF NOT EXISTS public.ons_pay_local_authority_yoy (
  record_id CHAR(64) NOT NULL,
  model_version INTEGER NOT NULL,
  area_code TEXT NOT NULL,
  area_name TEXT NOT NULL,
  reference_date DATE NOT NULL,
  pay_year INTEGER NOT NULL,
  weekly_pay_gbp DOUBLE PRECISION,
  status_code TEXT,
  previous_year INTEGER,
  previous_weekly_pay_gbp DOUBLE PRECISION,
  yoy_nominal_pct DOUBLE PRECISION,
  source_sha256 CHAR(64) NOT NULL,
  registry_commit CHAR(40) NOT NULL,
  PRIMARY KEY (record_id)
);

-- An index of immutable accepted evidence, not a source approval or release registry.
-- Existing publishing does not insert index entries automatically.
CREATE TABLE IF NOT EXISTS dpl_curated.publications (
  evidence_sha256 CHAR(64) PRIMARY KEY CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
  attempt_id UUID NOT NULL UNIQUE,
  dataset_id TEXT NOT NULL CHECK (dataset_id = 'ons-gross-median-weekly-pay'),
  table_name TEXT NOT NULL CHECK (table_name IN ('geography_year', 'local_authority_yoy')),
  target_id TEXT NOT NULL CHECK (length(target_id) BETWEEN 1 AND 128),
  registry_commit CHAR(40) NOT NULL CHECK (registry_commit ~ '^[0-9a-f]{40}$'),
  source_sha256 CHAR(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
  model_version INTEGER NOT NULL CHECK (model_version > 0),
  records_accepted BIGINT NOT NULL CHECK (records_accepted BETWEEN 0 AND 100000),
  source_plan_sha256 CHAR(64) NOT NULL CHECK (source_plan_sha256 ~ '^[0-9a-f]{64}$'),
  sink_snapshot_sha256 CHAR(64) NOT NULL CHECK (sink_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
  published_at TIMESTAMPTZ NOT NULL,
  evidence_path TEXT NOT NULL CHECK (length(evidence_path) BETWEEN 1 AND 4096)
);
CREATE INDEX IF NOT EXISTS publications_vintage_idx ON dpl_curated.publications
  (dataset_id, table_name, target_id, model_version, registry_commit, source_sha256);

DO $roles$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dpl_curated_writer') THEN
    CREATE ROLE dpl_curated_writer NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dpl_curated_reader') THEN
    CREATE ROLE dpl_curated_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$roles$;

-- The existing publisher issues CREATE TABLE IF NOT EXISTS before its upsert.
-- The writer is a trusted operator; it can create objects in public, but cannot
-- modify pipeline_runs or the migration ledger through these grants.
GRANT USAGE, CREATE ON SCHEMA public TO dpl_curated_writer;
GRANT USAGE ON SCHEMA public, dpl_curated TO dpl_curated_reader;
GRANT USAGE ON SCHEMA dpl_curated TO dpl_curated_writer;
GRANT SELECT, INSERT, UPDATE ON public.ons_pay_geography_year,
  public.ons_pay_local_authority_yoy TO dpl_curated_writer;
GRANT SELECT, INSERT ON dpl_curated.publications TO dpl_curated_writer;
GRANT SELECT ON public.ons_pay_geography_year, public.ons_pay_local_authority_yoy,
  dpl_curated.publications TO dpl_curated_reader;
GRANT SELECT ON dpl_curated.schema_migrations TO dpl_curated_reader, dpl_curated_writer;
