const COLUMNS = [
  "pipeline_name", "run_id", "status", "started_at", "ended_at", "duration_seconds",
  "rows_read", "rows_written", "rows_rejected", "files_processed", "files_rejected",
  "warnings", "errors", "extra",
];

export class PostgresRunStore {
  constructor(client) {
    if (!client || typeof client.query !== "function") throw new TypeError("client must implement query()");
    this.client = client;
  }

  async save(metadata) {
    const text = `INSERT INTO pipeline_runs (${COLUMNS.join(",")})
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb)
      ON CONFLICT (pipeline_name,run_id) DO UPDATE SET
      status=EXCLUDED.status, started_at=EXCLUDED.started_at, ended_at=EXCLUDED.ended_at,
      duration_seconds=EXCLUDED.duration_seconds, rows_read=EXCLUDED.rows_read,
      rows_written=EXCLUDED.rows_written, rows_rejected=EXCLUDED.rows_rejected,
      files_processed=EXCLUDED.files_processed, files_rejected=EXCLUDED.files_rejected,
      warnings=EXCLUDED.warnings, errors=EXCLUDED.errors, extra=EXCLUDED.extra, updated_at=now()`;
    const values = [
      metadata.pipeline_name, metadata.run_id, metadata.status, metadata.started_at,
      metadata.ended_at, metadata.duration_seconds, metadata.rows_read, metadata.rows_written,
      metadata.rows_rejected, metadata.files_processed, metadata.files_rejected,
      JSON.stringify(metadata.warnings), JSON.stringify(metadata.errors), JSON.stringify(metadata.extra),
    ];
    await this.client.query(text, values);
  }

  async get(pipelineName, runId) {
    const result = await this.client.query(`SELECT ${COLUMNS.join(",")} FROM pipeline_runs WHERE pipeline_name=$1 AND run_id=$2`, [pipelineName, runId]);
    return result.rows?.[0] ?? null;
  }

  async listRecent(limit = 20) {
    if (!Number.isInteger(limit) || limit < 1) throw new TypeError("limit must be a positive integer");
    const result = await this.client.query(`SELECT ${COLUMNS.join(",")} FROM pipeline_runs ORDER BY started_at DESC NULLS LAST, recorded_at DESC LIMIT $1`, [limit]);
    return result.rows ?? [];
  }
}
