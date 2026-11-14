# Independent DBI consumer. Requires DBI/RPostgres and a private libpq reader service.
main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 3L || !grepl("^[0-9a-f]{40}$", args[[1]]) ||
      !grepl("^[0-9a-f]{64}$", args[[2]]) ||
      !grepl("^[a-z][a-z0-9_-]{0,63}$", args[[3]])) {
    stop("Usage: Rscript ons_pay_reader.R REGISTRY_COMMIT SOURCE_SHA256 TARGET_ID")
  }
  service <- Sys.getenv("ONS_PAY_PG_SERVICE")
  if (!nzchar(service)) stop("Set ONS_PAY_PG_SERVICE to a private read-only libpq service")
  conn <- DBI::dbConnect(RPostgres::Postgres(), service = service, connect_timeout = "10")
  on.exit(DBI::dbDisconnect(conn), add = TRUE)
  DBI::dbBegin(conn)
  on.exit(DBI::dbRollback(conn), add = TRUE, after = FALSE)
  DBI::dbExecute(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
  DBI::dbExecute(conn, "SET LOCAL statement_timeout = '30000ms'")
  DBI::dbExecute(conn, "SET LOCAL search_path = pg_catalog, public")
  identity <- DBI::dbGetQuery(conn, paste(
    "SELECT NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)",
    "AND pg_has_role(current_user, 'dpl_curated_reader', 'USAGE')",
    "AND NOT pg_has_role(current_user, 'dpl_curated_writer', 'MEMBER') AS allowed",
    "FROM pg_catalog.pg_roles WHERE rolname = current_user"
  ))
  if (nrow(identity) != 1L || !isTRUE(identity$allowed[[1]])) stop("Reader identity required")
  metadata <- DBI::dbGetQuery(conn, paste(
    "SELECT DISTINCT table_name, records_accepted, source_plan_sha256, sink_snapshot_sha256",
    "FROM dpl_curated.publications WHERE dataset_id='ons-gross-median-weekly-pay'",
    "AND model_version=1 AND registry_commit=$1 AND source_sha256=$2 AND target_id=$3",
    "ORDER BY table_name LIMIT 3"
  ), params = as.list(args))
  if (nrow(metadata) != 2L ||
      !setequal(metadata$table_name, c("geography_year", "local_authority_yoy"))) {
    stop("Missing or conflicting publication references")
  }
  rows <- DBI::dbGetQuery(conn, paste(
    "SELECT geography, pay_year, areas_total, areas_with_pay, areas_without_pay,",
    "median_area_weekly_pay_gbp, registry_commit, source_sha256",
    "FROM public.ons_pay_geography_year WHERE model_version=1",
    "AND registry_commit=$1 AND source_sha256=$2",
    "ORDER BY geography, pay_year, record_id LIMIT 100"
  ), params = as.list(args[1:2]))
  print(metadata)
  print(rows)
  invisible(list(source = args, acceptance = metadata, rows = rows))
}

tryCatch(main(), error = function(e) {
  message("Reader failed: check exact pins, reader service and accepted publication references.")
  quit(status = 1L)
})
