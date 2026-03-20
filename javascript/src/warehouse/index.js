/**
 * Warehouse — load data into analytical stores and run warehouse-style queries.
 *
 * Covers SQLite loading, analytical query patterns, and gold-layer dataset
 * production for consumption by downstream tools.
 */

export {
  loadRawCsv,
  loadRawEventsJson,
  runSqlFile,
  runWarehousePipeline,
  WAREHOUSE_QUERIES,
} from "./loader.js";
