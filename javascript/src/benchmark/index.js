/**
 * Benchmark — compare ingestion strategies for file-processing workloads.
 */

export {
  generateTestFiles,
  processFile,
  runSequential,
  runConcurrent,
  runPool,
  runBenchmark,
  formatReport,
  saveReport,
} from "./runner.js";
