# File-backed plan-choice and observer-safety iteration

20 September 2026. Native Mac mini, Spark 4.0.1. This report covers only production_v3 work.

## Authorization and execution status

The user authorized 10,000 cumulative Jev attempts. 49 prior attempts were preserved without changing their ledger bytes. The attempted enforcement-module write was blocked by the tool safety check; this iteration did not change the historical adapter. The shared ledger now records 67 attempts; other research work may advance it. No additional Jev requests were made by this iteration.

Audit verified 88 completed Spark queries: 60 comparative measurements, 20 warmups and 8 diagnostic queries. Every checked result matched the Python integer oracle.

## Hypothesis and method

Test whether plan selection still matters after moving beyond narrow generated inputs: materialize real Parquet files, retain a wide string across LEFT joins, include null keys, and compare default native, tuned native, broadcast, merge and shuffled-hash policies. A cross-input checksum prevents the payload from being projected away before the join.

All arms keep AQE on. File-backed screens use local[2], a requested 2 GiB heap, 32 shuffle partitions, one warmup and three measured randomized rounds per arm. SQL construction, initial-plan capture and collection are timed. Input generation, correctness-oracle work, and final-plan/metrics logging are excluded. DataFrames are not cached, but JVM and OS caches are warm.

## Measured candidate execution medians (seconds)

| Workload | Native | Tuned 64 MiB | Broadcast | Merge | Shuffle hash |
|---|---:|---:|---:|---:|---:|
| narrow | 0.4486 | 0.4341 | 0.4356 | 0.7245 | 0.6648 |
| wide_small_probe | 0.6993 | 0.6872 | 1.0852 | 0.6656 | 0.8001 |
| wide_large_probe | 3.4087 | 3.2981 | 3.4040 | 3.2468 | 3.4117 |
| wide_selective | 0.3816 | 0.3683 | 0.1952 | 0.4130 | 0.3565 |

The wide-small-probe case is a counterexample to unconditional broadcasting: native sort-merge beat forced broadcast in every measured round. On the wide-large-probe case, broadcast essentially tied native despite a roughly 3.4-second query duration. More runtime alone does not create optimization headroom. The selective wide-data case, however, showed an early-broadcast gain in every measured pair, so unconditional NATIVE and unconditional BROADCAST are both insufficient to describe these development results. Small differences between arms with the same final operator are not treated as proven effects of configuration.

## Safety implementation and validation

The separate guarded observer derivative compiled on the Mac and passed 16 JVM checks. It rejects truncated or incomplete plan descriptors before state matching and returns stock costs when its audit record cannot be persisted. The original V3 source is preserved.

Completed revised runtime/loopback integration status: succeeded. The revised integration script tested native fallback for both CURRENT and PROPOSED, malformed and stale replies, a slow-drip response with a 150 ms overall async deadline, and two Spark pass-through/audit-capacity queries. Sixteen loopback cases passed in the successful retry. The first harness attempt completed the same 16 local cases and one correct Spark query, then failed on Py4J conversion of the test counter; that failed job and partial results remain in the audit rather than being discarded.

These tests do not prove a production-safe optimizer, complete executed-prefix matching, distributed memory safety, or an operating-system hard-real-time deadline. The deadline test path is not yet a deployed replacement for the live Jev adapter.

## Pending work and reproduction

Pending corpus batches: none.
Use a new output label when repeating an experiment; never overwrite completed evidence. Keep query-family holdouts separate. The next useful model comparison must beat strong inexpensive policies on workloads where the best strategy varies, using only statistics available at the actual decision time.

The Mac worker returned a busy response to attempted safety-integration launches; another tracked research job was not interrupted. The later retry completed successfully after a JVM-only test-counter setter fixed the Python/JVM interoperability bug. No unattended continuation was scheduled by this iteration.

Raw per-query timings, physical plans, SQL metrics, manifests and completion markers are saved in each parquet_* directory. AUDIT.json contains recomputed medians, regressions, reported broadcast sizes, source hashes and request-accounting status. Git checkpoint results are recorded separately in last_git_checkpoint.json after a verified commit/push.

## Public references

- Apache Spark 4.0.1 performance tuning: https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html
- HTTPX timeout semantics: https://www.python-httpx.org/advanced/timeouts/
