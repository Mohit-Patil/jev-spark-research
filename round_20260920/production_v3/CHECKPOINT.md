# File-backed corpus and guarded-observer checkpoint

20 September 2026. Own scope: `round_20260920/production_v3`. Do not confuse its numbering with the concurrently developed `production_v4` directory.

## Verified work

Read `audit_v1/RESULTS.md` and `audit_v1/AUDIT.json`. The successful native audit verified 88 query results: 60 comparative measurements, 20 warmups, and eight diagnostics. This includes one correct query retained from the initially failed safety harness, followed by two correct queries in its repaired run. Five Parquet batch directories contain immutable manifests, plans, raw timings, SQL metrics and completion markers. These are synthetic local file-backed tests, not production-cluster validation.

The wide-small-probe workload falsifies unconditional broadcast: broadcast lost all three measured pairs. With the same wide dimension and a larger probe, broadcast essentially tied native at about 3.4 seconds. A selective wide dimension showed early-broadcast headroom. None of these measurements are Jev-driven speedups or legal mid-query counterfactual labels.

The original V3 observer source was preserved. The V6 derivative rejects truncated/missing plan descriptors and returns stock costs when an audit record cannot be persisted. Sixteen native JVM checks passed. Sixteen loopback deadline/fallback cases passed in the repaired integration run, including both native decisions and slow-trickle cancellation around 153 milliseconds. These cases also ran before the earlier harness failed. They are repetitions of 16 configurations, not 32 distinct tests. Two Spark pass-through/audit-saturation queries then succeeded; no runtime plan overrides were applied.

## Failure retained

Job `1e2283b5cfc6483fb4477b1e49578537` failed after its first correct Spark query: Py4J converted the AtomicLong test counter to an integer, so Python `.set(1000)` failed. `runtime_safety_v1/` is retained without a false completion marker. `repair_runtime_safety.py` compiled a JVM-only test helper and created `runtime_safety_checks_v2.py`; job `2ed47ad701664fda8eb61d0a1c94c22f` completed the repaired run successfully. This fault helper changes only the observer's test audit counter, never an API counter.

## Authorization and accounting

The user increased the cumulative Jev allowance to 10,000 in the message at `2026-09-20T17:21:23Z`. `BUDGET_AUTHORIZATION.json` records that authorization and the digest of the 49-attempt ledger prefix then present. No prior ledger bytes were changed. The latest audit read 67 cumulative attempts and verified the earlier prefix; other research work advanced the shared count. This iteration made ZERO Jev requests.

The attempted new `cumulative_budget.py` write was blocked by tool safety. It was not bypassed or retried through another route. The historical `safe_jev.py` remains unchanged with LIMIT=50. The authorization and an implemented allowance mechanism are different facts. Any separately developed budget/client implementation must be inspected on resume; this checkpoint does not certify it. Do not reset the durable ledger or issue unaccounted requests.

## Resume safely

Rerun `audit_iteration_v2.py --label audit_v2` through a bounded native job to audit without new Spark or Jev requests. New benchmarks need new output labels. Frozen sources, datasets, failures and completed results must remain intact. The next meaningful selection test must use independent data/family holdouts and compare against tuned native and inexpensive policies, with realistic feature acquisition. Hardening is not complete: full executed-prefix identity, distributed memory/concurrency safety, and integration of a total deadline into a live advisor remain unproven.

Use `checkpoint_scoped.py` to commit only this directory after inspecting for other staged changes. It excludes Parquet data, binaries, credentials and the request ledger. `last_git_checkpoint.json` records the actual commit and push outcome after execution. No unattended research was scheduled by this iteration.
