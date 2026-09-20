# Measured milestone: candidate-headroom correction and real AQE observation

20 September 2026. Native Mac mini; Spark 4.0.1; local[2]; 1 GiB requested driver heap; AQE and the default 10 MiB broadcast threshold retained. Synthetic Range data only. No installation was needed.

## Hypothesis -> screen -> correction

The earlier tiny pilot had no useful alternative. A broader development-only screen varies dimension selectivity (1%, 10%, 50%) at fixed 1.2 million fact and 2 million dimension input rows, then varies skew at 50%. `headroom_v2` completed 84 executions, but inherited initial-plan deduplication from the old harness. That is unsafe for whole-query hint variants: NATIVE and MERGE have identical initial plans but can diverge during AQE.

`headroom_v3` fixes the inventory by retaining all four strategies, recording rather than removing initial collisions. It completed 144 executions: 32 warmups and 112 measurements, seven measurements per case/strategy. Every result matches the expected integer aggregate. The observed 10% case confirms the bug: both initial plans are SortMergeJoin, but native finishes as BroadcastHashJoin and MERGE remains SortMergeJoin. The original frozen pilot and holdout were not modified.

| Development case | Native median ms | Lowest candidate median ms | Candidate |
|---|---:|---:|---|
| 1% dimension selectivity | 83.797 | 60.772 | SHUFFLE_HASH |
| 10% dimension selectivity | 159.136 | 97.883 | BROADCAST |
| 50% dimension selectivity | 199.476 | 198.731 | MERGE |
| 50% selectivity, 80% hot-key generator | 252.291 | 213.286 | BROADCAST |

The 0.745 ms uniform-50% difference is not useful evidence of a winner. Timing variability is substantial in some cases; these are development headroom observations, not held-out speedup claims. All figures include SQL construction, initial-plan capture, and collect, but exclude candidate enumeration and any selector. A whole-query hinted improvement is not a label for changing plans after a shuffle has already completed.

## Observation implementation -> verification

The exact installed bytecode and saved Spark 4.0.1 source agree on the two cost-evaluator call sites: CURRENT at line 365 and PROPOSED at 366 in AdaptiveSparkPlanExec.scala. Release commit: 29434ea766b0fc3c3bf6eaadb43a8f931133649e. The hook depends on those version-specific internals; it is not a general stable pairwise evaluator API.

NativeBoundaryObserverV2 wraps the stock SimpleCostEvaluator and returns its Cost object unchanged. It pairs calls by evaluator instance, thread, source location and run tag, captures current/proposed remaining plans and actual visible query-stage state, and independently records the native comparison predicate. Missing values remain unknown. Visible stage leaves are not a full history of all completed work. Spark runtime size statistics, reported map-output partition bytes, and hash-table memory must not be conflated.

`observer_check_v2` completed 36 queries (28 measured, 8 warmups), capturing 63 pairs/126 cost calls, with zero unmatched calls, errors or dropped records. Every aggregate is correct, and all recorded native comparison predicates match recomputation. Measured observer self-time medians were 2.372 ms/query for the selective case and 1.413 ms/query for the skew case. Randomized paired end-to-end differences were +1.486 ms and +1.387 ms respectively, but include JVM/GC/scheduling variability; they do not demonstrate zero overhead.

Ten new offline tests passed: candidate retention, actual plan divergence, exact stored result comparisons, evidence isolation, view separation, unknown values, source-role validation, truncation rejection, intervention rejection and partition-total consistency.

## API accounting and next execution

The authoritative shared request ledger currently reports 13 attempts, including pre-existing expanded-development attempts 12 and 13. This continuation's baseline/observer checks made zero calls. Do not reset or infer this ledger from any older checkpoint.

The compiled V2 observer supports a bounded, loopback-only synchronous shadow callback. `live_shadow_v2.py` is prepared but has NOT yet been executed at this checkpoint. Its maximum is six additional attempts, within the unchanged cumulative 50-attempt limit. It excludes native decisions, stock-cost labels and future query outcomes from Jev inputs and never changes returned Spark costs. Only completed run artifacts can establish live execution.

Raw results, plans, manifests, bytecode checks, compiler records and test output are alongside this note. One combined helper write was denied by the connector; no code from that denied write executed. The subsequent smaller scripts shown here compiled/tested through the normal connector.
