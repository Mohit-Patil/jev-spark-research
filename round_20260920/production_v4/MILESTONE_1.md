# Runtime candidate generation — measured milestone

20 September 2026. This continuation owns `production_v4`; newer `practical_v2` results were inspected but are inherited, not re-attributed.

The native preflight observed Git f150ecff8e53ccb7a54a744b1f73ee1a1ed467e1 and a durable count of 29/50. A separate running research job advanced that count to 38 before our bounded round began. Busy-worker responses were respected; no other job or hidden bridge state was altered.

## Hypothesis and method

A native current/proposed selector cannot choose a broadcast candidate that is missing. A SparkSessionExtensions query-stage preparation rule can construct a bounded broadcast-right alternative from two already-materialized shuffle leaves, reuse the identical stage objects, and let normal AQE cost comparison adopt it. This is a fixed local rule, not Jev.

Admission: Spark 4.0.1, INNER/LEFT join, a single Long key, no residual condition or skew-split join, both input shuffle stages materialized, right runtime rows <=1,250,000 and runtime size <=32 MiB. These caps are experimental bounds, not a general broadcast hash-table memory proof. New plan output and Spark distribution requirements are checked; exceptions return the original plan. OFF is inert; SHADOW builds/validates without applying; APPLY admits once per run.

## Executed

Job 51e4fd548da34ad0a3cf5ae3d232cbed failed at compile time on Scala's Set type inference. The source and diagnostics were preserved. Job 8a55740b0fb14fff9d9c3ecee449b237 then completed the repair, a three-query smoke test, and 56 randomized development queries (40 measured, 16 warmups). All results matched the independent integer oracle. Fifteen runtime applications occurred including the smoke and warmups. No Jev call was made; the ledger stayed at 38/50 throughout this job.

The development cases had 12 million fact rows, 2 million unfiltered dimension rows, 50% dimension selectivity, and either uniform or retained-hot-key input. All arms used local[2], an actual 2 GiB JVM maximum heap, 32 shuffle partitions, and normal AQE enabled.

| Case | OFF native median | SHADOW median | APPLY runtime median | Whole-query broadcast diagnostic |
|---|---:|---:|---:|---:|
| Retained skew | 2.342674 s | 2.290919 s | 1.230624 s | 0.346912 s |
| Uniform | 1.872003 s | 1.834388 s | 1.426068 s | 0.528793 s |

APPLY beat OFF in all five measured rounds for each case. Exact recorded plan/stage-statistic signatures also matched between SHADOW and APPLY in all five rounds for each case. Whole-query costs include the original shuffles, candidate construction, and additional broadcast; they do not credit the runtime policy with avoiding completed work. Five repeats, shared JVM caches, and a single local machine limit inference. The modest apparent SHADOW advantage is not treated as a real optimization.

These results establish an executable candidate-generation path with local development headroom, NOT Jev value, a production-safe optimizer, or distributed-scale performance.

## Next falsification

Freeze the source and test independent 18-million-row snapshots. Add a standard native AQE comparator whose adaptive broadcast threshold matches the same 32 MiB candidate bound. This checks whether ordinary Spark configuration explains the gain without custom code or model calls.
