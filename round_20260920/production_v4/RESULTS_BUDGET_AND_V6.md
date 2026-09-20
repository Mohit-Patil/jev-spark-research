# Cumulative budget and runtime research checkpoint

20 September 2026. Active authorization: **10,000 cumulative API request attempts**, not 10,000 additional attempts. Historical usage was preserved.

Current verified accounting: **67/10,000 used; 9933 remaining**.

## What this continuation executed

The authorization update passed 18 offline accounting tests. The repaired prior-run audit passed 448 checks over 137 saved executions; it made no new Spark or model calls. It reconstructed original request-byte ordering with the frozen builder rather than hashing a sorted log representation.

The new representation experiment passed 12 offline feature-isolation tests, then executed 54 native Spark queries: nine execution warmups and 45 randomized measured runs. All aggregate results matched an independently recomputed integer oracle. Eighteen actual Jev requests were charged to the same durable ledger.

## Direct end-to-end measurements

Each cell is the median of three measured runs in seconds. All original stage work and synchronous advisor overhead are included. A small same-host study is not a production benchmark or a statistical guarantee.

| Case | Native defaults | Fixed runtime rule | Native AQE, 32 MiB | Jev full plans | Jev compact features |
|---|---:|---:|---:|---:|---:|
| v6_small_uniform | 0.211700 | 0.306850 | 0.304640 | 1.251037 | 0.642562 |
| v6_large_uniform | 3.353935 | 2.761571 | 2.767311 | 3.927050 | 3.750785 |
| v6_large_skew | 5.113855 | 2.268301 | 2.208591 | 6.083642 | 5.607851 |

Full and compact inputs used identical questions and choice labels. Compact input retained verified join semantics and calculated runtime summaries but omitted full plan text and partition ordering; it is deliberately lossy. Model choices used no execution outcomes or winner labels.

## Model responses and transport

| View | Requests | Succeeded | NATIVE | BROADCAST | ABSTAIN | Applied | Median HTTP seconds | Input tokens |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| VERBOSE | 9 | 7 | 0 | 0 | 9 | 0 | 0.610138 | 13592 |
| COMPACT | 9 | 9 | 0 | 0 | 9 | 0 | 0.387048 | 8994 |

Worst observed individual Jev/default ratio: **11.415x**, in `v6_small_uniform` / `JEV_VERBOSE`, repeat 2. This is one paired observation, not a workload-wide regression estimate.

## Interpretation boundaries

Native default, fixed-rule and tuned-AQE comparisons are all measured, not sums of component medians. The rule can have headroom on large inputs yet regress on small inputs; one larger broadcast limit is not universally beneficial. Any compact-input improvement must include prediction failures and network tails, not just request size or token reductions.
No broad prediction-accuracy, Jev superiority, distributed-scale speedup or production readiness is established. Three repeats, one LEFT-join family, generated Range inputs, fixed-width rows and a shared warmed JVM are substantial limitations. Header/network timing includes client/TLS/server effects and is not isolated model inference. In-memory hash footprint and live free-heap observations remain missing.

## Accounting and reproducibility

The new authorized runner preserves all original ledger bytes and appends under the existing file lock. Failed and interrupted attempts remain charged. Legacy frozen adapters were left unchanged and keep their old 50-attempt writer guard; new runs explicitly inject the authorized budget. The 600-second bridge timeout, local[2] execution and synthetic-only constraints remain unchanged.
The new audit passed 207 invariants and recomputed request hashes, source hashes, results, application scope and medians. See `AUDIT_REPRESENTATION_V6.json` and the raw event/decision files under `runtime_representation_v6/`.

## Resume

Read `authorized_budget.py` and call `AuthorizedBudget().status()` before new requests. Use `BatchBudget` to retain small job-local caps. Preserve all result directories and manifests. Do not treat repeatedly observed templates as untouched holdouts.
Next scientific question: selective invocation and decision quality across several genuinely different query families, rather than spending the expanded budget repeating only these three snapshots. Compare any proposed learner to a cheap local gate and independently tuned native AQE. Existing evidence does not justify deploying the Jev hook.
A separate-executor follow-on was blocked by a tool safety check before execution. That branch was not retried or bypassed. No unattended job has been scheduled.

## Primary-source context

Spark 4.0.1 documents separate initial and adaptive broadcast thresholds, runtime statistics, and the extra work retained when AQE switches strategies after execution begins: https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html
AQORA is relevant prior research on runtime planner extensions and stage-level feedback; its published results are not Jev results: https://arxiv.org/html/2510.10580v1

These references contextualize the design; the numerical tables above are from this Mac's saved measurements, not from the external papers.

## Late-response diagnostic

The valid-model-answer counts are separate from effective fallback choices: 16 requests completed validly and all 16 answered ABSTAIN. Two full-plan attempts failed the late-response handling and also used the native fallback. They remain charged; they are not two additional valid model abstentions. See CALL_AND_FALLBACK_DIAGNOSTIC_V6.json for sanitized statuses.

The 1.8-second response gate is a rejection threshold, not a strict wall-clock cancellation guarantee. The loopback callback has its own 2.2-second read timeout, while the HTTP operation can complete later; the longest recorded full-plan request was about 4.32 seconds. Late results cannot retroactively alter an already resumed query. This tail behavior remains a production limitation and is included in measured workflow costs.

The earlier V5 test did apply one live Jev BROADCAST recommendation. V6 applied none. These are different batches, not inconsistent reports. The model has not yet shown dependable net improvement over the cheap baselines.
