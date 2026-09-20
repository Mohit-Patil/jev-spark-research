# Jev + Spark: prospective history, caching, and resource failure

Audited 2026-09-20T18:37:52+00:00 / 2026-09-21T00:07:52+05:30 (Asia/Kolkata).
Native Mac mini; Apache Spark 4.0.1; Jev 1.13.0; iteration `history_v7`.

## Executive result

The new experiments do not establish incremental Jev value over tuned Spark or a cheap local history selector. Adding four historical performance examples did not produce a winning online Jev workflow on the four prospective evaluation cases. Every valid response in that comparison selected broadcast, including the wide-build case where the historical evidence discouraged it.

The prospective comparison produced 40 Jev workflows; none beat its paired native, tuned-native, or local-history workflow. The complete selective cache episode did beat native, including its first miss and dataset-registration cost, but remained slower than the inexpensive local selector. A second cached workload failed; a separate fresh-process reproduction reported insufficient memory while building/broadcasting its table.

This iteration contains **202 query attempts: 200 correct completions and 2 failures**. The failures are retained, not converted into missing samples. There were 69 new API attempts, bringing the verified cumulative count to 136.

## Authorization and unchanged operating envelope

The user removed the API/Git allowance restriction in the message dated 2026-09-20T18:08:42Z. The existing client retains its 10,000-request engineering backstop, which was not binding for this finite iteration; no ledger reset or new approval was needed. The original historical ledger prefix is verified unchanged. Per-job request caps and the native bridge maximum of 600 seconds remain operational bounds, not a claim that the user still authorizes only 50 calls.

All work used synthetic data under `~/JevResearch`, native `local[2]`, a verified 2 GiB JVM heap, and 32 shuffle partitions. AQE remained enabled. No Docker, cloud resources, extra paid APIs, dependency installation, bridge changes, or company data were used.

## 1. Experiment protocol and evidence isolation

The protocol, query parameters, prompts, source hashes, and historical observations were frozen before the four prospective cases executed. The freeze was pushed as commit `22bb243786cd1e119b0cc0636057b24b3084223b`. All four cases are new data/parameter snapshots. Three use the earlier LEFT lookup structure; one adds a residual join condition not present in the historical data. This is a limited transfer check, not a broad unseen-family benchmark.

The four historical examples came from completed file-backed development cases, not from the evaluation runs. Both Jev-with-history and the local selector received those same earlier candidate medians. The history remained unchanged throughout evaluation. No evaluation outcome entered either selector input. Warmups and other randomized arms can precede a prediction, but their results and timings were never inserted into the fixed payload. Every model response was recorded before executing its own selected query.

Features were obtained from Parquet footers: unfiltered row counts, file sizes, and uncompressed encoded-column bytes. Filter selectivity was explicitly a heuristic derived from the SQL modular predicate. No exact post-filter count was secretly read from the generator or obtained with an extra scan. Unknown post-filter rows and peak hash-table memory remained null. Footer-reading overhead was charged to local/Jev policies every time.

The cheap selector finds the nearest compatible historical regime using log-scaled size features. It accepts a historical alternative only when its advantage exceeds both 10% and 50 ms, and otherwise chooses native. It declines transfer to the previously unseen residual family. This is a small transparent baseline, not a trained general-purpose optimizer.

The Jev arms use identical questions and options, with history present only in one arm. They accept validated choices without a calibrated confidence threshold. Thus the study tests an exploratory selector, not a production confidence policy.

## 2. Direct end-to-end prospective measurements

Seconds below are medians of five measured workflows per cell. SQL configuration, relevant metadata acquisition, unused-candidate planning in Jev arms, live requests, decision recording, and selected SQL execution are included. Final plan logging and one-time data generation are excluded. Native arms do not pay for custom selector features.

| Evaluation case | Native | Tuned native | Local history | Jev without history | Jev with history |
|---|---:|---:|---:|---:|---:|
| lookup_narrow | 0.819348 | 0.675889 | 0.841958 | 1.785670 | 1.894060 |
| lookup_wide | 0.577431 | 0.498352 | 0.506266 | 1.485042 | 1.684108 |
| lookup_selective | 0.468469 | 0.467594 | 0.241677 | 0.801066 | 1.277768 |
| residual_transfer | 0.379153 | 0.377231 | 0.401203 | 0.951614 | 0.678802 |

Tuned native uses 64 MiB initial and adaptive broadcast thresholds as a fixed comparison, not a newly optimized per-query choice. Such a setting is not a universal memory-safety recommendation. Spark documents distinct static/adaptive thresholds, and hints can prioritize broadcast above the automatic threshold [1].

In the selective lookup case, the local-history policy chose broadcast and beat both native baselines in all five measured pairs. Its median workflow was 0.241677 seconds versus native 0.468469 seconds. Its roughly 26 ms metadata cost is already included. On the other cases, the same local policy chose native; differences between those native-equivalent executions should not be interpreted as a learned optimization.

### Model replies and failures

| Model arm | Requests | Valid replies | Valid BROADCAST | Failed/timed out | Median network seconds | Maximum network seconds |
|---|---:|---:|---:|---:|---:|---:|
| JEV_HISTORY | 20 | 14 | 14 | 6 | 0.904528 | 1.254324 |
| JEV_ZERO | 20 | 14 | 14 | 6 | 0.838258 | 1.254072 |

All 28 valid prospective replies selected BROADCAST. The 12 request failures/timeouts used native fallback; they are not 12 model abstentions. Both Jev arms lost all 20 of their paired comparisons against native, tuned native, and local history in this study. Adding history did not demonstrate better decision quality or end-to-end performance here. This is not proof that every history representation or model setup must fail.

The worst prospective Jev/native observation was **4.492x**, adding **1.366826 seconds**, in `residual_transfer` / `JEV_HISTORY` / repeat 0. This is a single observed regression, not a population tail estimate.

## 3. Known-answer controls separate routing from prediction

A separate 15-request control experiment permuted option names and order. Three exact routing cases and twelve minimum-number cases all returned the correct answer. The numerical cases included both anonymous labels and BROADCAST/MERGE/NATIVE labels, with the smallest supplied number changing across cases.

This supports the narrow conclusion that the client can transmit and validate different choices and that these small exact comparisons are not failing due to a basic response-mapping bug. It does not establish future-runtime prediction accuracy, general numerical capability, or a successful optimizer. The supplied durations in the controls are synthetic known-answer inputs, not leaked evaluation labels.

## 4. Decision caching: useful amortization, not model superiority

The next exploratory study used two further immutable Parquet snapshots. It compared repeated native, tuned, local-history, fresh-history-Jev, and cached-history-Jev workflows. A cache hit skips only the remote request: metadata reads and plan preparation remain charged. First misses are included. Entries are keyed by the registered file-content identity plus the complete canonical request, including model, prompt, plans, configuration, metadata, and historical observations. The TTL is 60 seconds and the cache holds at most eight entries. Failed API responses are not cached.

The registered immutable-dataset assumption matters. Computing its file hashes is not free; that one-time registration cost is included below. A production mutable table would require trustworthy snapshot versions and invalidation, not merely a SQL-text cache key.

### Completed selective repeated-query episode

| Policy | Total time for six queries, seconds |
|---|---:|
| NATIVE | 4.494866 |
| TUNED64 | 4.515810 |
| LOCAL_HISTORY | 2.124640 |
| JEV_HISTORY | 5.620048 |
| CACHED_HISTORY, including dataset registration | 2.738050 |

Cached advice used one successful request and five cache hits in this completed case. With the first miss and 0.064800-second registration included, the episode was **39.08% shorter than native**, but **28.87% longer than local history**. This is evidence that caching can amortize a useful recommendation, not that Jev generated a better policy than the cheap baseline.

### Wide repeated-query episode did not complete

The wide case completed five full randomized rounds, then failed on the sixth cached-policy query. The original cache job therefore has no successful completion marker. Its completed prefix already showed cached advice slower than native, but the failure must not be dropped to report a successful six-query comparison.

The failed cached recommendation was BROADCAST, sourced from API attempt 133. Its reported confidence was 0.08, and the exploratory cache did not use a calibrated confidence gate. Cache identity was still valid; identity validity and response validity are not the same as resource suitability. The original harness stored Py4JJavaError but did not preserve its Java cause, which is an observability defect.

## 5. Fresh-process reproduction exposed an explicit memory failure

A separate no-API diagnostic reused the same immutable wide Parquet dataset in a fresh Spark process, alternating native and forced broadcast. Five queries succeeded. The third broadcast attempt failed with a SparkException reporting insufficient memory to build and broadcast the table. The JVM maximum was 2,147,483,648 bytes; reported heap usage immediately before the failing attempt was 1,325,322,256 bytes. Those observations are not a peak-memory profile.

The separate reproduction supplies the explicit memory-error evidence. It is consistent with the earlier cached-broadcast failure, but cannot retroactively recover the unrecorded Java cause of that original event. We did not increase heap size, disable native AQE, or discard the failure to manufacture a speedup.

This changes the practical design requirement: admission must be checked for every proposed execution, including cache hits, and must account for resources and uncertainty. A recommendation that ran correctly earlier is not automatically safe later.

## 6. Deadlines, safeguards, and what is not yet tested

The new live client uses a reusable async HTTP connection with a 1.25-second overall network deadline and shorter phase timeouts. Cancellation/fallback is tested with mock slow-drip responses; all 16 history/client tests passed. Eight separate cache tests passed, including expiry, snapshot/metadata invalidation, capacity, source-attempt accounting, and rejection of failed responses from the cache.

The overall timeout uses cooperative asyncio cancellation [2]. It limits client-side waiting in this implementation; it is not a hard-real-time guarantee for the whole query, and does not prove that the server stopped work or that no charge was incurred after client cancellation.

A conservative experimental admission screen was written in `admission_guard.py`. It deliberately uses unfiltered footer bytes rather than accepting an optimistic filter fraction as a memory certificate, and checks fresh JVM headroom. Its native execution was blocked by the tool safety layer before it ran. It was not retried through another route. **The screen and its proposed nine checks are not counted as executed or validated.** Its conservative bounds would also reject some selective-wide opportunities; runtime evidence would be needed to admit them less conservatively.

The original AQE observers and runtime-candidate integrations from prior iterations were not replaced by this study. All new successful selections here occurred before query execution, with native AQE retained. There were zero new Jev-directed AQE interventions in this iteration.

## 7. Reproducibility and accounting

The failure-aware audit passed 795 checks covering frozen code/history, exact result comparisons, timing sums, selection ordering, request hashes, cache source/TTL identity, preserved incomplete runs, and cumulative request reconciliation.

| Query batch | Attempts | Correct completions | Failed |
|---|---:|---:|---:|
| lookup_narrow | 30 | 30 | 0 |
| lookup_wide | 30 | 30 | 0 |
| lookup_selective | 30 | 30 | 0 |
| residual_transfer | 30 | 30 | 0 |
| smoke_LEFT_LOOKUP | 5 | 5 | 0 |
| smoke_LEFT_RESIDUAL | 5 | 5 | 0 |
| repeat_selective | 35 | 35 | 0 |
| repeat_wide | 31 | 30 | 1 |
| broadcast_diagnostic | 6 | 5 | 1 |

Of 202 attempts, 156 were measured policy attempts, 30 warmups, and 16 smoke/failure-diagnostic attempts. The total includes only this iteration, not inherited experiments.

The 69 new API reservations are attempts 68-136: 40 prospective requests, 15 known-answer controls, and 14 cache-study requests. Cache hits do not become new reservations. Cumulative usage is 136; no historical ledger bytes were reset.

Returned usage records account for 185,707 input tokens and 3,305 output tokens. Usage is absent for 14 attempts; their billed consumption is unknown, not zero. No invoice was inspected.

Key evidence files under `round_20260920/history_v7/`: `FROZEN_PROTOCOL.json`, `HISTORY.json`, `eval_*_v1/`, `choice_controls_v1/`, `cache_trial_v1/`, `broadcast_diagnostic_v1/`, `FINAL_AUDIT.json`, and `AUDIT_SUMMARY.json`. `audit_final.py` is the failure-aware auditor. The earlier `audit_all.py` is an unexecuted success-only draft and must not be mistaken for the completed audit.

Initial freeze commit: `22bb243786cd1e119b0cc0636057b24b3084223b`. The subsequent result checkpoint is recorded separately after the verified push. Raw data and compiled artifacts are not blindly staged; all committed files are scanned for credentials.

## 8. Practical interpretation and next engineering gate

The evidence supports inexpensive history-based gating and selective reuse as hypotheses worth expanding, not putting an unrestricted remote call on every query. A production-practical advisor must first pass strict candidate/resource admission, then demonstrate incremental value over tuned native and simple selectors after all acquisition, miss, failure, and invalidation costs.

The next decision-quality corpus should have substantially more independently generated query structures and data regimes, and historical training examples should be separated from evaluation at the family level. The learned/remote selector needs to select different strategies for the right reasons, not simply name broadcast repeatedly. Memory, failure recovery, and explicit native fallback should be validated before expanding intervention scope.

The current results do not establish distributed-scale behavior, cold-storage performance, a general trained ranker, calibrated confidence, bounded peak-memory use, broad unseen-query generalization, or a production-safe cache. Five repeats per prospective case and one fully completed six-query cached episode are limited evidence. A negative or mixed result remains a useful outcome of this research.

## Primary-source context

[1] Apache Spark 4.0.1 performance tuning: https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html
[2] Python 3.11 asyncio task cancellation/timeouts: https://docs.python.org/3.11/library/asyncio-task.html
[3] TypeSafe Choice/API/model documentation: https://docs.typesafe.ai/primitives/choice ; https://docs.typesafe.ai/api ; https://docs.typesafe.ai/models . Unauthenticated HTTP 200 retrievals and SHA-256 digests are preserved in `PREFLIGHT.json`; extracted Choice text is in `official_choice_text.txt`.

The numerical findings in this report come from the native raw experiment records and audit, not vendor performance claims. Source documentation supplies interface/context only. No unattended research job was scheduled.
