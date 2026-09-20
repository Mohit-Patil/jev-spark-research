# Research log

## GitHub onboarding and saved-record audit

Hypothesis: the current research can be checkpointed without disclosing secrets or changing the cumulative API allowance. Experiment: initialize private GitHub repository, inspect native Git/gh authentication, scan approved files, push initial commit, rerun offline tests, recompute candidate medians and verify prediction ordering. Measurements: see round_20260920/repository_audit_v1.json. Interpretation: saved-record integrity is separate from a newly executed benchmark, production generalization, and genuine AQE intervention. Next change: make reviewed commits after completed experiment/code milestones; retain the untouched original runs and cumulative ledger.

## Native runtime research continuation — 20 September 2026

### Hypothesis: a larger development workload may expose useful plan choice

Experiment: fixed 1.2 million fact / 2 million dimension input rows; vary selectivity then hot-key distribution with native AQE and broadcast threshold retained. The initial 84-query screen exposed an inventory defect: equal initial plans can carry different future AQE behavior. Change: retain all four hint policies in a separate corrected harness. Measurements: corrected 144-query batch (112 measured), all results correct; some alternatives save tens of milliseconds, but the uniform 50% case has no useful advantage. Interpretation: development headroom exists, not a Jev speedup; whole-query labels cannot be applied to an in-flight boundary. Evidence: aqe_observer/headroom_v3/ and MILESTONE_1.md.

### Hypothesis: a low-overhead native AQE observer can capture usable runtime state

Experiment: verify installed 4.0.1 bytecode/source call sites and compile a stock-cost-preserving observer from existing compiler jars. Measurements: 36 queries, 63 paired comparisons, no unmatched calls/errors/drops, all exact results correct. Median observer self-time 1.4-2.4 ms/query. Change: add role, truncation, candidate-collision, statistics and evidence-isolation tests. Ten new tests pass. Interpretation: true runtime observation is implemented, but source-location pairing is version-specific and is not an atomic complete task snapshot.

### Hypothesis: Jev can provide useful advice at a real runtime comparison

Experiment: synchronous loopback callback inside the proposed-cost call; first changed pair on three synthetic queries; plans-only and explicit-statistics views. Measurements: six successes, six ABSTAIN choices, HTTP median 1.031 s; no callback errors or late answers; all query results correct. Requests 14-19 of the shared 50-attempt allowance. Interpretation: live integration works but this experiment supplies no positive plan-selection signal. Round-trip latency dominates these short queries. The prompt was not retuned to force agreement or non-abstention. Evidence: aqe_observer/live_shadow_v2/.

### Hypothesis: actual current/proposed remaining-work outcomes can be compared

Experiment: separate no-network V3 evaluator changes only one comparison on an exact preregistered observed-state hash; randomized CURRENT and native PROPOSED arms, then a fixed 30-round replication. Measurements: 25 + 65 queries, 26 actual overrides, all exact results correct. Both arms matched in 4 initial rounds and 11 replication rounds. CURRENT was 5.15 ms slower by paired median initially, then 2.26 ms faster in replication. Interpretation: no stable demonstrated advantage; variable stage-completion state and millisecond-scale noise matter. No Jev-directed intervention occurred. Preserve all target misses as native fallbacks and do not infer an effect from unmatched subset medians. Evidence: counterfactual_trial_v3/ and counterfactual_confirm_v3/.

### Hypothesis: candidate availability, not just selection, may limit the design

Experiment: inspect all already captured current/proposed operators, with no further Spark or Jev calls. Measurements: retained-skew case had 27 equal sort-merge comparisons and zero broadcast alternatives, despite whole-query broadcast headroom in that generator case. The live-skew generator used a different seed that filtered out the hot key; surviving-skew evidence comes from the seed-313 cases only. Interpretation: a current/proposed-only selector cannot choose an absent alternative. Next research change: examine legal remaining-plan generation, with explicit completed-work and memory accounting, before more prompt variations.

### Final integrity checkpoint

The final audit verified 357 new query results against the independent oracle, reran 25 offline tests, verified original saved-run/source hashes, checked cross-language state signatures and one-boundary intervention scope, and reconciled 19 cumulative API attempts. Estimated published-rate token charge: $0.001879 for all 19 recorded calls; not an inspected bill. Full report: aqe_observer/RESULTS_2026-09-20.md; machine-readable evidence: aqe_observer/FINAL_AUDIT.json. No asynchronous continuation was scheduled.

## Production-practical continuation — 20 September 2026

Hypothesis: longer queries and connection reuse might allow Jev's plan-choice savings to repay its overhead. Experiments: retain all hint policies, add LEFT joins and 9–12 million-row snapshots; compare fresh/pooled HTTP, direct native/rule/Jev workflows, a stronger bounded broadcast rule, native threshold tuning, and actual scaled AQE shadow pairs. Measurements: 218 new correct queries; 34 new tests passed; pooled median 0.496 s versus fresh 1.511 s in a four-sample-per-arm transport probe. Jev sometimes beats default native end to end, but loses all six stronger-baseline paired comparisons. Tuned native wins all nine default-versus-tuned pairs. Ten scaled AQE pairs expose absent broadcast options in two workloads; the eligible live request abstains. Cumulative requests 45/50, 5 left.

Interpretation: online model overhead can amortize, but no incremental Jev advantage over inexpensive policy/configuration or beneficial runtime Jev override is established. Worst observed Jev workflow regression 28.15%. New source freezes, result equivalence, timing sums, evidence isolation and accounting passed final audit. Next change: zero-API varied file-based/multi-join corpus and robust native/local baselines before more live prompting; improve legal remaining-plan coverage, prefix-state accounting, deadline/fallback validation and memory/tail-risk tests. These are proposed next experiments, not executed production validation. Full evidence: practical_v2/RESULTS_2026-09-20.md and FINAL_AUDIT.json.
