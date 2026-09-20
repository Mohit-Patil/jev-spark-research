"""Build the evidence report from the successful audit and update resume documents.
No Spark, model, network or credential access. Existing root checkpoint is archived.
"""
from pathlib import Path
import fcntl,hashlib,json
import scale_benchmark as b
HERE=Path(__file__).resolve().parent

def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+['| '+' | '.join(map(str,row))+' |' for row in rows])

def main():
    audit=json.loads((HERE/'FINAL_AUDIT.json').read_text());assert audit['status']=='succeeded'
    before=b.Budget().count();assert before==audit['request_accounting']['cumulative_used']
    counts=audit['new_query_totals'];req=audit['request_accounting']
    batch_table=table(['New batch','Queries','Measured','Warmups / diagnostics'],[
        [r['batch'],r['queries'],r.get('measured',0),r.get('warmups',r.get('diagnostics',0))] for r in audit['batches']])
    headroom=table(['Development case','Native','Broadcast','Merge','Shuffle hash'],[
        [r['case']]+[f"{r['medians_s'][arm]:.3f} s" for arm in ('NATIVE','BROADCAST','MERGE','SHUFFLE_HASH')]
        for r in audit['candidate_headroom']])
    def policy_table(index,arms):
        return table(['Independent snapshot']+arms,[[c['case']]+[f"{c['arms'][a]['median_workflow_s']:.3f} s" for a in arms]
          for c in audit['policy_results'][index]['cases']])
    policy=policy_table(0,['NATIVE','RULE','JEV_UNGATED'])
    strong=policy_table(1,['NATIVE','BOUNDED_BROADCAST','JEV_UNGATED'])
    tuned=table(['New native-only snapshot','Default 10 MiB','Tuned 64 MiB'],[
        [c['case'],f"{c['arms']['DEFAULT_10MIB']['median_s']:.3f} s",f"{c['arms']['TUNED_64MIB']['median_s']:.3f} s"]
        for c in audit['tuned_native']['cases']])
    transport=audit['transport']['arms']
    transport_table=table(['HTTP client','Measured requests','Median round trip','Observed range','New TCP / TLS handshakes'],[
        [arm,transport[arm]['n'],f"{transport[arm]['median_roundtrip_s']:.3f} s",f"{transport[arm]['min_roundtrip_s']:.3f}–{transport[arm]['max_roundtrip_s']:.3f} s",str(transport[arm]['tcp_connects'])+' / '+str(transport[arm]['tls_handshakes'])]
        for arm in ('FRESH','POOLED')])
    shadow=audit['runtime_shadow']
    coverage=table(['Runtime query','AQE pairs','Changed pairs','Broadcast absent from every candidate pair'],[
      [c['case'],c['pairs'],c['changed_pairs'],c['unavailable_broadcast_in_every_pair']] for c in shadow['coverage']])
    worst=audit['worst_observed_jev_workflow_regression']
    report=f'''# Jev + Spark: practical research checkpoint

**20 September 2026 · native Mac mini · Spark 4.0.1 · Jev 1.13.0**

## Executive conclusion

We now have a measured end-to-end pre-execution Jev workflow that beats default Spark on some larger synthetic queries, including all measured retained-skew trials in two batches. However, the evidence does **not** establish incremental value over cheaper approaches: a bounded broadcast rule beat Jev in all six stronger-baseline paired trials, and a fixed native Spark broadcast-threshold change beat default native in all nine native-control paired trials.

The runtime AQE result remains different. Real boundary capture and live shadow advice work, but useful whole-query broadcast alternatives were absent from the actual native current/proposed pairs in two larger workloads. The only eligible changed pair produced ABSTAIN. No Jev-directed runtime override or production-ready optimization is established.

These are distinct findings: headroom exists; transport overhead can be reduced; an online model workflow can sometimes amortize its overhead; and simpler baselines explain the currently demonstrated gains. The next research question is whether Jev adds value where the best legal strategy genuinely varies after good native tuning—not whether broadcasting can outperform a default threshold.

## 1. What executed in this continuation

The health challenge matched `native-mac-1842`. Both `~/JevResearch` and `~/jev-mac-native` were verified. The existing environment reports macOS ARM64, 16 GiB RAM, Python 3.11.16, Java 17, and Spark 4.0.1. No dependencies were installed and no cloud, Docker or company data was used.

New experiments used `local[2]`, a requested 2 GiB driver heap and 32 shuffle partitions. These settings were identical across arms within a study. Native AQE stayed enabled. The default 10 MiB automatic broadcast threshold stayed unchanged except in the explicitly labeled tuning-control arm. Resolved JVM heap/configuration files are saved in each batch.

{batch_table}

**Total: {counts['all']} newly executed queries = {counts['measured']} measured executions + {counts['warmups']} warmups + {counts['runtime_diagnostics']} runtime diagnostics.** The final audit recomputed exact integer results from the independent Python generator oracle for every stored query. All matched. Thirty-four new offline tests passed on the final rerun.

Separately, the inherited AQE audit was rerun: its 25 tests and all 357 saved query records passed. Those 357 were **audited existing executions**, not new queries launched by this continuation. Its earlier 26 fixed-arm AQE overrides were not Jev-directed and are not re-attributed here.

The new final audit checks raw medians, all frozen source hashes, timing-component sums, per-policy decision-before-action timestamps, absence of outcomes/native labels in model inputs, unchanged stock AQE costs, runtime partition totals, original source preservation and the shared request ledger.

## 2. Larger workloads expose useful whole-query headroom

The initial-plan deduplication repair was already present and verified when this continuation resumed. All four hint policies remain in the inventory, even when initial plans collide. `NATIVE` and `MERGE` can start identically and diverge during later AQE planning.

We retained the two-table integer generator and added LEFT joins so an INNER-join filter could not eliminate most fact-side work. The larger development cases have 12 million fact rows and two million unfiltered dimension rows. Selectivity and retained hot-key skew are explicit. The skewed case's hot key survives the filter; it is not the earlier mislabeled filtered-out-skew scenario.

{headroom}

The recheck has three measured repetitions per candidate; each larger case has five, after two warmup rounds per candidate. Order is randomized. All inputs are uncached generated Range data, with a shared warmed JVM within each batch. OS caches, JIT and garbage-collection state are not reset. This is not disk-based Parquet or distributed network-shuffle validation.

Times include SQL construction, initial-plan capture and collect. They exclude enumeration of the other candidates and final-plan logging. Therefore these are **candidate execution measurements**, not complete selector-workflow speedups or mid-query counterfactual outcomes.

## 3. Connection pooling materially changes the overhead calculation

The original adapter opened a fresh client/connection per request. The new bounded client retains a single connection, with no automatic retries, no redirects, no inherited proxy settings, request/response limits and the same durable request ledger. Trace logging records only event names and relative times, never headers or trace-event information that could contain credentials.

Ten API attempts measured one fixed, previously captured synthetic AQE payload: two warmups plus four randomized FRESH/POOLED blocks. Reusing a saved request here is solely a transport experiment; it supplies no independent plan-selection evidence.

{transport_table}

The measured pooled requests had no new TCP or TLS handshakes. HTTPX documents that a reusable Client can retain connections and reduce repeated handshake overhead [2]. Four measured samples per arm are too few to characterize production tail latency. Server-side caching is unknown, and these are client/network/server round trips, **not isolated Jev inference times**.

The client has phase timeouts and a late-response rejection gate. That gate is not a verified hard wall-clock cancellation mechanism. A production deadline, nonblocking fallback path and cancellation/accounting behavior still need end-to-end fault testing. Mock tests cover rejection, redaction, no retry, model-version validation and native fallback, but they are not production outage tests.

## 4. Direct end-to-end Jev policy measurements

The first policy study uses new 10-million-row snapshots of the same three workload families. Policies, prompts, sources, seeds and repetition counts were frozen before evaluation. Each case has three randomized measured rounds across native, the fixed 10 MiB size rule, and Jev. The first cold API request is included in the workflow that paid for it.

{policy}

Unlike earlier estimates that added separate medians, these are **directly measured workflow wall times**. Jev's interval includes all candidate construction/plan capture, feature preparation, request time, durable decision recording and selected-query execution. Native and the rule do not pay for unused candidate enumeration. Their plan logging happens after the timed result. Pool creation is a one-time process setup; the first network handshake is not removed.

Jev chose BROADCAST in all nine runs. It beat native in two of three selective-left pairs and all three retained-skew pairs; it lost all three half-selective INNER pairs. The simple rule was faster than Jev on the selective LEFT case. On the larger dimension cases, that deliberately transparent 10 MiB rule remained native and missed available headroom.

The rule and Jev receive the same extra input-size evidence: exact counts computed cheaply from the synthetic generator. Those facts are **not** free production catalog statistics or runtime measurements. This source of information must not be credited as knowledge discovered by Jev.

The Jev arm is explicitly exploratory and ungated by confidence, accepting only validated supplied candidate IDs. All fifteen pre-execution choices across this and the next study were BROADCAST; their returned confidence values range from {audit['confidence_range'][0]:.2f} to {audit['confidence_range'][1]:.2f}. We did not calibrate those scores against speedup. This does not establish 100% accuracy or an appropriate production confidence threshold.

Every decision was persisted before executing its selected query. Warmups and other randomized arms can precede a Jev turn, but none of their timings or results entered the fixed request payload. The snapshots are independently generated; the **query families are not unseen**.

## 5. A stronger cheap baseline removes the apparent model advantage

Before inspecting the first policy study's results, a follow-up script was written using the development finding that broadcast won across the tested dimension range. It compares a simple bounded-domain broadcast policy with native and the unchanged Jev prompt, on new 11-million-row snapshots. This rule is limited to the tested two-numeric-column dimension with at most one million filtered rows and a two-million-row source. It is not a general memory-safety guarantee.

{strong}

There are two measured repetitions per arm per snapshot: six matched rounds total. The bounded rule beat Jev in **all six**. Jev still beat default native in five of six runs, but its request overhead had no compensating advantage over taking the same inexpensive action directly.

The worst observed Jev workflow regression across both policy studies was **{100*(worst['ratio']-1):.2f}%**, or **{1000*worst['extra_s']:.2f} ms** extra versus same-round native (`{worst['case']}`, repetition {worst['rep']}). That is an observed single-run regression, not an estimate of a population tail probability.

## 6. Tuned native Spark is the more demanding comparison

A separate native-only control uses new nine-million-row snapshots. It compares default 10 MiB broadcasting with one fixed 64 MiB threshold, not a threshold sweep. No join hints, model calls, or extra exact generator counts are supplied to Spark's planner. Only the threshold differs between paired arms; native AQE stays enabled.

{tuned}

Tuned native won all nine measured paired rounds. Its initial plans were already broadcast joins, rather than waiting for an adaptive conversion or never selecting broadcasting. Spark documents both the automatic threshold and the fact that an AQE conversion to broadcast is less efficient than planning broadcast initially [1].

This control supports the narrower interpretation that the current gains largely reflect plan/configuration selection that does not require Jev. It does **not** justify setting 64 MiB on arbitrary production deployments: row width, hash-table expansion, executor concurrency and distributed broadcast cost have not been stress-tested. The setting was changed only in benchmark Spark sessions, not in global Mac or production configuration.

## 7. Genuine runtime AQE remains a separate problem

The already verified, version-specific V2 observer was recompiled from source using installed Scala/JDK jars. It was attached to the same larger development queries. The proposed/current call pairing is tied to Spark 4.0.1 source/bytecode locations and release commit `29434ea766b0fc3c3bf6eaadb43a8f931133649e`; this is not a stable portable Spark pairwise API.

{coverage}

The three queries completed correctly and produced ten actual AQE comparison pairs, with no unmatched calls, observer errors, callback errors or dropped records. All returned costs remained stock Spark costs. The callback gate sends a request only for the first structurally changed pair per query, so this batch used **one** live API attempt rather than three.

That changed selective-LEFT pair produced **ABSTAIN**, confidence 0.55, with a 1.684-second cold round trip. Native selected PROPOSED. The other two workloads offered no broadcast alternative anywhere in either candidate's plan text. A CURRENT/PROPOSED selector cannot choose an absent option; the whole-query broadcast gains do not provide its missing remaining-work counterfactual.

The model received only the whitelisted snapshot evidence. Stock costs, native choice labels and future execution outcomes were excluded. The response was received before query completion, and no Jev recommendation was applied.

Visible stage leaves are not a complete history of executed work, and consecutive captures do not atomically freeze in-flight tasks. This continuation did not repair that limitation or claim a new runtime intervention. The earlier fixed-arm trials demonstrated feasibility of scoped overrides, not a stable measured benefit or a Jev-selected faster branch.

## 8. What is practical now, and what is not

**Implemented and exercised:** durable request accounting; redacted persistent-client transport; four-policy candidate retention; exact result checks; input/output/time accounting; frozen independent-snapshot policy studies; stronger inexpensive baselines; actual version-pinned AQE observation and a no-change invocation gate; secret-scanned Git checkpoints.

**Measured but narrowly scoped:** selected whole-query Jev workflows beat default native; connection reuse reduced median round-trip latency in a small probe; a cheaper broadcast policy and tuned native achieved the demonstrated advantages without per-query inference.

**Not established:** additional Jev value over tuned native across varied plan-optimum regimes; confidence calibration; learned local-ranker superiority; unseen-family generalization; real file-based or distributed-cluster performance; memory/tail-risk safety; production outage behavior; a hard-deadline intervention path; complete executed-prefix matching; or a beneficial Jev-directed AQE override.

The useful production direction is not a remote call at every AQE comparison. First find cases where tuning and low-cost rules fail, construct legal remaining alternatives, and estimate whether remaining-time headroom can repay the invocation overhead. A cached or off-critical-path advisor is a design possibility, not an implemented result. Cache keys would need engine/config/data-regime/state validity, not just SQL text.

## 9. Next evidence gates — proposed, not executed

1. **Build a varied zero-API corpus first.** Add bounded Parquet scans, wider rows, multiple joins, changing selectivity and cases where broadcast genuinely loses. Preserve separate query-family holdouts. Keep jobs resumable and below bridge limits. Include well-tuned native baselines selected only on development data.
2. **Make feature acquisition realistic.** Compare available catalog/data-source estimates and actual runtime observations. Charge any extra scans or computations. Keep missing values explicit and distinguish serialized bytes from in-memory hash-table requirements.
3. **Strengthen the runtime boundary.** Use an explicit, tested, version-pinned comparison hook and an observed-prefix ledger with clear coverage, legal-candidate validation and state revalidation. Do not treat whole-query hints or shadow recommendations as measured remaining-work labels.
4. **Validate failure and performance envelopes.** Measure strict decision deadlines, native fallback under API errors, cancellation, memory pressure, concurrency, severe regressions and tail latency. The current small sample does not supply deployment SLAs.
5. **Require incremental value.** Only after a varied corpus exists should Jev be compared with a cheap calibrated local numerical model. Current labels overwhelmingly favor broadcast, so fitting a learned ranker now would test little. Any expanded live study must respect the remaining shared allowance or obtain a new explicit allowance first.

These are acceptance gates for further research, not promises of a positive result or work running unattended.

## 10. Request budget, versioning and reproduction

The final audited cumulative budget is **{req['cumulative_used']}/50**, leaving **{req['remaining']}** attempts. This continuation consumed {req['new_requests_this_continuation']} attempts: ten for transport, nine for the initial policy study, six for the stronger baseline and one for actual AQE shadow advice. All succeeded. Returned usage for these new calls totals {req['new_input_tokens']:,} input tokens and {req['new_output_tokens']:,} output tokens. No bill or current token charge is inferred here.

The key remained local and was not returned or inserted into source, prompts or reports. The request ledger and `.env` files remain outside Git. The existing kit's secret-file permissions were not changed.

Milestone `f150ecf` was pushed during this continuation. The final result/report commit is recorded by the later checkpoint command; inspect Git for its exact hash. All new evidence is under `round_20260920/practical_v2/`. Key entry points:

- `FINAL_AUDIT.json` and `audit_results.py`: recompute counts, results, ordering, hashes and accounting with no new Spark queries or Jev requests.
- `scale_benchmark.py` and the four candidate-screen directories: raw candidate timings and actual plans.
- `bounded_client.py`, `transport_v1/`: bounded pooled transport, redacted traces and official-document checks.
- `policy_holdout_v1/`, `strong_baseline_v1/`: frozen manifests, raw complete workflows, model payloads and decisions.
- `tuned_native_v1/`: static native-only control with resolved configuration and actual initial/final plans.
- `scaled_aqe_shadow_v1/`: compiled-source records, raw boundary snapshots and one live recommendation; zero overrides.

Do not overwrite completed directories or change frozen sources to retune these outcomes. A fresh experiment needs a new directory and a frozen manifest. Builds use local installed compiler jars; binary classes/JARs are not automatically versioned. All jobs launched for this checkpoint were inspected for completed status. No detached or unattended research continuation was scheduled here.

## Sources and evidence

Empirical numbers come from this workspace's raw JSONL files, completion markers, frozen manifests and successful final audit; they are not claims derived from vendor benchmarks.

[1] Apache Spark 4.0.1 performance tuning: https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html — statistics, join hints, automatic broadcasting, AQE conversion and custom cost evaluators.

[2] HTTPX Clients: https://www.python-httpx.org/advanced/clients/ — connection reuse and lifecycle.

[3] HTTPX Extensions: https://www.python-httpx.org/advanced/extensions/ — event tracing. Our trace intentionally excludes event-info objects and headers.

[4] TypeSafe API/model pages: https://docs.typesafe.ai/api and https://docs.typesafe.ai/models — fetched successfully without authentication on the native machine; HTTP status, content SHA-256 and endpoint/model checks are in `transport_v1/official_document_checks.json`. The web-fetch tool failed to retrieve those pages, so native documentation checks are the evidence for that verification.
'''
    target=HERE/'RESULTS_2026-09-20.md'
    with target.open('x') as f:f.write(report)
    checkpoint=f'''# Latest practical research checkpoint — 20 September 2026

Private repository: `Mohit-Patil/jev-spark-research`, branch `main`.
Workspace: `~/JevResearch`; native bridge/environment: `~/jev-mac-native`.

Read `round_20260920/practical_v2/RESULTS_2026-09-20.md` and `FINAL_AUDIT.json` first.

This continuation completed {counts['all']} new synthetic Spark queries ({counts['measured']} measured, {counts['warmups']} warmups, {counts['runtime_diagnostics']} runtime diagnostics), all exact results correct. Thirty-four new offline tests passed. The prior 357 queries and 25 tests were separately audited, not re-executed as new experiments.

Jev with a pooled client achieved some directly measured pre-execution gains over default Spark, but a stronger bounded broadcast rule beat Jev in all six paired trials. A fixed 64 MiB native broadcast threshold beat default native in nine of nine paired trials in the tested numeric-schema domain. This is not a generally safe production threshold or evidence of incremental model value.

The scaled actual AQE check captured ten pairs across three queries with stock costs unchanged. Two workloads had no broadcast candidate at any comparison; the one live changed-pair request abstained. No new Jev-directed AQE override or stable remaining-time benefit is established.

Final audited ledger: {req['cumulative_used']}/50 used, {req['remaining']} remaining. Re-read the durable ledger before any subsequent call; never reset it. This continuation used requests 20–45. No additional paid APIs, dependencies, global Spark configuration changes or cloud resources were introduced.

Resume verification with `round_20260920/practical_v2/audit_results.py`. It reruns offline tests and audits saved results, not live Spark or Jev. Preserve all frozen sources and result directories. The next corpus must challenge tuned native and cheap policies and include varying optimal strategies, realistic feature acquisition and stronger runtime-state coverage. Read the report's proposed evidence gates before new live calls.

The prior checkpoint text is preserved at `round_20260920/practical_v2/PREVIOUS_CHECKPOINT.md`; earlier AQE evidence remains under `round_20260920/aqe_observer/`. Milestone f150ecf was pushed; the final report commit is logged by the later checkpoint job. No unattended continuation was scheduled here.
'''
    root=b.WS/'CHECKPOINT.md';assert not root.is_symlink()
    with root.open('r+') as f:
        fcntl.flock(f.fileno(),fcntl.LOCK_EX);old=f.read()
        with (HERE/'PREVIOUS_CHECKPOINT.md').open('x') as previous:previous.write(old)
        f.seek(0);f.write(checkpoint);f.truncate();f.flush()
    log=b.WS/'RESEARCH_LOG.md';assert not log.is_symlink()
    entry=f'''\n## Production-practical continuation — 20 September 2026\n\nHypothesis: longer queries and connection reuse might allow Jev's plan-choice savings to repay its overhead. Experiments: retain all hint policies, add LEFT joins and 9–12 million-row snapshots; compare fresh/pooled HTTP, direct native/rule/Jev workflows, a stronger bounded broadcast rule, native threshold tuning, and actual scaled AQE shadow pairs. Measurements: {counts['all']} new correct queries; 34 new tests passed; pooled median 0.496 s versus fresh 1.511 s in a four-sample-per-arm transport probe. Jev sometimes beats default native end to end, but loses all six stronger-baseline paired comparisons. Tuned native wins all nine default-versus-tuned pairs. Ten scaled AQE pairs expose absent broadcast options in two workloads; the eligible live request abstains. Cumulative requests {req['cumulative_used']}/50, {req['remaining']} left.\n\nInterpretation: online model overhead can amortize, but no incremental Jev advantage over inexpensive policy/configuration or beneficial runtime Jev override is established. Worst observed Jev workflow regression {100*(worst['ratio']-1):.2f}%. New source freezes, result equivalence, timing sums, evidence isolation and accounting passed final audit. Next change: zero-API varied file-based/multi-join corpus and robust native/local baselines before more live prompting; improve legal remaining-plan coverage, prefix-state accounting, deadline/fallback validation and memory/tail-risk tests. These are proposed next experiments, not executed production validation. Full evidence: practical_v2/RESULTS_2026-09-20.md and FINAL_AUDIT.json.\n'''
    with log.open('a') as f:fcntl.flock(f.fileno(),fcntl.LOCK_EX);f.write(entry);f.flush()
    summary={'status':'succeeded','report':str(target.relative_to(b.WS)),'report_bytes':target.stat().st_size,'report_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
      'audit_sha256':hashlib.sha256((HERE/'FINAL_AUDIT.json').read_bytes()).hexdigest(),'new_query_totals':counts,'request_accounting':req}
    (HERE/'REPORT_METADATA.json').write_text(json.dumps(summary,indent=2))
    assert b.Budget().count()==before;print(json.dumps(summary,indent=2))

if __name__=='__main__':main()
