"""Read completed V6 artifacts and independently verify results, frozen inputs,
request bytes and cumulative accounting. No new Spark or model execution.
"""
from pathlib import Path
import hashlib,json,statistics,time
R=Path(__file__).resolve().parent;ROUND=R.parent
from authorized_budget import AuthorizedBudget
from representation_inputs import build
from runtime_representation_v6 import oracle
from runtime_benchmark import signature

def read(p):return json.loads(p.read_text())
def rows(p):return [json.loads(x) for x in p.read_text().splitlines() if x]
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    start=AuthorizedBudget().count();out=R/'runtime_representation_v6';done=read(out/'COMPLETED.json')
    raw=rows(out/'raw.jsonl');calls=rows(out/'decisions.jsonl');manifest=read(out/'FROZEN_MANIFEST.json')
    prereg=read(R/'REPRESENTATION_PREREGISTRATION.json');checks=[]
    def check(name,ok):
        if not ok:raise AssertionError(name)
        checks.append(name)
    check('completed_batch_counts',done['status']=='succeeded' and len(raw)==54 and len(calls)==18)
    check('preregistered_before_first_query',prereg['frozen_unix']<min(r['started_unix'] for r in raw))
    check('frozen_before_first_query',manifest['frozen_unix']<min(r['started_unix'] for r in raw))
    for path,h in manifest['hashes'].items():check('source_unchanged_'+path,digest(ROUND/path)==h)
    for c in manifest['cases']:
        want=oracle(c);cr=[r for r in raw if r['case']==c['name']]
        check(c['name']+'_result_oracle',len(cr)==18 and all(r['result']==want and r['correct'] and r['status']=='succeeded' for r in cr))
        for arm in manifest['arms']:check(c['name']+'_'+arm+'_three_measured',sum(r['phase']=='measured' and r['arm']==arm for r in cr)==3)
    lookup={r['run_id']:r for r in raw};expected_attempts=[];applications=0;packet_sizes={}
    for d in calls:
        r=lookup[d['run_id']];e=r['events'][0];rec=d['record'];p=build(d['snapshot'],d['view'])
        check(d['run_id']+'_input_rebuilt',p==d['payload'])
        body=json.dumps(p,separators=(',',':'),allow_nan=False).encode()
        check(d['run_id']+'_packet_hash',hashlib.sha256(body).hexdigest()==rec['request_sha256'])
        check(d['run_id']+'_packet_size',len(body)==rec['request_bytes'])
        if rec['status']=='succeeded':check(d['run_id']+'_response_before_query_finish',rec['completed_unix']<=r['finished_unix'])
        check(d['run_id']+'_snapshot_precedes_response',d['snapshot']['epoch_ms']<=rec['completed_unix']*1000)
        should_apply=rec['status']=='succeeded' and rec['choice']=='BROADCAST' and rec['latency_s']<=1.8 and not e.get('callback_error')
        check(d['run_id']+'_decision_and_execution_agree',bool(e['rule_selected'])==should_apply)
        if should_apply:
            applications+=1
            check(d['run_id']+'_actual_final_broadcast','BroadcastHashJoin' in r['final_plan'].split('== Initial Plan ==')[0])
        check(d['run_id']+'_stage_objects_reused',e['exact_stage_objects_reused'] and e['output_and_distribution_validated'])
        check(d['run_id']+'_signature_recomputed',r['signatures']==[signature(x) for x in r['events']])
        check(d['run_id']+'_both_inputs_materialized',all(e[k]['materialized'] and e[k]['is_runtime'] for k in ('left','right')))
        expected_attempts.append(rec['attempt'])
    check('exact_cumulative_attempt_sequence',sorted(expected_attempts)==list(range(manifest['request_budget_at_start']['attempts_used']+1,manifest['request_budget_at_start']['attempts_used']+19)))
    global_calls=rows(ROUND/'jev_calls.jsonl')
    check('all_18_attempts_in_shared_log',len([x for x in global_calls if x['attempt'] in expected_attempts])==18)
    measured=[r for r in raw if r['phase']=='measured'];table=[];worst={'ratio':0.0}
    for c in done['cases']:
        case=c['case'];t={'case':case}
        for arm,a in c['arms'].items():
            rr=[r for r in measured if r['case']==case and r['arm']==arm]
            val=statistics.median(r['to_result_s'] for r in rr)
            check(case+'_'+arm+'_summary_recomputed',abs(val-a['median_s'])<1e-10)
            t[arm]=val
        table.append(t)
    for r in measured:
        if not r['arm'].startswith('JEV_'):continue
        base=next(x for x in measured if x['case']==r['case'] and x['rep']==r['rep'] and x['arm']=='OFF')
        ratio=r['to_result_s']/base['to_result_s']
        if ratio>worst['ratio']:worst={'case':r['case'],'arm':r['arm'],'rep':r['rep'],'ratio':ratio,'extra_seconds':r['to_result_s']-base['to_result_s']}
    check('applications_match_summary',applications==done['actual_jev_applications'])
    budget=AuthorizedBudget().status();check('no_api_requests_by_audit',start==budget['attempts_used'])
    report={'status':'succeeded','audit_unix':time.time(),'checks':len(checks),'runs':len(raw),'measured_runs':len(measured),
        'new_live_attempts_in_batch':len(calls),'new_live_attempts_by_audit':0,'actual_jev_applications':applications,
        'exact_results_recomputed':True,'all_frozen_source_hashes_match':True,'all_model_request_hashes_match':True,
        'budget':budget,'cases':table,'views':done['views'],'worst_single_jev_regression':worst,
        'raw_sha256':digest(out/'raw.jsonl'),'decisions_sha256':digest(out/'decisions.jsonl'),
        'status_scope':'V6 audited; preceding V4/V5 audit is separate'}
    (R/'AUDIT_REPRESENTATION_V6.json').write_text(json.dumps(report,indent=2))
    (R/'AUDIT_REPRESENTATION_CHECKS_V6.json').write_text(json.dumps(checks,indent=2))
    print(json.dumps(report,indent=2),flush=True)
    lines=['# Cumulative budget and runtime research checkpoint','',
        '20 September 2026. Active authorization: **10,000 cumulative API request attempts**, not 10,000 additional attempts. Historical usage was preserved.',
        '',f"Current verified accounting: **{budget['attempts_used']}/10,000 used; {budget['remaining']} remaining**.",
        '', '## What this continuation executed', '',
        'The authorization update passed 18 offline accounting tests. The repaired prior-run audit passed 448 checks over 137 saved executions; it made no new Spark or model calls. It reconstructed original request-byte ordering with the frozen builder rather than hashing a sorted log representation.',
        '', 'The new representation experiment passed 12 offline feature-isolation tests, then executed 54 native Spark queries: nine execution warmups and 45 randomized measured runs. All aggregate results matched an independently recomputed integer oracle. Eighteen actual Jev requests were charged to the same durable ledger.',
        '', '## Direct end-to-end measurements', '',
        'Each cell is the median of three measured runs in seconds. All original stage work and synchronous advisor overhead are included. A small same-host study is not a production benchmark or a statistical guarantee.', '',
        '| Case | Native defaults | Fixed runtime rule | Native AQE, 32 MiB | Jev full plans | Jev compact features |',
        '|---|---:|---:|---:|---:|---:|']
    for t in table:lines.append('| '+t['case']+' | '+' | '.join(f"{t[a]:.6f}" for a in ('OFF','APPLY','TUNED_AQE','JEV_VERBOSE','JEV_COMPACT'))+' |')
    lines+=['','Full and compact inputs used identical questions and choice labels. Compact input retained verified join semantics and calculated runtime summaries but omitted full plan text and partition ordering; it is deliberately lossy. Model choices used no execution outcomes or winner labels.',
        '', '## Model responses and transport', '', '| View | Requests | Succeeded | NATIVE | BROADCAST | ABSTAIN | Applied | Median HTTP seconds | Input tokens |',
        '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for view,v in done['views'].items():
        lines.append(f"| {view} | {v['calls']} | {v['successful']} | {v['choices']['NATIVE']} | {v['choices']['BROADCAST']} | {v['choices']['ABSTAIN']} | {v['applied']} | {v['median_http_s']:.6f} | {v['input_tokens']} |")
    lines+=['', f"Worst observed individual Jev/default ratio: **{worst['ratio']:.3f}x**, in `{worst['case']}` / `{worst['arm']}`, repeat {worst['rep']}. This is one paired observation, not a workload-wide regression estimate.",
        '', '## Interpretation boundaries', '',
        'Native default, fixed-rule and tuned-AQE comparisons are all measured, not sums of component medians. The rule can have headroom on large inputs yet regress on small inputs; one larger broadcast limit is not universally beneficial. Any compact-input improvement must include prediction failures and network tails, not just request size or token reductions.',
        'No broad prediction-accuracy, Jev superiority, distributed-scale speedup or production readiness is established. Three repeats, one LEFT-join family, generated Range inputs, fixed-width rows and a shared warmed JVM are substantial limitations. Header/network timing includes client/TLS/server effects and is not isolated model inference. In-memory hash footprint and live free-heap observations remain missing.',
        '', '## Accounting and reproducibility', '',
        'The new authorized runner preserves all original ledger bytes and appends under the existing file lock. Failed and interrupted attempts remain charged. Legacy frozen adapters were left unchanged and keep their old 50-attempt writer guard; new runs explicitly inject the authorized budget. The 600-second bridge timeout, local[2] execution and synthetic-only constraints remain unchanged.',
        f"The new audit passed {len(checks)} invariants and recomputed request hashes, source hashes, results, application scope and medians. See `AUDIT_REPRESENTATION_V6.json` and the raw event/decision files under `runtime_representation_v6/`.",
        '', '## Resume', '',
        'Read `authorized_budget.py` and call `AuthorizedBudget().status()` before new requests. Use `BatchBudget` to retain small job-local caps. Preserve all result directories and manifests. Do not treat repeatedly observed templates as untouched holdouts.',
        'Next scientific question: selective invocation and decision quality across several genuinely different query families, rather than spending the expanded budget repeating only these three snapshots. Compare any proposed learner to a cheap local gate and independently tuned native AQE. Existing evidence does not justify deploying the Jev hook.',
        'A separate-executor follow-on was blocked by a tool safety check before execution. That branch was not retried or bypassed. No unattended job has been scheduled.',
        '', '## Primary-source context', '',
        'Spark 4.0.1 documents separate initial and adaptive broadcast thresholds, runtime statistics, and the extra work retained when AQE switches strategies after execution begins: https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html',
        'AQORA is relevant prior research on runtime planner extensions and stage-level feedback; its published results are not Jev results: https://arxiv.org/html/2510.10580v1',
        '', 'These references contextualize the design; the numerical tables above are from this Mac\'s saved measurements, not from the external papers.']
    (R/'RESULTS_BUDGET_AND_V6.md').write_text('\n'.join(lines)+'\n')
    (R/'RESUME.md').write_text('# Active research resume checkpoint\n\nRead RESULTS_BUDGET_AND_V6.md and AUDIT_REPRESENTATION_V6.json. Active cumulative Jev allowance is 10,000 attempts. Use AuthorizedBudget().status(), not the historical header or a legacy LIMIT constant. Do not overwrite frozen result directories or repeat tested cases as fresh holdouts. All jobs in this checkpoint have completed; no detached job is scheduled.\n')
if __name__=='__main__':main()
