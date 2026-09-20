"""Offline audit of already completed V4/V5 records. No Spark execution,
network, dependency install, key loading, or process launch. Does not audit or
claim ownership of another continuation's practical_v2 experiments.
"""
from pathlib import Path
import hashlib,json,statistics,sys,time
R=Path(__file__).resolve().parent;ROUND=R.parent
sys.path[:0]=[str(ROUND/'practical_v2'),str(ROUND)]
from scale_benchmark import oracle
from safe_jev import Budget

def read(p):return json.loads(p.read_text())
def rows(p):return [json.loads(s) for s in p.read_text().splitlines() if s]
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    before=Budget().count();checks=[];batches=[];allrows=[];events=[];saved=[]
    def check(name,condition):
        if not condition:raise AssertionError(name)
        checks.append(name)
    for name in ('runtime_dev_v1','runtime_holdout_v1'):
        out=R/name;manifest=read(out/'manifest.json');done=read(out/'COMPLETED.json');rr=rows(out/'raw.jsonl')
        check(name+'_completed_count',done['status']=='succeeded' and len(rr)==done['runs']==56)
        check(name+'_exact_result_flags',all(r['correct'] and r['status']=='succeeded' for r in rr))
        for c in manifest['cases']:
            want=oracle(c);case_rows=[r for r in rr if r['case']==c['name']]
            check(name+'_'+c['name']+'_oracle_recomputed',all(r['result']==want for r in case_rows))
        for rel,h in manifest['hashes'].items():check(name+'_source_'+rel,digest(ROUND/rel)==h)
        check(name+'_api_count_unchanged',done['budget_before']==done['budget_after'])
        summary=read(out/'summary.json')
        for s in summary:
            measured=[r for r in rr if r['case']==s['case'] and r['phase']=='measured']
            for arm,t in s['arms'].items():
                ar=[r for r in measured if r['arm']==arm]
                check(name+'_'+s['case']+'_'+arm+'_median',len(ar)==5 and abs(statistics.median(x['to_result_s'] for x in ar)-t['median_s'])<1e-10)
        for r in rr:
            check(name+'_'+r['run_id']+'_single_application',sum(bool(e['rule_selected']) for e in r['events'])<=1)
            if r['arm']!='APPLY':check(name+'_'+r['run_id']+'_no_unrequested_application',not any(e['rule_selected'] for e in r['events']))
            events.extend(r['events'])
        allrows.extend(rr)
        batches.append({'batch':name,'runs':len(rr),'measured':sum(r['phase']=='measured' for r in rr),
            'applications':sum(bool(e['rule_selected']) for r in rr for e in r['events']),'all_results_recomputed_correct':True,
            'raw_sha256':digest(out/'raw.jsonl'),'completion_sha256':digest(out/'COMPLETED.json')})
    pre=read(R/'PREREGISTERED_HOLDOUT.json');hold=rows(R/'runtime_holdout_v1'/'raw.jsonl')
    check('holdout_freeze_before_first_query',pre['frozen_unix']<min(r['started_unix'] for r in hold))
    check('holdout_source_still_frozen',digest(R/'runtime_holdout.py')==pre['source_sha256'])
    check('extension_still_frozen',digest(R/'RuntimeCandidateExtension.scala')==pre['extension_sha256'])
    out=R/'runtime_jev_v5';manifest=read(out/'FROZEN_LIVE_MANIFEST.json');done=read(out/'COMPLETED.json');rr=rows(out/'raw.jsonl')
    check('v5_completed_count',done['status']=='succeeded' and len(rr)==22)
    cases=manifest['cases']+[dict(name='callback_fault_fixture',n=1200000,m=2000000,keep=500,skew=True,seed=313,join='LEFT'),dict(name='oversize_build_guard',n=1200000,m=4000000,keep=500,skew=False,seed=313,join='LEFT')]
    for c in cases:
        want=oracle(c);cr=[r for r in rr if r['case']==c['name']]
        check('v5_'+c['name']+'_oracle_recomputed',bool(cr) and all(r['correct'] and r['result']==want and r['status']=='succeeded' for r in cr))
    decisions=rows(out/'model_decisions.jsonl');bytag={r['run_id']:r for r in rr}
    check('exactly_four_unique_live_calls',len(decisions)==len({d['record']['attempt'] for d in decisions})==4)
    outcomes={}
    for d in decisions:
        rec=d['record'];r=bytag[d['run_id']];p=d['payload'];e=r['events'][0]
        check(d['run_id']+'_reply_before_finish',rec['completed_unix']<=r['finished_unix'])
        check(d['run_id']+'_snapshot_before_reply',d['snapshot']['epoch_ms']<=rec['completed_unix']*1000)
        check(d['run_id']+'_live_freeze_before_query',manifest['frozen_unix']<r['started_unix'])
        body=json.dumps(p,separators=(',',':'),allow_nan=False).encode()
        check(d['run_id']+'_request_hash_matches',hashlib.sha256(body).hexdigest()==rec['request_sha256'])
        check(d['run_id']+'_no_outcome_fields',set(p['state'])=={'decision','engine','resources','join_inputs','native_remaining_join','broadcast_remaining_join','work_accounting','measurement_units'})
        check(d['run_id']+'_live_scope',set(p['questions']['plan']['criteria'])=={'NATIVE','BROADCAST','ABSTAIN'})
        applied=rec['status']=='succeeded' and rec['choice']=='BROADCAST' and rec['latency_s']<=1.8
        check(d['run_id']+'_returned_choice_applied_exactly',e['rule_selected']==applied)
        final='BroadcastHashJoin' in r['final_plan'].split('== Initial Plan ==')[0]
        check(d['run_id']+'_applied_final_join',not applied or final)
        outcomes[d['run_id']]={'attempt':rec['attempt'],'choice':rec['choice'],'confidence':rec['confidence'],
            'http_latency_s':rec['latency_s'],'query_s':r['to_result_s'],'applied':applied}
    check('one_real_jev_application',sum(v['applied'] for v in outcomes.values())==1)
    for r in rr:
        check('v5_'+r['run_id']+'_single_application',sum(e['rule_selected'] for e in r['events'])<=1)
        events.extend(r['events'])
    live_names={c['name'] for c in manifest['cases']}
    live=[r for r in rr if r['case'] in live_names]
    faults=read(out/'FAULT_TESTS.json')
    check('six_recorded_fault_tests_passed',len(faults['tests'])==6 and all(x['passed'] for x in faults['tests']))
    worst={'ratio':0}
    for r in live:
        if r['arm']!='JEV':continue
        baseline=next(x for x in live if x['case']==r['case'] and x['rep']==r['rep'] and x['arm']=='OFF')
        ratio=r['to_result_s']/baseline['to_result_s']
        if ratio>worst['ratio']:worst={'run_id':r['run_id'],'ratio':ratio,'extra_seconds':r['to_result_s']-baseline['to_result_s']}
    batches.append({'batch':'runtime_jev_v5','runs':len(rr),'measured':len(live),
        'applications':sum(e['rule_selected'] for r in rr for e in r['events']),
        'real_jev_applications':1,'fake_callback_applications':1,'all_results_recomputed_correct':True,
        'raw_sha256':digest(out/'raw.jsonl'),'completion_sha256':digest(out/'COMPLETED.json')})
    allrows.extend(rr)
    out=R/'build_smoke_v2';smoke=read(out/'COMPLETED.json')
    c=smoke['case'];want=oracle(c);sr=[read(out/(mode+'_execution.json')) for mode in ('OFF','SHADOW','APPLY')]
    check('smoke_recomputed_oracle',all(r['correct'] and r['result']==want for r in sr))
    ee=read(out/'events.json')['events'];events.extend(ee);allrows.extend(sr)
    batches.append({'batch':'build_smoke_v2','runs':3,'measured':0,'applications':sum(e['rule_selected'] for e in ee),'all_results_recomputed_correct':True,'completion_sha256':digest(out/'COMPLETED.json')})
    for i,e in enumerate(events):
        check('event_'+str(i)+'_completed_input_reuse',e['exact_stage_objects_reused'] and e['output_and_distribution_validated'] and all(e[s]['materialized'] and e[s]['is_runtime'] for s in ('left','right')))
        check('event_'+str(i)+'_build_admission',0<e['right']['rows']<=1250000 and 0<e['right']['runtime_size_bytes']<=33554432)
    globalcalls=rows(ROUND/'jev_calls.jsonl');attempts={d['record']['attempt'] for d in decisions}
    check('four_calls_present_in_global_sanitized_log',len([c for c in globalcalls if c['attempt'] in attempts])==4)
    after=Budget().count();check('audit_no_api_attempts',before==after)
    report={'status':'succeeded','audit_unix':time.time(),'scope':'production_v4 experiments only; inherited practical_v2 excluded',
        'invariants_checked':len(checks),'all_checks_passed':True,'batches':batches,
        'new_executions_total':len(allrows),'comparative_measurements':sum(b['measured'] for b in batches),
        'actual_extension_applications_total':sum(b['applications'] for b in batches),
        'jev_directed_applications':1,'live_decisions':outcomes,'worst_single_jev_regression':worst,
        'new_api_attempts_in_this_continuation':4,'api_attempts_consumed_by_audit':0,'global_budget_used':after,'global_limit':50,'global_remaining':50-after,
        'limits':['same-host local[2] synthetic data only','no statistically powered Jev decision-quality result','no isolated remaining-time counterfactual from this audit','multi-process follow-on blocked before execution; no bypass attempted']}
    with (R/'AUDIT_COMPLETED.json').open('w') as f:json.dump(report,f,indent=2)
    with (R/'AUDIT_CHECKS.json').open('w') as f:json.dump(checks,f,indent=2)
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
