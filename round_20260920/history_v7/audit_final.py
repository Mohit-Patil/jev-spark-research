"""Failure-aware audit of SAVED history_v7 evidence.
No Spark sessions, model calls, key reads or execution of the blocked admission
experiment. Recompute result oracles in ordinary Python; preserve failed rows.
Supersedes the unexecuted success-only audit_all.py draft.
"""
import hashlib,json,statistics,time
from history_core import *
from cache_trial import CASES_CACHE,POLICIES_CACHE,identity
from choice_controls import cases as control_cases

def read(p):return json.loads(p.read_text())

def main():
    before=AuthorizedBudget().count();checks=[];main_rows=[];network=[];summaries=[];query_counts=[]
    def check(name,v):
        if not v:raise AssertionError(name)
        checks.append(name)
    def correctness(label,rr,c):
        want=oracle(c);good=[r for r in rr if r['status']=='succeeded'];bad=[r for r in rr if r['status']!='succeeded']
        check(label+'_successful_results',all(r.get('correct') and r['result']==want for r in good))
        query_counts.append({'batch':label,'attempted':len(rr),'successful_correct':len(good),'failed':len(bad),
          'measured_attempts':sum(r.get('phase')=='measured' for r in rr),
          'warmups':sum(r.get('phase')=='warmup' for r in rr)})
        return good,bad
    frozen=read(R/'FROZEN_PROTOCOL.json');hist=read(R/'HISTORY.json')
    check('history_frozen',sha(R/'HISTORY.json')==frozen['history_sha256'])
    for p,h in frozen['source_hashes'].items():check('source_'+p,sha(WS/p)==h)
    for p,h in hist['source_hashes'].items():check('historical_result_'+p,sha(WS/p)==h)
    for case,c in CASES.items():
        out=R/('eval_'+case+'_v1');done=read(out/'COMPLETED.json');manifest=read(out/'manifest.json')
        rr=rows(out/'raw.jsonl');ds=rows(out/'decisions.jsonl')
        check(case+'_complete',done['status']=='succeeded' and len(rr)==30 and done['queries']==30)
        check(case+'_protocol',manifest['protocol_sha256']==sha(R/'FROZEN_PROTOCOL.json'))
        good,bad=correctness(case,rr,c);check(case+'_zero_query_failures',not bad)
        check(case+'_ten_model_attempts',len(ds)==done['new_api_attempts']==10)
        measured=[r for r in rr if r['phase']=='measured'];bytag={r['run_id']:r for r in measured};summary=read(out/'summary.json')
        for policy in POLICIES:
            xs=[r for r in measured if r['policy']==policy]
            check(case+'_'+policy+'_median',len(xs)==5 and abs(statistics.median(r['to_result_s'] for r in xs)-summary['policies'][policy]['median_s'])<1e-10)
        for r in measured:
            check(r['run_id']+'_freeze_before_query',frozen['created_unix']<r['started_unix'])
            check(r['run_id']+'_timing_sum',abs(r['selection_s']+r['selected_query_s']-r['to_result_s'])<1e-9)
            if r['policy']=='LOCAL_HISTORY':check(r['run_id']+'_local_recomputed',local_select(r['features'],hist['observations'])==r['decision'])
        for d in ds:
            rec=d['record'];r=bytag[d['run_id']];p=d['payload'];with_history=r['policy']=='JEV_HISTORY'
            check(d['run_id']+'_request_hash',hashlib.sha256(encode(p)).hexdigest()==rec['request_sha256'])
            check(d['run_id']+'_before_selected_query',rec['completed_unix']<=r['execution_started_unix'])
            check(d['run_id']+'_choice',r['selected']==effective(rec))
            check(d['run_id']+'_feature_identity',p['state']['metadata']==r['features'])
            check(d['run_id']+'_history_isolation',p['state'].get('historical_observations')==(hist['observations'] if with_history else None))
            check(d['run_id']+'_current_outcomes_absent',p['state']['unknown']=={'actual_post_filter_rows':None,'peak_hash_table_memory':None,'current_candidate_runtimes':None})
        network.extend(ds);main_rows.extend(measured)
        summaries.append({'case':case,'parameters':c,'policies':summary['policies'],'raw_sha256':sha(out/'raw.jsonl')})
    smoke=rows(R/'prepare_v1'/'smoke.jsonl');check('ten_smoke_checks',len(smoke)==10)
    for family in ('LEFT_LOOKUP','LEFT_RESIDUAL'):
        c=dict(n=10000,m=2048,width=128,keep=100,seed=941,family=family)
        sr=[{**r,'status':'succeeded' if r['correct'] else 'failed'} for r in smoke if r['family']==family]
        correctness('smoke_'+family,sr,c)
    controls=R/'choice_controls_v1';cs=read(controls/'COMPLETED.json');cr=rows(controls/'raw.jsonl');spec=control_cases()
    check('control_completion',cs['status']=='succeeded' and len(cr)==15)
    check('control_source_hash',read(controls/'FROZEN.json')['source_sha256']==sha(R/'choice_controls.py'))
    for d,c in zip(cr,spec):
        criteria={k:('Abstain if required information is absent.' if k=='ABSTAIN' else 'Select candidate '+k+'.') for k in c['options']}
        p={'model':MODEL,'state':c['state'],'questions':{'plan':{'type':'choice','instructions':c['instructions'],'criteria':criteria}}}
        check('control_'+str(d['index'])+'_reconstructed',p==d['payload'])
        b=json.dumps(p,separators=(',',':'),allow_nan=False).encode()
        check('control_'+str(d['index'])+'_hash',hashlib.sha256(b).hexdigest()==d['record']['request_sha256'])
        check('control_'+str(d['index'])+'_answer',d['correct']==(d['record']['status']=='succeeded' and d['record']['choice']==c['expected']))
    network.extend(cr)
    cache_root=R/'cache_trial_v1';cache_protocol=read(cache_root/'FROZEN.json')
    check('failed_cache_has_no_false_completion_marker',not (cache_root/'COMPLETED.json').exists())
    check('cache_source_preserved',cache_protocol['source_sha256']==sha(R/'cache_trial.py'))
    cache_brief=[];cache_hits=[];cache_failures=[]
    for name,c in CASES_CACHE.items():
        out=cache_root/name;rr=rows(out/'raw.jsonl');ds=rows(out/'decisions.jsonl');good,bad=correctness(name,rr,c)
        snapshot,files=identity(out/'data');stored=read(out/'IMMUTABLE_DATASET.json')
        check(name+'_immutable_data_unchanged',snapshot==stored['identity'] and files==stored['files'])
        measured=[r for r in rr if r['phase']=='measured'];bytag={r['run_id']:r for r in measured}
        modelrows=[r for r in measured if r['policy'] in ('JEV_HISTORY','CACHED_HISTORY')]
        check(name+'_all_model_decisions_retained',len(ds)==len(modelrows))
        sources={d['record']['attempt']:d['record'] for d in ds if d['record'].get('attempt') is not None}
        for d in ds:
            r=bytag[d['run_id']];rec=d['record'];p=d['payload']
            check(d['run_id']+'_hash',hashlib.sha256(encode(p)).hexdigest()==rec['request_sha256'])
            check(d['run_id']+'_frozen_history',p['state']['historical_observations']==hist['observations'])
            check(d['run_id']+'_decision_precedes_execution',rec['completed_unix']<=r['execution_started_unix'])
            check(d['run_id']+'_freeze',cache_protocol['frozen_unix']<r['started_unix'])
            check(d['run_id']+'_choice',effective(rec)==r['selected'])
            if rec.get('cache_hit'):
                source=sources[rec['source_attempt']]
                check(d['run_id']+'_source',source['status']=='succeeded' and source['choice']==rec['choice'] and source['request_sha256']==rec['request_sha256'])
                check(d['run_id']+'_identity',rec['snapshot_id']==snapshot and rec['attempt'] is None)
                check(d['run_id']+'_ttl',0<=rec['completed_unix']-source['completed_unix']<60.1)
                cache_hits.append({'run_id':d['run_id'],'query_succeeded':r['status']=='succeeded','source_attempt':rec['source_attempt']})
            else:network.append(d)
        complete_rounds=[rep for rep in range(6) if all(any(r['rep']==rep and r['policy']==p and r['status']=='succeeded' for r in measured) for p in POLICIES_CACHE)]
        paired=[r for r in measured if r['rep'] in complete_rounds]
        totals={p:sum(r['to_result_s'] for r in paired if r['policy']==p) for p in POLICIES_CACHE}
        medians={p:statistics.median(r['to_result_s'] for r in paired if r['policy']==p) for p in POLICIES_CACHE}
        b={'case':name,'parameters':c,'attempted_queries':len(rr),'successful_queries':len(good),'failed_queries':len(bad),
          'complete_paired_rounds':complete_rounds,'paired_prefix_totals_s':totals,'paired_prefix_medians_s':medians,
          'registration_s':stored['registration_s'],'raw_sha256':sha(out/'raw.jsonl')}
        if not bad and len(complete_rounds)==6:
            saved=read(out/'summary.json');check(name+'_completed_case_summary',saved['queries']==len(rr)==35)
            for p in POLICIES_CACHE:check(name+'_'+p+'_total',abs(totals[p]-saved['policies'][p]['total_s'])<1e-9)
            total=totals['CACHED_HISTORY']+stored['registration_s']
            b.update(status='completed_case_inside_failed_batch',cached_episode_including_registration_s=total,
              cached_episode_ratio_vs_native=total/totals['NATIVE'],cached_episode_ratio_vs_local=total/totals['LOCAL_HISTORY'],
              policy_details=saved['policies'])
        else:
            check(name+'_failure_retained',len(bad)==1 and rr[-1]['status']=='failed')
            b.update(status='incomplete_due_to_query_failure',performance_scope='Five-round completed prefix only; no completed six-query cached episode or successful full-batch claim.')
            for r in bad:
                entry={k:r.get(k) for k in ('run_id','rep','policy','selected','status','error_type','elapsed_s')}
                entry['source_attempt']=r.get('decision',{}).get('record',{}).get('source_attempt');cache_failures.append(entry)
        cache_brief.append(b)
    diag=R/'broadcast_diagnostic_v1';dr=rows(diag/'raw.jsonl');dd=read(diag/'COMPLETED.json')
    check('diagnostic_completed',dd['status']=='diagnostic_completed' and len(dr)==dd['queries_attempted'])
    good,bad=correctness('broadcast_diagnostic',dr,CASES_CACHE['repeat_wide'])
    check('diagnostic_zero_calls',dd['budget_before']==dd['budget_after'])
    check('diagnostic_original_error_type_preserved',dd['original_cache_failure']=='Py4JJavaError')
    diagnostic_failures=[{k:r.get(k) for k in ('rep','arm','error_type','java_causes','elapsed_s','heap_used_before','heap_used_after','heap_max')} for r in bad]
    globalcalls=rows(ROUND/'jev_calls.jsonl');byattempt={r['attempt']:r for r in globalcalls};ids=[]
    for d in network:
        rec=d['record'];ids.append(rec['attempt']);g=byattempt[rec['attempt']]
        check('request_'+str(rec['attempt'])+'_global_record',rec['request_sha256']==g['request_sha256'] and rec['status']==g['status'] and rec['choice']==g['choice'])
    check('unique_attempts',len(ids)==len(set(ids)))
    current=AuthorizedBudget().status();initial=read(R/'PREFLIGHT.json')['budget']['attempts_used']
    check('no_requests_during_audit',before==current['attempts_used'])
    check('continuous_cumulative_attempts',sorted(ids)==list(range(initial+1,current['attempts_used']+1)))
    model_stats={}
    for policy in ('JEV_ZERO','JEV_HISTORY'):
        ds=[d for case in CASES for d in rows(R/('eval_'+case+'_v1')/'decisions.jsonl') if policy in d['run_id']]
        valid=[d['record'] for d in ds if d['record']['status']=='succeeded'];errors=[d['record'] for d in ds if d['record']['status']!='succeeded']
        model_stats[policy]={'requests':len(ds),'valid':len(valid),'valid_choices':{k:sum(r['choice']==k for r in valid) for k in CRITERIA},
          'failed_requests':len(errors),'error_types':{k:sum(r.get('error_type')==k for r in errors) for k in sorted({r['error_type'] for r in errors})},
          'median_network_s':statistics.median(d['record']['latency_s'] for d in ds),'maximum_network_s':max(d['record']['latency_s'] for d in ds)}
    comparisons={};worst={'ratio':0}
    for policy in ('JEV_ZERO','JEV_HISTORY'):
        xs=[r for r in main_rows if r['policy']==policy];wins={p:0 for p in ('NATIVE','TUNED64','LOCAL_HISTORY')}
        for r in xs:
            for p in wins:
                other=next(x for x in main_rows if x['case']==r['case'] and x['rep']==r['rep'] and x['policy']==p)
                wins[p]+=r['to_result_s']<other['to_result_s']
                if p=='NATIVE':
                    ratio=r['to_result_s']/other['to_result_s']
                    if ratio>worst['ratio']:worst={'ratio':ratio,'case':r['case'],'policy':policy,'rep':r['rep'],'extra_s':r['to_result_s']-other['to_result_s']}
        comparisons[policy]={'paired_comparisons':len(xs),'wins':wins}
    report={'status':'audit_succeeded_with_experimental_failures_preserved','audited_unix':time.time(),'checks':len(checks),
      'query_batches':query_counts,'query_attempts':sum(b['attempted'] for b in query_counts),
      'correct_completed_queries':sum(b['successful_correct'] for b in query_counts),'failed_queries':sum(b['failed'] for b in query_counts),
      'measured_query_attempts':sum(b['measured_attempts'] for b in query_counts),'warmup_queries':sum(b['warmups'] for b in query_counts),
      'new_api_attempts':len(ids),'attempt_range':[min(ids),max(ids)],'budget':current,
      'offline_tests':{'history':read(R/'OFFLINE_TESTS.json'),'cache':read(cache_root/'TESTS.json')},
      'known_answer_controls':cs['groups'],'prospective_cases':summaries,'model_statistics':model_stats,'paired_model_comparisons':comparisons,
      'cache_cases':cache_brief,'cache_hit_records':cache_hits,'original_cache_query_failures':cache_failures,'diagnostic_failures':diagnostic_failures,
      'worst_prospective_jev_regression':worst,'runtime_aqe_interventions_in_iteration':0,
      'admission_guard_status':'Written but execution blocked by tool safety. Not rerouted or counted as tested.',
      'reported_usage':{'input_tokens':sum(d['record'].get('usage',{}).get('input_tokens',0) for d in network),
        'output_tokens':sum(d['record'].get('usage',{}).get('output_tokens',0) for d in network),
        'attempts_without_usage':sum('usage' not in d['record'] for d in network),'meaning':'Known reported usage only; missing usage may still incur cost. No bill inspected.'},
      'limitations':['Synthetic Parquet on one local[2] Mac, 2 GiB heap, warmed JVM and OS cache.',
        'Four prior history cases; three parameter-transfer queries and one residual-condition variant, not broad unseen-family generalization.',
        'Five prospective repeats and six planned cache queries per policy; cache wide case did not complete.',
        'No tuning or repeated testing against evaluation outcomes; cache trial is a separately labeled exploratory follow-up.',
        'Original cache Java cause was not recorded; explicit insufficient-memory cause is from the separate reproduction.',
        'Whole-query pre-execution policies with AQE enabled, not new in-flight AQE interventions.',
        'Network cancellation is cooperative; does not prove server-side cancellation or total resource safety.']}
    dump(R/'FINAL_AUDIT.json',report);dump(R/'AUDIT_CHECKS.json',checks)
    brief={k:report[k] for k in ('status','checks','query_attempts','correct_completed_queries','failed_queries','measured_query_attempts','warmup_queries','new_api_attempts','attempt_range','budget','model_statistics','paired_model_comparisons','known_answer_controls','original_cache_query_failures','diagnostic_failures','reported_usage','worst_prospective_jev_regression')}
    brief['main_medians']=[{'case':s['case'],**{p:v['median_s'] for p,v in s['policies'].items()}} for s in summaries]
    brief['cache_cases']=[{k:v for k,v in c.items() if k not in ('policy_details','parameters')} for c in cache_brief]
    dump(R/'AUDIT_SUMMARY.json',brief);print(json.dumps(brief,indent=2))
if __name__=='__main__':main()
