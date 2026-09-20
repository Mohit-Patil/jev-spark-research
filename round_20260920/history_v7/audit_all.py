"""Independent saved-record audit. No Spark queries, model requests or key reads.
Checks frozen evidence, request identity, timing, exact results and cache accounting.
"""
import hashlib,json,statistics,time
from history_core import *
from cache_trial import CASES_CACHE,POLICIES_CACHE,identity
from choice_controls import cases as control_cases

def read(p):return json.loads(p.read_text())

def main():
    before=AuthorizedBudget().count();checks=[];summaries=[];all_decisions=[];all_main=[];count=0
    def check(name,v):
        if not v:raise AssertionError(name)
        checks.append(name)
    frozen=read(R/'FROZEN_PROTOCOL.json');hist=read(R/'HISTORY.json')
    check('history_frozen',sha(R/'HISTORY.json')==frozen['history_sha256'])
    for p,h in frozen['source_hashes'].items():check('source_'+p,sha(WS/p)==h)
    for p,h in hist['source_hashes'].items():check('historical_result_'+p,sha(WS/p)==h)
    groups=[]
    for case,c in CASES.items():
        out=R/('eval_'+case+'_v1');done=read(out/'COMPLETED.json');manifest=read(out/'manifest.json');rr=rows(out/'raw.jsonl');ds=rows(out/'decisions.jsonl')
        check(case+'_complete',done['status']=='succeeded' and len(rr)==30 and done['queries']==30)
        check(case+'_protocol',manifest['protocol_sha256']==sha(R/'FROZEN_PROTOCOL.json'))
        want=oracle(c);check(case+'_oracle',all(r['status']=='succeeded' and r['correct'] and r['result']==want for r in rr))
        check(case+'_decision_count',len(ds)==10)
        measured=[r for r in rr if r['phase']=='measured'];check(case+'_measured',len(measured)==25)
        bytag={r['run_id']:r for r in measured};summary=read(out/'summary.json')
        for p in POLICIES:
            xs=[r for r in measured if r['policy']==p]
            check(case+'_'+p+'_median',len(xs)==5 and abs(statistics.median(r['to_result_s'] for r in xs)-summary['policies'][p]['median_s'])<1e-10)
        for r in measured:
            check(r['run_id']+'_freeze_before_execution',frozen['created_unix']<r['started_unix'])
            check(r['run_id']+'_timing_sum',abs(r['selection_s']+r['selected_query_s']-r['to_result_s'])<1e-9)
            if r['policy']=='LOCAL_HISTORY':check(r['run_id']+'_local_recomputed',local_select(r['features'],hist['observations'])==r['decision'])
        for d in ds:
            rec=d['record'];r=bytag[d['run_id']];p=d['payload'];is_history=r['policy']=='JEV_HISTORY'
            check(d['run_id']+'_request_hash',hashlib.sha256(encode(p)).hexdigest()==rec['request_sha256'])
            check(d['run_id']+'_before_selected_query',rec['completed_unix']<=r['execution_started_unix'])
            check(d['run_id']+'_selected_mapping',r['selected']==effective(rec))
            check(d['run_id']+'_feature_identity',p['state']['metadata']==r['features'])
            check(d['run_id']+'_history_isolation',p['state'].get('historical_observations')==(hist['observations'] if is_history else None))
            check(d['run_id']+'_current_outcomes_absent',p['state']['unknown']=={'actual_post_filter_rows':None,'peak_hash_table_memory':None,'current_candidate_runtimes':None})
            all_decisions.append(d)
        mainbrief={'case':case,'family':c['family'],'parameters':c,'policies':summary['policies'],'raw_sha256':sha(out/'raw.jsonl')}
        summaries.append(mainbrief);all_main.extend(measured);count+=len(rr)
    smoke=rows(R/'prepare_v1'/'smoke.jsonl');check('ten_smoke_checks',len(smoke)==10 and all(r['correct'] for r in smoke));count+=len(smoke)
    control_root=R/'choice_controls_v1';cs=read(control_root/'COMPLETED.json');cr=rows(control_root/'raw.jsonl');spec=control_cases()
    check('control_completion',cs['status']=='succeeded' and len(cr)==15)
    check('control_source_hash',read(control_root/'FROZEN.json')['source_sha256']==sha(R/'choice_controls.py'))
    for d,c in zip(cr,spec):
        criteria={k:('Abstain if required information is absent.' if k=='ABSTAIN' else 'Select candidate '+k+'.') for k in c['options']}
        p={'model':MODEL,'state':c['state'],'questions':{'plan':{'type':'choice','instructions':c['instructions'],'criteria':criteria}}}
        check('control_'+str(d['index'])+'_reconstructed',p==d['payload'])
        b=json.dumps(p,separators=(',',':'),allow_nan=False).encode()
        check('control_'+str(d['index'])+'_hash',hashlib.sha256(b).hexdigest()==d['record']['request_sha256'])
        check('control_'+str(d['index'])+'_answer',d['correct']==(d['record']['status']=='succeeded' and d['record']['choice']==c['expected']))
    cache_root=R/'cache_trial_v1';cache_done=read(cache_root/'COMPLETED.json');cache_protocol=read(cache_root/'FROZEN.json')
    check('cache_complete',cache_done['status']=='succeeded' and cache_done['new_queries']==70)
    check('cache_source_frozen',cache_protocol['source_sha256']==sha(R/'cache_trial.py'))
    cache_brief=[];cache_network=[];cache_hits=0
    for name,c in CASES_CACHE.items():
        out=cache_root/name;rr=rows(out/'raw.jsonl');ds=rows(out/'decisions.jsonl');want=oracle(c)
        check(name+'_oracle',len(rr)==35 and all(r['correct'] and r['status']=='succeeded' and r['result']==want for r in rr))
        snapshot,files=identity(out/'data');stored=read(out/'IMMUTABLE_DATASET.json')
        check(name+'_data_unchanged',snapshot==stored['identity'] and files==stored['files'])
        measured=[r for r in rr if r['phase']=='measured'];bytag={r['run_id']:r for r in measured}
        check(name+'_decision_count',len(ds)==12)
        live_sources={d['record']['attempt']:d['record'] for d in ds if d['record'].get('attempt') is not None}
        for d in ds:
            r=bytag[d['run_id']];rec=d['record'];p=d['payload']
            check(d['run_id']+'_hash',hashlib.sha256(encode(p)).hexdigest()==rec['request_sha256'])
            check(d['run_id']+'_history_frozen',p['state']['historical_observations']==hist['observations'])
            check(d['run_id']+'_decision_before_action',rec['completed_unix']<=r['execution_started_unix'])
            check(d['run_id']+'_freeze',cache_protocol['frozen_unix']<r['started_unix'])
            check(d['run_id']+'_selection',effective(rec)==r['selected'])
            if rec.get('cache_hit'):
                source=live_sources[rec['source_attempt']]
                check(d['run_id']+'_cache_source',source['status']=='succeeded' and source['choice']==rec['choice'] and source['request_sha256']==rec['request_sha256'])
                check(d['run_id']+'_cache_epoch',rec['snapshot_id']==snapshot and rec['attempt'] is None)
                check(d['run_id']+'_cache_before_expiry',0<=rec['completed_unix']-source['completed_unix']<60.1)
                cache_hits+=1
            else:cache_network.append(d)
        totals={p:sum(r['to_result_s'] for r in measured if r['policy']==p) for p in POLICIES_CACHE}
        policy_summary=read(out/'summary.json')['policies']
        for p in POLICIES_CACHE:
            xs=[r for r in measured if r['policy']==p]
            check(name+'_'+p+'_totals',len(xs)==6 and abs(totals[p]-policy_summary[p]['total_s'])<1e-9)
        charged=totals['CACHED_HISTORY']+stored['registration_s']
        cache_brief.append({'case':name,'parameters':c,'policies':policy_summary,'episode_totals_s':totals,
          'cache_identity_registration_s':stored['registration_s'],'cached_episode_s_including_registration':charged,
          'cached_mean_s_including_registration':charged/6,'cached_episode_ratio_vs_native':charged/totals['NATIVE'],
          'cached_episode_ratio_vs_local':charged/totals['LOCAL_HISTORY'],'raw_sha256':sha(out/'raw.jsonl')})
        count+=len(rr)
    globalcalls=rows(ROUND/'jev_calls.jsonl');byattempt={r['attempt']:r for r in globalcalls}
    network=all_decisions+cr+cache_network;ids=[d['record']['attempt'] for d in network]
    check('unique_network_attempts',len(ids)==len(set(ids)))
    for d in network:
        rec=d['record'];g=byattempt[rec['attempt']]
        check('attempt_'+str(rec['attempt'])+'_global_log',rec['request_sha256']==g['request_sha256'] and rec['status']==g['status'] and rec['choice']==g['choice'])
    stats={}
    for policy in ('JEV_ZERO','JEV_HISTORY'):
        ds=[d for d in all_decisions if policy in d['run_id']];valid=[d for d in ds if d['record']['status']=='succeeded']
        stats[policy]={'requests':len(ds),'valid':len(valid),'valid_choices':{k:sum(d['record']['choice']==k for d in valid) for k in CRITERIA},
          'failed':len(ds)-len(valid),'error_types':{k:sum(d['record'].get('error_type')==k for d in ds) for k in sorted({d['record'].get('error_type') for d in ds if d['record'].get('error_type')})},
          'median_network_s':statistics.median(d['record']['latency_s'] for d in ds),
          'maximum_network_s':max(d['record']['latency_s'] for d in ds)}
    worst={'ratio':0}
    for r in all_main:
        if not r['policy'].startswith('JEV_'):continue
        n=next(x for x in all_main if x['case']==r['case'] and x['rep']==r['rep'] and x['policy']=='NATIVE')
        ratio=r['to_result_s']/n['to_result_s']
        if ratio>worst['ratio']:worst={'ratio':ratio,'case':r['case'],'policy':r['policy'],'rep':r['rep'],'extra_s':r['to_result_s']-n['to_result_s']}
    after=AuthorizedBudget().status();check('audit_no_api_calls',before==after['attempts_used'])
    check('history_still_frozen',sha(R/'HISTORY.json')==frozen['history_sha256'])
    report={'status':'succeeded','audited_unix':time.time(),'checks':len(checks),'new_spark_queries':count,'measured_policy_queries':160,
      'new_api_attempts':len(ids),'attempt_range':[min(ids),max(ids)],'budget':after,'runtime_aqe_interventions_in_iteration':0,
      'offline_tests':16+8,'known_answer_controls':cs['groups'],'prospective_cases':summaries,'model_statistics':stats,
      'cache_cases':cache_brief,'cache_hits':cache_hits,'worst_prospective_jev_regression':worst,
      'reported_usage':{'input_tokens':sum(d['record'].get('usage',{}).get('input_tokens',0) for d in network),
        'output_tokens':sum(d['record'].get('usage',{}).get('output_tokens',0) for d in network),
        'attempts_without_usage':sum('usage' not in d['record'] for d in network),
        'caveat':'Missing usage is unknown, not zero billed tokens; no invoice or cost claim.'},
      'source_protocol_sha256':sha(R/'FROZEN_PROTOCOL.json'),
      'limitations':['One local[2] Mac, 2 GiB driver, warm JVM/OS cache, synthetic Parquet.',
        'Four historical development snapshots; three parameter-transfer cases and one new residual family, not broad family generalization.',
        'Five prospective repetitions and six repeated-cache queries per case; no population tail or accuracy claim.',
        'Candidate-only performance counterfactuals are not measured here; primary outcomes are actual selected workflows.',
        'Current experiments are pre-execution choices with AQE enabled, not new runtime AQE interventions.',
        'Cache assumes registered data snapshots remain immutable and charges initial content-hash registration in episode totals.',
        '1.25-second network deadline is cooperative cancellation, not a hard-real-time guarantee for whole query or server-side cancellation.']}
    dump(R/'FINAL_AUDIT.json',report);dump(R/'AUDIT_CHECKS.json',checks)
    brief={k:report[k] for k in ('status','checks','new_spark_queries','measured_policy_queries','new_api_attempts','attempt_range','budget','model_statistics','known_answer_controls','cache_hits','worst_prospective_jev_regression','reported_usage')}
    brief['main_medians']=[{'case':s['case'],**{p:v['median_s'] for p,v in s['policies'].items()}} for s in summaries]
    brief['cache_episodes']=[{k:c[k] for k in ('case','episode_totals_s','cached_episode_s_including_registration','cached_episode_ratio_vs_native','cached_episode_ratio_vs_local')} for c in cache_brief]
    print(json.dumps(brief,indent=2))
if __name__=='__main__':main()
