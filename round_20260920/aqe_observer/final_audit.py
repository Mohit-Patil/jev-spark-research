"""Offline integrity/analysis pass over completed native runs. No Spark or model requests.
Checks exact oracle results, state hashes, intervention scope, evidence isolation and budgets.
"""
from pathlib import Path
from functools import lru_cache
import hashlib,json,statistics,subprocess,sys,time
ROOT=Path(__file__).resolve().parent
ROUND=ROOT.parent;WS=ROUND.parent
sys.path.insert(0,str(ROUND))
from baseline import expected
from safe_jev import Budget
from counterfactual_trial_v3 import signature

def load(p):return json.loads(p.read_text())
def lines(p):return [json.loads(s) for s in p.read_text().splitlines() if s.strip()]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
@lru_cache(maxsize=32)
def oracle(n,m,keep,skew,seed):return expected(dict(n=n,m=m,keep=keep,skew=skew,seed=seed))

def main():
    before=Budget().count();report={'audit_unix':time.time(),'kind':'offline audit of completed native experiments','new_api_calls':0}
    tests=[]
    for script in [ROUND/'offline_checks.py',ROOT/'offline_v2_tests.py']:
        p=subprocess.run([sys.executable,str(script)],cwd=WS,capture_output=True,text=True,timeout=60)
        (ROOT/(script.stem+'_final_audit_log.txt')).write_text(p.stdout+p.stderr)
        assert p.returncode==0,script.name
        result=load(ROUND/'offline_tests.json' if script.parent==ROUND else ROOT/'offline_v2_tests.json')
        assert result['successful']
        tests.append({'script':str(script.relative_to(WS)),'tests_run':result['tests_run'],'passed':True})
    report['offline_tests']=tests
    batch_names=['headroom_v2','headroom_v3','observer_check_v2','live_shadow_v2','counterfactual_trial_v3','counterfactual_confirm_v3']
    report['batches']=[];all_rows={};count_total=count_measured=0
    for name in batch_names:
        directory=ROOT/name;complete=load(directory/'COMPLETED.json');assert complete['status']=='succeeded'
        manifest=load(directory/('frozen_trial.json' if name.startswith('counterfactual') else 'manifest.json'))
        cases=manifest.get('cases',[manifest.get('case')]);assert all(c is not None for c in cases)
        bycase={c['name']:c for c in cases};rows=lines(directory/'raw.jsonl');all_rows[name]=rows
        for r in rows:
            c=bycase[r['case']]
            want=oracle(*(c[k] for k in ['n','m','keep','skew','seed']))
            assert r['result']==want and r['correct'] and r['status']=='succeeded',(name,r.get('run_id'))
        measured=sum(r.get('phase')=='measured' for r in rows);warmups=sum(r.get('phase')=='warmup' for r in rows)
        rec={'batch':name,'runs':len(rows),'measured':measured,'warmups':warmups,
             'calibration':sum(r.get('phase')=='calibration' for r in rows),'all_exact_oracle_results_correct':True,
             'raw_sha256':sha(directory/'raw.jsonl'),'completion_sha256':sha(directory/'COMPLETED.json')}
        report['batches'].append(rec);count_total+=len(rows);count_measured+=measured
    report['total_new_query_executions']=count_total;report['total_comparative_measurements']=count_measured
    report['original_preservation']={}
    audit=load(ROUND/'repository_audit_v1.json')
    for b in audit['batches']:
        for name,digest in b['sha256'].items():
            assert sha(WS/name)==digest,name
            report['original_preservation'][name]=True
    oldfreeze=load(ROUND/'frozen_policy.json')
    assert sha(ROUND/'live_pilot.py')==oldfreeze['source_sha256']
    report['original_frozen_policy']=oldfreeze['policy'];report['old_live_pilot_freeze_hash_unchanged']=True
    # All final measurements of NATIVE/MERGE have equal initial fingerprints but differing final joins at 10%.
    rows=all_rows['headroom_v3'];comparisons=[];worst=None
    for case in sorted({r['case'] for r in rows}):
        native={r['rep']:r for r in rows if r['case']==case and r['phase']=='measured' and r['strategy']=='NATIVE'}
        groups={}
        for strategy in ['NATIVE','BROADCAST','MERGE','SHUFFLE_HASH']:
            group=[r for r in rows if r['case']==case and r['phase']=='measured' and r['strategy']==strategy]
            assert len(group)==7
            values=[r['to_result_s'] for r in group]
            groups[strategy]={'median_s':statistics.median(values),'min_s':min(values),'max_s':max(values)}
            if strategy!='NATIVE':
                for r in group:
                    delta=r['to_result_s']-native[r['rep']]['to_result_s'];ratio=r['to_result_s']/native[r['rep']]['to_result_s']
                    if worst is None or ratio>worst['ratio_to_same_round_native']:
                        worst={'case':case,'strategy':strategy,'rep':r['rep'],'ratio_to_same_round_native':ratio,'extra_s':delta,
                         'caveat':'single noisy paired execution, not a general regression estimate'}
        comparisons.append({'case':case,'strategies':groups})
    report['headroom_v3']=comparisons;report['worst_single_whole_query_variant_regression']=worst
    # Revalidate live shadow payload and timing without sending anything.
    decisions=lines(ROOT/'live_shadow_v2'/'decisions.jsonl');queries={r['case']:r for r in all_rows['live_shadow_v2']}
    callbacks={p['run_id']:p for p in lines(ROOT/'live_shadow_v2'/'callback_snapshots.jsonl')}
    for d in decisions:
        payload=d['payload'];r=d['record'];q=queries[d['run_id']];pair=callbacks[d['run_id']]
        text=json.dumps(payload['state'])
        assert 'native_would_choose' not in text and 'stock_cost' not in text and 'oracle_remaining_s' not in text
        assert payload['model']=='jev-1.13.0'
        assert q['started_unix']<=d['received_unix']<=d['started_unix']<=r['completed_unix']<=q['finished_unix']
        assert pair['proposed']['captured_epoch_ms']/1000<=r['completed_unix']
        assert r['status']=='succeeded' and r['choice'] in ('CURRENT','PROPOSED','ABSTAIN')
    report['live_shadow']={'calls':len(decisions),'attempts':[d['record']['attempt'] for d in decisions],
       'choices':[d['record']['choice'] for d in decisions],
       'latency_s':{'min':min(d['record']['latency_s'] for d in decisions),'median':statistics.median(d['record']['latency_s'] for d in decisions),'max':max(d['record']['latency_s'] for d in decisions)},
       'input_tokens':sum(d['record']['usage']['input_tokens'] for d in decisions),'output_tokens':sum(d['record']['usage']['output_tokens'] for d in decisions),
       'before_query_finish':True,'native_labels_and_outcomes_excluded':True,'jev_directed_interventions':0}
    report['counterfactuals']=[]
    for name in ['counterfactual_trial_v3','counterfactual_confirm_v3']:
        directory=ROOT/name;frozen=load(directory/'frozen_trial.json');rows=all_rows[name];pairs=lines(directory/'boundary_pairs.jsonl')
        for filename,digest in frozen['source_sha256'].items():
            source=(ROUND/filename) if filename=='baseline.py' else ROOT/filename
            assert sha(source)==digest,(name,filename)
        for p in pairs:
            assert signature(p)==p['state_signature']
            old=p['current']['stock_cost_value'];new=p['proposed']['stock_cost_value']
            native='PROPOSED' if new<old or (new==old and not p['plans_equal']) else 'CURRENT'
            assert native==p['native_would_choose']
            if p['intervention']:
                assert p['arm']=='CURRENT' and p['target_selected_once'] and p['state_signature']==frozen['target_signature']
                assert p['effective_choice']=='CURRENT' and native=='PROPOSED'
            else:assert p['effective_choice']==native and p['cost_returned_unchanged']
        for r in rows:
            ps=[p for p in pairs if p['run_id']==r['run_id']]
            assert sum(p['intervention'] for p in ps)==r['interventions']<=1
            assert r['target_reached']==any(p['target_selected_once'] for p in ps)
            assert all(r['observer_metrics'][k]==0 for k in ('errors','unmatched','dropped'))
        paired=[];alltrial=[]
        for rep in range(frozen['measured_rounds']):
            group={r['arm']:r for r in rows if r['phase']=='measured' and r['rep']==rep};assert len(group)==2
            a=group['CURRENT'];b=group['PROPOSED']
            alltrial.append({'rep':rep,'whole_saving_current_s':b['to_result_s']-a['to_result_s']})
            if a['target_reached'] and b['target_reached']:
                paired.append({'rep':rep,'current_remaining_s':a['remaining_s'],'proposed_remaining_s':b['remaining_s'],
                   'saving_current_s':b['remaining_s']-a['remaining_s'],
                   'ratio_current_to_proposed':a['remaining_s']/b['remaining_s']})
        savings=[x['saving_current_s'] for x in paired]
        result={'batch':name,'boundary_pairs':len(pairs),'total_actual_interventions':sum(r['interventions'] for r in rows),
           'matched_measured':{arm:sum(r['phase']=='measured' and r['arm']==arm and r['target_reached'] for r in rows) for arm in ['CURRENT','PROPOSED']},
           'attempted_measured_per_arm':frozen['measured_rounds'],'both_arms_matched_rounds':len(paired),
           'paired_median_saving_current_s':statistics.median(savings) if savings else None,
           'current_faster_pairs':sum(x>0 for x in savings),'paired_min_saving_current_s':min(savings,default=None),
           'paired_max_saving_current_s':max(savings,default=None),'paired_records':paired,
           'all_trial_paired_median_whole_saving_current_s':statistics.median(x['whole_saving_current_s'] for x in alltrial),
           'whole_all_trial_warning':'includes target misses with native fallback; do not substitute for a fully matched branch-only effect',
           'max_paired_current_regression_ratio':max((x['ratio_current_to_proposed'] for x in paired),default=None)}
        report['counterfactuals'].append(result)
    report['total_fixed_arm_interventions']=sum(c['total_actual_interventions'] for c in report['counterfactuals'])
    calls=lines(ROUND/'jev_calls.jsonl');attempts=[r['attempt'] for r in calls]
    assert len(set(attempts))==len(attempts) and max(attempts)==Budget().count()
    report['request_accounting']={'attempts_used':Budget().count(),'limit':50,'remaining':50-Budget().count(),
      'sanitized_completed_records':len(calls),'successful':sum(r['status']=='succeeded' for r in calls),
      'input_tokens':sum(r.get('usage',{}).get('input_tokens',0) for r in calls),'output_tokens':sum(r.get('usage',{}).get('output_tokens',0) for r in calls)}
    report['request_accounting']['estimated_usd_at_published_input_rate']=report['request_accounting']['input_tokens']*.042/1000000
    report['request_accounting']['pricing_source']='https://docs.typesafe.ai/models (inspected 2026-09-20): $0.042 per million input tokens; output free. Arithmetic estimate, not billing verification.'
    report['files_exceeding_checkpoint_4mb_limit']=[{'path':str(p.relative_to(WS)),'bytes':p.stat().st_size} for p in ROOT.rglob('*') if p.is_file() and p.suffix in ('.json','.jsonl','.md','.txt','.py','.scala') and p.stat().st_size>4000000]
    assert Budget().count()==before
    report['budget_unchanged_by_audit']=True;report['status']='succeeded'
    (ROOT/'FINAL_AUDIT.json').write_text(json.dumps(report,indent=2))
    summary={k:v for k,v in report.items() if k not in ('headroom_v3','original_preservation')}
    for c in summary['counterfactuals']:c.pop('paired_records')
    print(json.dumps(summary,indent=2),flush=True)
if __name__=='__main__':main()
