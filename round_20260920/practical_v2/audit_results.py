"""Recompute this continuation's evidence from raw records; no Spark jobs or model calls.
Does not claim inherited AQE trials as newly executed. Every completed study's
frozen source digest is checked against the exact currently present files.
"""
from functools import lru_cache
from pathlib import Path
import hashlib,json,math,statistics,subprocess,sys,time
import scale_benchmark as b
from safe_jev import validate_response
HERE=Path(__file__).resolve().parent

def load(p):return json.loads(p.read_text())
def lines(p):return [json.loads(x) for x in p.read_text().splitlines() if x.strip()] if p.exists() else []
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
@lru_cache(maxsize=40)
def oracle(n,m,keep,skew,seed,join):return b.oracle(dict(n=n,m=m,keep=keep,skew=skew,seed=seed,join=join))
def check_rows(rows,cases):
    lookup={c['name']:c for c in cases}
    for r in rows:
        c=lookup[r['case']];want=oracle(*(c[k] for k in ('n','m','keep','skew','seed','join')))
        assert r['status']=='succeeded' and r['correct'] and r['result']==want,(r['case'],r.get('run_id'))

def source_checks(directory):
    manifest=load(directory/'frozen_manifest.json')
    hashes=manifest.get('sources_sha256',manifest.get('source_sha256',{}))
    for name,digest in hashes.items():
        # The transport probe uses filenames; the other new batches use workspace-relative paths.
        p=(HERE/name) if '/' not in name else b.WS/name
        assert p.exists() and sha(p)==digest,('source_changed',directory.name,name)
    return manifest

def main():
    started=time.time();before=b.Budget().count();report={'audit_unix':started,'kind':'new continuation artifact verification','new_api_calls':0,'batches':[]}
    for name,digest in load(HERE/'preflight.json')['source_sha256'].items():
        assert sha(b.ROUND/name)==digest,('inherited_source_changed',name)
    tests=[]
    for name in ('test_scale','test_client','test_policy','test_strong'):
        r=subprocess.run([sys.executable,str(HERE/(name+'.py'))],cwd=b.WS,capture_output=True,text=True,timeout=60)
        (HERE/(name+'_audit_log.txt')).write_text(r.stdout+r.stderr)
        assert r.returncode==0,name
        result=load(HERE/(name+'.json'));assert result['successful'];tests.append({'name':name,'tests':result['tests'],'passed':True})
    report['new_offline_tests']=tests
    total=measured=warmups=diagnostics=0
    scale_names=('recheck_inner_10_v1','left_uniform_10_v1','inner_uniform_50_v1','left_skew80_50_v1')
    report['candidate_headroom']=[]
    for name in scale_names:
        d=HERE/name;assert load(d/'COMPLETED.json')['status']=='succeeded';manifest=source_checks(d)
        rows=lines(d/'raw.jsonl');check_rows(rows,[manifest['case']]);summary=load(d/'summary.json');computed=b.summarize(rows)
        assert computed==summary['candidate_statistics']
        n=sum(r['phase']=='measured' for r in rows);w=len(rows)-n;total+=len(rows);measured+=n;warmups+=w
        report['batches'].append({'batch':name,'queries':len(rows),'measured':n,'warmups':w,'all_correct':True,'raw_sha256':sha(d/'raw.jsonl')})
        report['candidate_headroom'].append({'case':manifest['case']['name'],'medians_s':{k:v['median_s'] for k,v in computed.items()},
          'worst_single_variant_ratio':max(v['worst_ratio_to_same_round_native'] for v in computed.values())})
    report['policy_results']=[];worst=None;all_model_choices=[];confidence=[]
    for name in ('policy_holdout_v1','strong_baseline_v1'):
        d=HERE/name;assert load(d/'COMPLETED.json')['status']=='succeeded';manifest=source_checks(d)
        rows=lines(d/'raw.jsonl');ws=lines(d/'warmups.jsonl');check_rows(rows+ws,manifest['cases'])
        total+=len(rows)+len(ws);measured+=len(rows);warmups+=len(ws)
        decisions={r['run_id']:r for r in lines(d/'decisions.jsonl')}
        report['batches'].append({'batch':name,'queries':len(rows)+len(ws),'measured':len(rows),'warmups':len(ws),'all_correct':True,'raw_sha256':sha(d/'raw.jsonl')})
        recomputed=[]
        for c in manifest['cases']:
            cr=[r for r in rows if r['case']==c['name']];native={r['rep']:r['workflow_s'] for r in cr if r['arm']=='NATIVE'}
            arm_summary={}
            for arm in manifest['arms']:
                rs=[r for r in cr if r['arm']==arm]
                arm_summary[arm]={'n':len(rs),'median_workflow_s':statistics.median(r['workflow_s'] for r in rs),
                  'wins_against_native':sum(r['workflow_s']<native[r['rep']] for r in rs)}
            recomputed.append({'case':c['name'],'arms':arm_summary})
            for r in cr:
                assert math.isclose(r['workflow_s'],r['preparation_s']+r['advice_and_recording_s']+r['collect_s'],abs_tol=1e-8)
                if r['arm']=='JEV_UNGATED':
                    dec=decisions[r['run_id']];rec=dec['record'];payload=dec['payload']
                    assert rec['completed_unix']<=r['execution_started_unix']<=r['finished_unix']
                    assert r['selected']==dec['selected']
                    text=json.dumps(payload['state'])
                    for banned in ('native_would_choose','stock_cost','workflow_s','oracle_remaining_s','candidate_median_s'):
                        assert banned not in text
                    assert payload['state']['completed_stages']==[]
                    all_model_choices.append(rec['choice']);confidence.append(rec.get('confidence'))
                    ratio=r['workflow_s']/native[r['rep']]
                    if worst is None or ratio>worst['ratio']:
                        worst={'batch':name,'case':r['case'],'rep':r['rep'],'ratio':ratio,
                          'extra_s':r['workflow_s']-native[r['rep']],'jev_workflow_s':r['workflow_s'],'native_workflow_s':native[r['rep']]}
        saved=load(d/'summary.json')
        for actual,old in zip(recomputed,saved['cases']):
            assert actual['case']==old['case']
            for arm,v in actual['arms'].items():assert math.isclose(v['median_workflow_s'],old['policies'][arm]['median_workflow_s'],abs_tol=1e-12)
        report['policy_results'].append({'batch':name,'cases':recomputed,'source_freeze_verified':True,'decisions_precede_selected_query_execution':True,
          'caveat':'Spark warmups and other randomized arms can precede a Jev turn. None of those timings were included in the fixed request payload.'})
    report['whole_query_model_choices']=all_model_choices;report['confidence_range']=[min(confidence),max(confidence)]
    report['worst_observed_jev_workflow_regression']=worst
    d=HERE/'transport_v1';manifest=source_checks(d);rows=lines(d/'raw.jsonl');assert len(rows)==10
    for r in rows:assert r['request_sha256']==manifest['payload_sha256'] and r['status']=='succeeded'
    report['transport']=load(d/'summary.json')
    for arm in ('FRESH','POOLED'):
        rs=[r for r in rows if r['mode']==arm and r['phase']=='measured']
        assert math.isclose(statistics.median(r['latency_s'] for r in rs),report['transport']['arms'][arm]['median_roundtrip_s'],abs_tol=1e-12)
    d=HERE/'scaled_aqe_shadow_v1';manifest=source_checks(d);assert load(d/'COMPLETED.json')['status']=='succeeded'
    rows=lines(d/'raw.jsonl');check_rows(rows,manifest['cases']);total+=len(rows);diagnostics+=len(rows)
    pairs=lines(d/'boundary_pairs.jsonl')
    for pair in pairs:
        assert pair['cost_returned_unchanged'] and not pair['intervention']
        old=pair['current']['stock_cost_value'];new=pair['proposed']['stock_cost_value']
        assert pair['native_would_choose']==('PROPOSED' if new<old or (new==old and not pair['plans_equal']) else 'CURRENT')
        for side in ('current','proposed'):
            for stage in pair[side]['stages']:
                sizes=stage.get('shuffle_serialized_partition_bytes')
                if sizes is not None:assert sum(sizes)==stage['partition_bytes_total'] and len(sizes)==stage['partition_count']
    bytag={r['case']:r for r in rows}
    for dec in lines(d/'decisions.jsonl'):
        assert bytag[dec['run_id']]['started_unix']<=dec['received_unix']<=dec['record']['completed_unix']<=bytag[dec['run_id']]['finished_unix']
        assert 'native_would_choose' not in json.dumps(dec['payload']['state'])
    report['runtime_shadow']=load(d/'summary.json')
    report['batches'].append({'batch':d.name,'queries':len(rows),'diagnostics':len(rows),'all_correct':True,'raw_sha256':sha(d/'raw.jsonl')})
    d=HERE/'tuned_native_v1'
    if (d/'COMPLETED.json').exists():
        assert load(d/'COMPLETED.json')['status']=='succeeded';manifest=source_checks(d);rows=lines(d/'raw.jsonl');check_rows(rows,manifest['cases'])
        n=sum(r['phase']=='measured' for r in rows);w=len(rows)-n;total+=len(rows);measured+=n;warmups+=w
        saved=load(d/'summary.json')
        for c in saved['cases']:
            for arm,stats in c['arms'].items():
                ts=[r['to_result_s'] for r in rows if r['case']==c['case'] and r['arm']==arm and r['phase']=='measured']
                assert math.isclose(statistics.median(ts),stats['median_s'],abs_tol=1e-12)
        report['tuned_native']=saved;report['batches'].append({'batch':d.name,'queries':len(rows),'measured':n,'warmups':w,'all_correct':True,'raw_sha256':sha(d/'raw.jsonl')})
    else:report['tuned_native']={'status':'not_completed_not_claimed'}
    report['new_query_totals']={'all':total,'measured':measured,'warmups':warmups,'runtime_diagnostics':diagnostics}
    assert total==measured+warmups+diagnostics
    calls=lines(b.ROUND/'jev_calls.jsonl');numbers=[r['attempt'] for r in calls]
    assert len(numbers)==len(set(numbers)) and sorted(numbers)==list(range(1,b.Budget().count()+1))
    ours=[r for r in calls if r['purpose'].startswith(('transport_','policy_held_','strong_confirm_','scaled_aqe_'))]
    report['request_accounting']={'cumulative_used':b.Budget().count(),'limit':50,'remaining':50-b.Budget().count(),
      'new_requests_this_continuation':len(ours),'new_request_numbers':[r['attempt'] for r in ours],
      'all_new_succeeded':all(r['status']=='succeeded' for r in ours),
      'new_input_tokens':sum(r.get('usage',{}).get('input_tokens',0) for r in ours),
      'new_output_tokens':sum(r.get('usage',{}).get('output_tokens',0) for r in ours)}
    report['oversize_git_artifacts']=[str(p.relative_to(b.WS)) for p in HERE.rglob('*') if p.is_file() and p.suffix in ('.json','.jsonl','.md','.txt','.py','.scala') and p.stat().st_size>4000000]
    assert b.Budget().count()==before
    report['budget_unchanged_by_audit']=True;report['status']='succeeded';report['audit_elapsed_s']=time.time()-started
    (HERE/'FINAL_AUDIT.json').write_text(json.dumps(report,indent=2))
    concise={k:report[k] for k in ('status','new_offline_tests','new_query_totals','request_accounting','confidence_range','worst_observed_jev_workflow_regression','oversize_git_artifacts','audit_elapsed_s')}
    print(json.dumps(concise,indent=2),flush=True)

if __name__=='__main__':main()
