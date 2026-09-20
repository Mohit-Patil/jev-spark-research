"""Frozen prospective full-query policy comparison. One bounded case/job.
All API calls precede their selected query and exclude evaluation feedback.
Earlier other-arm runs/warmups may exist, but never enter fixed decision inputs.
"""
from pathlib import Path
import argparse, hashlib, json, random, shutil, statistics, sys, threading, time
from history_core import *

def run_policy(spark,c,policy,history,data,out,tag,want,client):
    spark.sparkContext.setJobGroup(tag,'bounded prospective history test',interruptOnCancel=True)
    timer=threading.Timer(60,spark.sparkContext.cancelJobGroup,args=(tag,));timer.start()
    start=time.perf_counter();r={'policy':policy,'run_id':tag,'started_unix':time.time()}
    choice='NATIVE';decision=None
    try:
        corpus.configure(spark,'TUNED64' if policy=='TUNED64' else 'NATIVE')
        if policy=='TUNED64':choice='TUNED64'
        elif policy in ('LOCAL_HISTORY','JEV_ZERO','JEV_HISTORY'):
            f=metadata_features(spark,c,data)
            if policy=='LOCAL_HISTORY':
                decision=local_select(f,history);choice=decision['choice']
            else:
                plans={k:plan_info(spark.sql(query(c,k))) for k in ACTIONS}
                p=payload(f,plans,history if policy=='JEV_HISTORY' else None)
                rec=client.request(p,tag);choice=effective(rec)
                decision={'record':rec,'payload':p,'selected':choice,'decided_unix':time.time()}
                append(out/'decisions.jsonl',{'run_id':tag,**decision})
            r['features']=f
        selection_end=time.perf_counter()
        r['selected']=choice;r['decision']=decision
        r['execution_started_unix']=time.time()
        df=spark.sql(query(c,choice))
        result=sorted([list(x) for x in df.collect()]);end=time.perf_counter()
        r.update(status='succeeded' if result==want else 'failed',correct=result==want,result=result,
          to_result_s=end-start,selection_s=selection_end-start,selected_query_s=end-selection_end,
          finished_unix=time.time())
        r.update(plan_info(df));r['final_plan']=df._jdf.queryExecution().executedPlan().toString()
        if policy.startswith('JEV_'):
            assert decision['record']['completed_unix']<=r['execution_started_unix']
    except Exception as e:
        r.update(status='failed',error_type=type(e).__name__,elapsed_s=time.perf_counter()-start)
    finally:
        timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
    return r

def summarize(rr):
    measured=[r for r in rr if r['phase']=='measured'];native={r['rep']:r for r in measured if r['policy']=='NATIVE'}
    tuned={r['rep']:r for r in measured if r['policy']=='TUNED64'};local={r['rep']:r for r in measured if r['policy']=='LOCAL_HISTORY'}
    out={}
    for p in POLICIES:
        xs=[r for r in measured if r['policy']==p];ts=[r['to_result_s'] for r in xs]
        out[p]={'n':len(xs),'median_s':statistics.median(ts),'min_s':min(ts),'max_s':max(ts),
          'median_selection_s':statistics.median(r['selection_s'] for r in xs),
          'choices':{k:sum(r['selected']==k for r in xs) for k in (*ACTIONS,'TUNED64')},
          'pairs_faster_than_native':sum(r['to_result_s']<native[r['rep']]['to_result_s'] for r in xs),
          'median_paired_saving_vs_native_s':statistics.median(native[r['rep']]['to_result_s']-r['to_result_s'] for r in xs),
          'median_paired_saving_vs_tuned_s':statistics.median(tuned[r['rep']]['to_result_s']-r['to_result_s'] for r in xs),
          'median_paired_saving_vs_local_s':statistics.median(local[r['rep']]['to_result_s']-r['to_result_s'] for r in xs),
          'worst_ratio_vs_native':max(r['to_result_s']/native[r['rep']]['to_result_s'] for r in xs)}
    return out

def main():
    a=argparse.ArgumentParser();a.add_argument('--case',choices=CASES,required=True);a.add_argument('--label',required=True);args=a.parse_args()
    if not args.label.replace('_','').isalnum():raise ValueError('invalid_label')
    freeze=json.loads((R/'FROZEN_PROTOCOL.json').read_text())
    for path,h in freeze['source_hashes'].items():
        if sha(WS/path)!=h:raise ValueError('frozen_source_modified')
    assert sha(R/'HISTORY.json')==freeze['history_sha256']
    c=CASES[args.case];history=json.loads((R/'HISTORY.json').read_text())['observations']
    assert c==freeze['evaluation_cases'][args.case]
    out=R/args.label;out.mkdir(exist_ok=False);budget=BatchBudget(12);before=budget.count()
    if shutil.disk_usage(WS).free<8*1024**3:raise ValueError('disk_headroom')
    start=time.perf_counter();want=oracle(c);dump(out/'oracle.json',want)
    dump(out/'manifest.json',{'protocol_sha256':sha(R/'FROZEN_PROTOCOL.json'),'history_sha256':sha(R/'HISTORY.json'),
      'case_name':args.case,'case':c,'created_unix':time.time(),'reps':5,'policies':POLICIES,'budget_before':before,
      'state':'pre-execution policy, not AQE intervention','random_seed':7219,
      'cache':'One per-strategy warmup; shared warmed JVM and OS cache. No cached DataFrames.',
      'timing':'Policy config, footer feature reads, candidate enumeration if needed, live request and decision recording, selected SQL build and collect. Final logging excluded.',
      'feature_costs':'All footer reads are charged on every LOCAL_HISTORY/JEV workflow; native arms do not pay this custom metadata cost.'})
    spark=corpus.session();spark.sparkContext.setLogLevel('ERROR');rr=[];client=None
    try:
        assert spark.version=='4.0.1' and spark.conf.get('spark.sql.adaptive.enabled')=='true'
        assert spark._jvm.java.lang.Runtime.getRuntime().maxMemory()==2147483648
        dataset=corpus.write_inputs(spark,c,out);dump(out/'dataset.json',dataset)
        client=DeadlineClient(budget,ROUND/'jev_calls.jsonl',deadline=1.25)
        # Equal strategy exposure before measured policy trials. These outcomes are
        # correctness/timing diagnostics, not inputs to either frozen selector.
        rng=random.Random(7219);warm=list(corpus.ARMS);rng.shuffle(warm)
        for i,action in enumerate(warm):
            corpus.configure(spark,action);tag=args.label+'_warm_'+action
            t=time.perf_counter();df=spark.sql(query(c,action));got=sorted([list(x) for x in df.collect()]);dt=time.perf_counter()-t
            wr={'phase':'warmup','case':args.case,'run_id':tag,'policy':action,'selected':action,'rep':-1,
              'correct':got==want,'result':got,'status':'succeeded' if got==want else 'failed','to_result_s':dt}
            append(out/'raw.jsonl',wr);rr.append(wr)
            if got!=want:raise ValueError('warmup_wrong_result')
        for rep in range(5):
            order=list(POLICIES);rng.shuffle(order)
            for pos,policy in enumerate(order):
                if time.perf_counter()-start>400:raise TimeoutError('batch_launch_deadline')
                tag=args.label+'_'+str(rep)+'_'+policy
                rec=run_policy(spark,c,policy,history,out/'data',out,tag,want,client)
                rec.update(phase='measured',case=args.case,rep=rep,position=pos);append(out/'raw.jsonl',rec);rr.append(rec)
                if rec['status']!='succeeded':raise ValueError('execution_failed_preserved')
            print(json.dumps({'case':args.case,'rep':rep,'complete':True}),flush=True)
        ds=rows(out/'decisions.jsonl')
        summary={'status':'succeeded','case':args.case,'queries':len(rr),'measured':25,'all_correct':True,
          'new_api_attempts':budget.used,'budget':AuthorizedBudget().status(),'policies':summarize(rr),
          'model_choices':{p:{'calls':len([d for d in ds if p in d['run_id']]),
             'valid':sum(d['record']['status']=='succeeded' for d in ds if p in d['run_id']),
             'choices':{k:sum(d['record']['status']=='succeeded' and d['record']['choice']==k for d in ds if p in d['run_id']) for k in CRITERIA}}
            for p in ('JEV_ZERO','JEV_HISTORY')},
          'elapsed_s':time.perf_counter()-start}
        dump(out/'summary.json',summary);dump(out/'COMPLETED.json',{'status':'succeeded','queries':len(rr),'new_api_attempts':budget.used,'all_correct':True})
        print(json.dumps(summary,indent=2),flush=True)
    finally:
        if client is not None:client.close()
        spark.stop()
if __name__=='__main__':main()
