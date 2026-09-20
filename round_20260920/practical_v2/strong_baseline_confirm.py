"""Predeclared stronger-baseline confirmation on another set of synthetic snapshots.
The bounded broadcast arm is justified by the THREE DEVELOPMENT screens, not
by inspecting policy_holdout_v1 outcomes. It is not a general memory-safety rule.
Six maximum new Jev requests. Original policy/prompt/source remains unchanged.
"""
from pathlib import Path
import hashlib,json,random,statistics,threading,time
import policy_holdout as p
b=p.b
HERE=Path(__file__).resolve().parent
CASES=[
 dict(name='confirm_left_selective',n=11000000,m=2000000,keep=100,skew=False,seed=2189,join='LEFT'),
 dict(name='confirm_inner_half',n=11000000,m=2000000,keep=500,skew=False,seed=2267,join='INNER'),
 dict(name='confirm_left_skew',n=11000000,m=2000000,keep=500,skew=True,seed=2399,join='LEFT')]
ARMS=['NATIVE','BOUNDED_BROADCAST','JEV_UNGATED']

def bounded_strategy(c):
    # Explicitly limited to this already exercised synthetic schema and dimension range.
    f=b.features(c)
    return 'BROADCAST' if c['m']<=2000000 and f['filtered_dimension_rows']<=1000000 else 'NATIVE'

def broadcast_workflow(spark,c,heap,want,tag):
    spark.sparkContext.setJobGroup(tag,'bounded broadcast comparator',interruptOnCancel=True)
    timer=threading.Timer(90,spark.sparkContext.cancelJobGroup,args=(tag,));timer.start()
    begin=time.perf_counter();r={'case':c['name'],'arm':'BOUNDED_BROADCAST','run_id':tag,'started_unix':time.time()}
    try:
        strategy=bounded_strategy(c);df=spark.sql(b.query(c,strategy));prepared=time.perf_counter()
        execution_start=time.time();got=sorted([list(x) for x in df.collect()]);end=time.perf_counter()
        r.update(status='succeeded' if got==want else 'failed',correct=got==want,result=got,selected=strategy,
          workflow_s=end-begin,preparation_s=prepared-begin,advice_and_recording_s=0,collect_s=end-prepared,
          execution_started_unix=execution_start,finished_unix=time.time(),decision=None,
          initial_plan=b.plan_info(df)['initial_plan'],final_plan=df._jdf.queryExecution().executedPlan().toString())
    except Exception as e:r.update(status='failed',error_type=type(e).__name__,elapsed_s=time.perf_counter()-begin)
    finally:timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
    return r

def main():
    before=p.Budget().count()
    if before+6>50:raise RuntimeError('insufficient_shared_budget')
    out=HERE/'strong_baseline_v1';out.mkdir(exist_ok=False)
    sources=[Path(__file__),HERE/'policy_holdout.py',HERE/'scale_benchmark.py',HERE/'bounded_client.py',b.ROUND/'baseline.py',b.ROUND/'safe_jev.py']
    spec={'frozen_unix':time.time(),'cases':CASES,'arms':ARMS,'measured_rounds':2,'candidate_warmups_per_case':1,
      'order_seed':6221,'maximum_new_api_attempts':6,'cold_first_request_included':True,
      'selection_basis':'All three larger DEVELOPMENT screens favored BROADCAST. Challenge Jev against that cheap action inside the tested synthetic dimension-size domain; no policy_holdout outcomes are read by this program.',
      'limits':'Only 2 randomized repetitions per arm per snapshot. New generator snapshots, not new query families. Not production memory validation. Ungated Jev is exploratory, not a deployed confidence policy.',
      'model_prompt_unchanged':True,'baseline_rule':'BROADCAST if synthetic m <= 2 million and filtered dimension rows <= 1 million; else native. Applies only to the tested numeric schema and 2 GiB local driver.',
      'sources_sha256':{str(f.relative_to(b.WS)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sources}}
    (out/'frozen_manifest.json').write_text(json.dumps(spec,indent=2))
    client=p.BoundedClient();spark=b.session('JevStrongBaselineConfirmation');spark.sparkContext.setLogLevel('ERROR')
    rng=random.Random(6221);rows=[];warms=[];summaries=[];started=time.perf_counter()
    try:
        assert spark.version=='4.0.1';assert spark.conf.get('spark.sql.adaptive.enabled')=='true'
        heap=spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
        (out/'resolved_config.json').write_text(json.dumps({'jvm_max_heap_bytes':heap,'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'),'auto_broadcast_threshold':spark.conf.get('spark.sql.autoBroadcastJoinThreshold')},indent=2))
        for c in CASES:
            want=b.oracle(c);b.setup(spark,c);(out/(c['name']+'_oracle.json')).write_text(json.dumps(want))
            arms=list(b.STRATEGIES);rng.shuffle(arms)
            for strategy in arms:
                r=b.run(spark,c,strategy,want,'strong_warm_'+c['name']+'_'+strategy)
                b.append(out/'warmups.jsonl',r);warms.append(r)
                if r['status']!='succeeded':raise RuntimeError('warmup_failure')
            for rep in range(2):
                arms=list(ARMS);rng.shuffle(arms)
                for pos,arm in enumerate(arms):
                    if time.perf_counter()-started>400:raise RuntimeError('batch_launch_budget')
                    tag='strong_'+c['name']+'_'+str(rep)+'_'+arm.lower()
                    if arm=='BOUNDED_BROADCAST':r=broadcast_workflow(spark,c,heap,want,tag)
                    else:r=p.workflow(spark,c,arm,client,heap,want,tag,out)
                    r.update(phase='measured',rep=rep,position=pos);b.append(out/'raw.jsonl',r);rows.append(r)
                    if r['status']!='succeeded':raise RuntimeError('query_failure_preserved')
                print(json.dumps({'case':c['name'],'round':rep,'completed':True}),flush=True)
            case_rows=[r for r in rows if r['case']==c['name']];native={r['rep']:r['workflow_s'] for r in case_rows if r['arm']=='NATIVE'}
            baseline={r['rep']:r['workflow_s'] for r in case_rows if r['arm']=='BOUNDED_BROADCAST'}
            summary={'case':c['name'],'policies':{}}
            for arm in ARMS:
                group=[r for r in case_rows if r['arm']==arm]
                summary['policies'][arm]={'n':len(group),'median_workflow_s':statistics.median(r['workflow_s'] for r in group),
                  'selected':[r['selected'] for r in group],'model_choices':[r['decision']['choice'] for r in group if r['decision']],
                  'confidences':[r['decision'].get('confidence') for r in group if r['decision']],
                  'faster_than_native_pairs':sum(r['workflow_s']<native[r['rep']] for r in group),
                  'faster_than_bounded_broadcast_pairs':sum(r['workflow_s']<baseline[r['rep']] for r in group),
                  'worst_ratio_to_native':max(r['workflow_s']/native[r['rep']] for r in group)}
            summaries.append(summary);print(json.dumps(summary),flush=True)
        attempts=sum(r.get('decision') is not None and 'attempt' in r['decision'] for r in rows)
        assert p.Budget().count()==before+attempts
        result={'status':'succeeded','cases':summaries,'measured_workflows':len(rows),'spark_warmups':len(warms),
          'all_results_correct':True,'actual_new_api_attempts':attempts,'attempts_before':before,'attempts_after':p.Budget().count(),
          'timing':'Direct whole-policy wall timings, no summed component estimates','scope':spec['limits']}
        (out/'summary.json').write_text(json.dumps(result,indent=2))
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','measured':len(rows),'warmups':len(warms),'all_correct':True,'new_api_attempts':attempts}))
        print(json.dumps(result,indent=2),flush=True)
    finally:client.close();spark.stop()

if __name__=='__main__':main()
