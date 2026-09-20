"""One additional DEVELOPMENT case after the frozen small-workload holdout.
Does not revise the frozen policy or reuse holdout outcomes for tuning.
Both Jev decisions precede every execution of this independently generated case.
"""
from pathlib import Path
import json,random,statistics,time
from baseline import append,candidates,expected,run_one,session,setup
from live_pilot import features
from safe_jev import Budget,choose
ROOT=Path(__file__).resolve().parent;OUT=ROOT/'expanded_development';OUT.mkdir(exist_ok=False)
c=dict(name='large_filtered_development',n=1200000,m=2000000,keep=10,skew=False,seed=31)
spark=session('JevExpandedDevelopment');spark.sparkContext.setLogLevel('ERROR')
try:
    setup(spark,c);t=time.perf_counter();plans,aliases=candidates(spark);prep=time.perf_counter()-t
    t=time.perf_counter();feat=features(c);feature_s=time.perf_counter()-t
    state={'mode':'whole-query choice BEFORE execution with native AQE enabled; not runtime AQE intervention','engine':'Apache Spark 4.0.1',
           'resources':{'master':'local[2]','driver_heap_bytes':1073741824,'cpu_only':True},'completed_stages':[],
           'candidate_initial_physical_plans':{k:v['initial_plan'] for k,v in plans.items()}}
    criteria={k:('Use native Spark SQL planning with AQE' if k=='NATIVE' else 'Execute the supplied legal '+k+' hinted whole-query variant, keeping AQE on') for k in plans}
    criteria['ABSTAIN']='Use native Spark decision; evidence insufficient to prefer a supplied alternative'
    decisions={}
    for view in ['PLANS','STATS']:
        evidence=dict(state)
        if view=='STATS':evidence['statistics']=feat
        t=time.perf_counter();r,payload=choose(evidence,criteria,'expanded_dev_'+view.lower());r['decision_wall_s']=time.perf_counter()-t
        decisions[view]=r;append(OUT/'decisions.jsonl',{'view':view,'record':r,'payload':payload})
    want=expected(c);rng=random.Random(314159);rows=[]
    for phase,count in [('warmup',2),('measured',7)]:
        for rep in range(count):
            order=list(plans);rng.shuffle(order)
            for position,k in enumerate(order):
                started=time.time();assert all(r['completed_unix']<started for r in decisions.values())
                r=run_one(spark,c,k,want);r.update(phase=phase,rep=rep,position=position,started_unix=started)
                append(OUT/'raw.jsonl',r);rows.append(r)
                if r['status']!='succeeded':raise RuntimeError('candidate_execution_failed')
    stats={}
    for k in plans:
        rs=[r for r in rows if r['phase']=='measured' and r['strategy']==k];ts=[r['to_result_s'] for r in rs]
        stats[k]={'n':len(rs),'median_s':statistics.median(ts),'min_s':min(ts),'max_s':max(ts),
                  'initial_operators':[op for op in ['SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin'] if op in rs[0]['initial_plan']],
                  'final_operators':sorted(set(op for r in rs for op in ['SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]))}
    summary={'case':c,'split':'additional_development_not_holdout','unique_initial_plans':len(plans),'aliases':aliases,'candidate_statistics':stats,
             'preparation_s':prep,'feature_compute_s':feature_s,'model_decisions':decisions,'all_results_correct':True,'api_attempts_cumulative':Budget().count(),
             'policy_unchanged':json.loads((ROOT/'frozen_policy.json').read_text())['policy'],'offline_estimates':{}}
    for view,r in decisions.items():
        k=r['choice'] if r['choice'] in plans and r['status']=='succeeded' else 'NATIVE'
        overhead=prep+r['decision_wall_s']+(feature_s if view=='STATS' else 0)
        summary['offline_estimates'][view]={'selected':k,'selected_execution_median_s':stats[k]['median_s'],'estimated_total_s':stats[k]['median_s']+overhead,
                    'estimated_ratio_to_native':(stats[k]['median_s']+overhead)/stats['NATIVE']['median_s'],'NOT_end_to_end_measurement':True}
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
    (OUT/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','runs':len(rows),'all_correct':True,'api_attempts_cumulative':Budget().count()}))
finally:spark.stop()
