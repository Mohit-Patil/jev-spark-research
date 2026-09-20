"""Pre-execution Jev characterization; combined policy costs are OFFLINE ESTIMATES.
Predictions for both feature views are persisted before any candidate executes.
Validation policy is frozen before independently generated held-out snapshots.
"""
from pathlib import Path
import argparse, hashlib, json, random, statistics, time
from baseline import append, candidates, expected, run_one, session, setup
from safe_jev import Budget, MODEL, choose
ROOT=Path(__file__).resolve().parent
VALIDATION=[dict(name='val_selective',n=350000,m=50000,keep=10,skew=False,seed=73),
            dict(name='val_half',n=350000,m=50000,keep=500,skew=False,seed=137),
            dict(name='val_skew80',n=350000,m=50000,keep=500,skew=True,seed=211)]
HOLDOUT=[dict(name='holdout_uniform',n=450000,m=60000,keep=200,skew=False,seed=919),
         dict(name='holdout_skew80',n=450000,m=60000,keep=200,skew=True,seed=907)]
POLICIES=['NATIVE','RULE']+[view+'_'+str(t) for view in ['PLANS','STATS'] for t in [0.0,0.5,0.8,0.95]]

def features(c):
    dim_rows=sum(1 for k in range(c['m']) if (k*17+c['seed'])%1000<c['keep'])
    return {'source':'known synthetic generator inputs and analytically counted dimension keys; NOT AQE runtime observations',
            'fact_input_rows':c['n'],'dimension_unfiltered_rows':c['m'],'dimension_filtered_rows':dim_rows,
            'dimension_payload_bytes_estimate':dim_rows*16,'payload_row_width_assumption_bytes':16,
            'hash_table_memory_bytes':None,'skew_generator_hot_key_fraction':0.8 if c['skew'] else 0,
            'actual_shuffle_partition_distribution':None,'available_runtime_stages':[],
            'auto_broadcast_threshold_bytes':10485760}

def selection(policy,cands,rec,feat):
    if policy=='NATIVE': return 'NATIVE'
    if policy=='RULE':
        if feat['dimension_payload_bytes_estimate']<=feat['auto_broadcast_threshold_bytes']:
            for name in ['NATIVE','BROADCAST']:
                if name in cands and 'BroadcastHashJoin' in cands[name]['initial_plan']: return name
        return 'NATIVE'
    view,threshold=policy.split('_'); r=rec[view]
    if r['status']!='succeeded' or r['choice']=='ABSTAIN' or r['confidence']<float(threshold): return 'NATIVE'
    return r['choice'] if r['choice'] in cands else 'NATIVE'

def main():
    p=argparse.ArgumentParser();p.add_argument('--phase',choices=['validation','holdout'],required=True);a=p.parse_args()
    cases=VALIDATION if a.phase=='validation' else HOLDOUT
    frozen=None
    if a.phase=='holdout':
        frozen=json.loads((ROOT/'frozen_policy.json').read_text())
        if frozen['source_sha256']!=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(): raise RuntimeError('code_changed_after_freeze')
    out=ROOT/('live_'+a.phase);out.mkdir(exist_ok=False)
    manifest={'phase':a.phase,'model':MODEL,'cases':cases,'seed':271828,'warmups':2,'reps':5,
              'costs':'OFFLINE ESTIMATES combining separately measured execution and selector costs; NOT end-to-end speedups',
              'client':'fresh HTTP client/connection per request; includes network and TLS; not isolated server inference',
              'frozen_policy':frozen,'feature_view_caveat':'plans-only still includes Range cardinalities and filter expressions inherent in physical plans',
              'cache':'uncached synthetic Range inputs; warmed shared JVM; no cached DataFrames'}
    spark=session('JevLivePilot_'+a.phase);spark.sparkContext.setLogLevel('ERROR');rng=random.Random(271828); summaries=[]
    try:
        if spark.conf.get('spark.sql.autoBroadcastJoinThreshold')!='10485760b':
            # Accept the common numeric spelling as well, but do not silently assume a different threshold.
            if spark.conf.get('spark.sql.autoBroadcastJoinThreshold')!='10485760': raise RuntimeError('unexpected_broadcast_threshold')
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
        for c in cases:
            setup(spark,c)
            t=time.perf_counter();cands,aliases=candidates(spark); prep=time.perf_counter()-t
            t=time.perf_counter();feat=features(c);feat_s=time.perf_counter()-t
            state={'mode':'whole-query choice BEFORE execution with native AQE enabled; not runtime AQE intervention',
                   'engine':'Apache Spark 4.0.1','resources':{'master':'local[2]','driver_heap_bytes':1073741824,'cpu_only':True},
                   'completed_stages':[],
                   'candidate_initial_physical_plans':{k:v['initial_plan'] for k,v in cands.items()}}
            criteria={k:('Use native Spark SQL planning with AQE' if k=='NATIVE' else 'Execute the supplied legal '+k+' hinted whole-query variant, keeping AQE on') for k in cands}
            criteria['ABSTAIN']='Use native Spark decision; evidence insufficient to prefer a supplied alternative'
            rec={}
            for view in ['PLANS','STATS']:
                evidence=dict(state)
                if view=='STATS':evidence['statistics']=feat
                t=time.perf_counter();r,payload=choose(evidence,criteria,c['name']+'_'+view.lower());r['decision_wall_s']=time.perf_counter()-t
                rec[view]=r
                append(out/'decisions.jsonl',{'case':c['name'],'view':view,'record':r,'payload':payload})
            # No candidate execution, not even a warmup, occurs before both records above.
            want=expected(c);rows=[]
            for phase,count in [('warmup',2),('measured',5)]:
                for rep in range(count):
                    order=list(cands);rng.shuffle(order)
                    for position,k in enumerate(order):
                        started=time.time()
                        assert all(r['completed_unix']<=started for r in rec.values())
                        r=run_one(spark,c,k,want);r.update(phase=phase,rep=rep,position=position,started_unix=started)
                        append(out/'raw.jsonl',r);rows.append(r)
                        if r['status']!='succeeded':raise RuntimeError('candidate_failed_run_preserved')
            med={k:statistics.median(r['to_result_s'] for r in rows if r['strategy']==k and r['phase']=='measured') for k in cands}
            est={}
            for policy in POLICIES:
                selected=selection(policy,cands,rec,feat)
                overhead=0.0
                if policy=='RULE':overhead=prep+feat_s
                elif policy!='NATIVE':
                    view=policy.split('_')[0];overhead=prep+rec[view]['decision_wall_s']+(feat_s if view=='STATS' else 0.0)
                est[policy]={'selected':selected,'estimated_total_s':med[selected]+overhead,'estimated_ratio_to_native':(med[selected]+overhead)/med['NATIVE'],'overhead_s':overhead}
            summary={'case':c['name'],'candidate_median_s':med,'aliases':aliases,'preparation_s':prep,'feature_compute_s':feat_s,
                     'model_decisions':rec,'offline_policy_estimates':est,'all_21_runs_correct':True}
            summaries.append(summary);print(json.dumps(summary),flush=True)
        (out/'summary.json').write_text(json.dumps(summaries,indent=2))
        if a.phase=='validation':
            scores={k:sum(s['offline_policy_estimates'][k]['estimated_total_s'] for s in summaries) for k in POLICIES}
            winner=min(POLICIES,key=lambda k:scores[k])
            freeze={'policy':winner,'selection_criterion':'minimum sum of validation OFFLINE estimated times including overhead; ties prefer native',
                    'validation_scores_s':scores,'frozen_unix':time.time(),'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    'model':MODEL,'holdout_not_yet_generated':True}
            with (ROOT/'frozen_policy.json').open('x') as f:f.write(json.dumps(freeze,indent=2))
            print(json.dumps({'frozen_policy':freeze}),flush=True)
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','cases':len(cases),'api_attempts_cumulative':Budget().count(),'all_correct':True}))
    finally:spark.stop()

if __name__=='__main__':main()
