"""Development-only headroom screen. Defaults retain native AQE/broadcast thresholds.
No Jev calls. Original frozen pilot and holdout are not changed or reused.
"""
from pathlib import Path
import hashlib,json,random,statistics,sys,time
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from baseline import append,candidates,expected,run_one,session,setup
from safe_jev import Budget
CASES=[dict(name='selective_1pct',n=1200000,m=2000000,keep=10,skew=False,seed=313),
       dict(name='selective_10pct',n=1200000,m=2000000,keep=100,skew=False,seed=313),
       dict(name='selective_50pct',n=1200000,m=2000000,keep=500,skew=False,seed=313),
       dict(name='skew80_50pct',n=1200000,m=2000000,keep=500,skew=True,seed=313)]

def main():
    out=ROOT/'headroom_v2';out.mkdir(exist_ok=False)
    count=Budget().count();rng=random.Random(1843)
    manifest={'phase':'development screening, no heldout evaluation','cases':CASES,'seed':1843,'warmup_rounds':2,'measured_rounds':5,
      'cache':'same warmed JVM; uncached generated Range inputs; no persist; OS/JIT caches uncontrolled',
      'timing':'SQL creation + plan capture + collect; final-plan capture excluded; no advisor or candidate-enumeration overhead included',
      'sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),ROOT.parent/'baseline.py']},
      'comparison':'whole-query legal join hints, not AQE interventions','budget_before':count}
    spark=session('JevDevelopmentHeadroomV2');spark.sparkContext.setLogLevel('ERROR')
    summary=[]
    try:
        manifest['spark_version']=spark.version
        manifest['config']={k:spark.conf.get(k) for k in ['spark.sql.adaptive.enabled','spark.sql.shuffle.partitions','spark.sql.autoBroadcastJoinThreshold','spark.sql.adaptive.skewJoin.enabled','spark.sql.adaptive.forceOptimizeSkewedJoin']}
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
        for c in CASES:
            setup(spark,c);want=expected(c);plans,aliases=candidates(spark);rows=[]
            (out/(c['name']+'_candidates.json')).write_text(json.dumps({'candidates':plans,'aliases':aliases,'expected_result':want},indent=2))
            for phase,n in [('warmup',2),('measured',5)]:
                for rep in range(n):
                    order=list(plans);rng.shuffle(order)
                    for pos,k in enumerate(order):
                        rec=run_one(spark,c,k,want);rec.update(phase=phase,rep=rep,position=pos)
                        append(out/'raw.jsonl',rec);rows.append(rec)
                        if rec['status']!='succeeded':raise RuntimeError('failed_run_preserved_stop')
            med={k:statistics.median(r['to_result_s'] for r in rows if r['phase']=='measured' and r['strategy']==k) for k in plans}
            native={r['rep']:r['to_result_s'] for r in rows if r['phase']=='measured' and r['strategy']=='NATIVE'}
            s={'case':c['name'],'candidate_median_s':med,'aliases':aliases,'winner_by_median':min(med,key=med.get),'native_over_best_median_ratio':med['NATIVE']/min(med.values()),'comparisons':{}}
            for k in plans:
                rs=[r for r in rows if r['phase']=='measured' and r['strategy']==k]
                deltas=[native[r['rep']]-r['to_result_s'] for r in rs]
                s['comparisons'][k]={'wins_vs_native_same_round':sum(d>0 for d in deltas),'paired_median_saving_s':statistics.median(deltas),
                  'min_s':min(r['to_result_s'] for r in rs),'max_s':max(r['to_result_s'] for r in rs),
                  'final_join_operators':sorted({op for r in rs for op in ['BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]})}
            summary.append(s);(out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(s),flush=True)
        assert Budget().count()==count
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','cases':len(CASES),'all_correct':True,'new_api_attempts':0,'budget_used':count}))
    finally:spark.stop()
if __name__=='__main__':main()
