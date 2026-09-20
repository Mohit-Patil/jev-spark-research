"""Corrected development screen: initial-plan collisions never remove join-hint variants.
No new dependencies, model calls, or Spark policy changes.
"""
from pathlib import Path
import hashlib,json,random,statistics,sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from baseline import STRATEGIES,append,expected,plan_info,run_one,session,setup,sql
from safe_jev import Budget
from headroom_v2 import CASES

def inventory(spark):
    plans={k:plan_info(spark.sql(sql(k))) for k in STRATEGIES}
    groups={}
    for k,p in plans.items():groups.setdefault(p['fingerprint'],[]).append(k)
    return plans,[x for x in groups.values() if len(x)>1]

def main():
    out=ROOT/'headroom_v3';out.mkdir(exist_ok=False);before=Budget().count();rng=random.Random(1844)
    manifest={'kind':'corrected_development_screen','cases':CASES,'seed':1844,'warmups':2,'repetitions':7,
      'candidate_policy':'retain NATIVE, BROADCAST, MERGE and SHUFFLE_HASH even when initial plans collide; hints affect future AQE replanning',
      'cache':'uncached Range inputs, shared warmed JVM, OS/JIT caches uncontrolled','aqe_interventions':0,'api_calls':0,
      'timing':'SQL construction plus plan capture plus collect; excludes candidate enumeration and final-plan logging',
      'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),ROOT/'headroom_v2.py',ROOT.parent/'baseline.py']}}
    spark=session('JevHeadroomV3');spark.sparkContext.setLogLevel('ERROR');summaries=[];total=0
    try:
        manifest['spark_version']=spark.version
        manifest['configuration']={k:spark.conf.get(k) for k in ['spark.sql.adaptive.enabled','spark.sql.autoBroadcastJoinThreshold','spark.sql.shuffle.partitions','spark.sql.adaptive.skewJoin.enabled']}
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
        for c in CASES:
            setup(spark,c);want=expected(c);plans,collisions=inventory(spark);rows=[]
            assert list(plans)==list(STRATEGIES) and len(plans)==4
            (out/(c['name']+'_candidates.json')).write_text(json.dumps({'candidates':plans,'initial_plan_collisions_not_removed':collisions,'expected_result':want},indent=2))
            for phase,reps in [('warmup',2),('measured',7)]:
                for rep in range(reps):
                    order=list(plans);rng.shuffle(order)
                    for pos,k in enumerate(order):
                        r=run_one(spark,c,k,want);r.update(phase=phase,rep=rep,position=pos)
                        append(out/'raw.jsonl',r);rows.append(r);total+=1
                        if r['status']!='succeeded':raise RuntimeError('failed_execution_preserved')
            native={r['rep']:r['to_result_s'] for r in rows if r['strategy']=='NATIVE' and r['phase']=='measured'}
            s={'case':c['name'],'initial_plan_collisions':collisions,'strategies':{}}
            for k in plans:
                rs=[r for r in rows if r['phase']=='measured' and r['strategy']==k]
                times=[r['to_result_s'] for r in rs];savings=[native[r['rep']]-r['to_result_s'] for r in rs]
                s['strategies'][k]={'n':len(rs),'median_s':statistics.median(times),'min_s':min(times),'max_s':max(times),
                 'wins_vs_native_same_round':sum(x>0 for x in savings),'median_paired_saving_s':statistics.median(savings),
                 'final_joins':sorted({op for r in rs for op in ['BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]})}
            s['winner']=min(s['strategies'],key=lambda k:s['strategies'][k]['median_s'])
            s['native_over_best_median']=s['strategies']['NATIVE']['median_s']/s['strategies'][s['winner']]['median_s']
            summaries.append(s);(out/'summary.json').write_text(json.dumps(summaries,indent=2));print(json.dumps(s),flush=True)
        assert Budget().count()==before
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','runs':total,'measured_runs':112,'all_correct':True,'new_jev_calls':0,'budget_used':before}))
    finally:spark.stop()
if __name__=='__main__':main()
