"""Randomized whole-query measurement of real AQE candidate generation, no Jev.
OFF: native rules, extension inert. SHADOW: generate/validate but do not apply.
APPLY: fixed bounded runtime rule. PRE_BROADCAST: diagnostic whole-query hint,
never a counterfactual for a mid-query switch. Every run includes completed work.
"""
from pathlib import Path
import argparse,hashlib,json,random,re,statistics,sys,time
from pyspark.sql import SparkSession
ROOT=Path(__file__).resolve().parent;ROUND=ROOT.parent
sys.path[:0]=[str(ROUND/'practical_v2'),str(ROUND)]
from scale_benchmark import oracle,run,setup
from baseline import append
from safe_jev import Budget
ARMS=['OFF','SHADOW','APPLY','TUNED_AQE']
CASES=[dict(name='heldout_skew18m',n=18000000,m=2000000,keep=500,skew=True,seed=81283,join='LEFT'),
       dict(name='heldout_uniform18m',n=18000000,m=2000000,keep=500,skew=False,seed=37121,join='LEFT')]

def signature(e):
    def part(s):return {k:s[k] for k in ['rows','runtime_size_bytes','is_runtime','serialized_partition_bytes','materialized']}
    plan=re.sub(r'#\d+L?','#_',e['original_join']);plan=re.sub(r'\[plan_id=\d+\]','[plan_id=_]',plan)
    plan=re.sub(r'\*\(\d+\)','*(_)',plan)
    return hashlib.sha256(json.dumps({'left':part(e['left']),'right':part(e['right']),'plan':plan},sort_keys=True).encode()).hexdigest()

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--label',default='runtime_holdout_v1');ap.add_argument('--reps',type=int,default=5)
    args=ap.parse_args();assert re.fullmatch('[a-zA-Z0-9_]+',args.label) and 3<=args.reps<=7
    out=ROOT/args.label;out.mkdir(exist_ok=False);started=time.perf_counter();before=Budget().count()
    build=ROOT/'build_smoke_v2';assert json.loads((build/'COMPLETED.json').read_text())['status']=='succeeded'
    jar=build/'candidate.jar'
    sourcefiles=[Path(__file__),ROOT/'RuntimeCandidateExtension.scala',ROUND/'practical_v2'/'scale_benchmark.py',ROUND/'baseline.py']
    hashes={str(p.relative_to(ROUND)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sourcefiles}
    manifest={'cases':CASES,'arms':ARMS,'warmups':2,'reps':args.reps,'seed':90267,'scope':'frozen independent snapshots; same query families, no model choice; aligned native AQE baseline added',
      'runtime_gate':'both shuffle inputs materialized; INNER/LEFT, one Long key; right rows<=1250000, runtime bytes<=32 MiB; no skew-split joins',
      'memory_admission_caveat':'these are conservative experimental bounds, not a general hash-table memory-safety proof',
      'hashes':hashes,'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'model_calls':0,
      'timing':'whole SQL construction, initial-plan capture, all stage execution including extra broadcast and synchronous candidate generation. Final-plan/event extraction excluded.',
      'cache':'uncached generated inputs, no materialized DataFrames shared between runs; warmed shared JVM and uncontrolled OS/JIT caches',
      'holdout':'independent 18-million-row snapshots; same LEFT family, not an unseen-template holdout; no retuning'}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
    spark=(SparkSession.builder.master('local[2]').appName('RuntimeCandidateV4Benchmark')
      .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1')
      .config('spark.driver.host','127.0.0.1').config('spark.driver.memory','2g')
      .config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','32')
      .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
      .config('spark.sql.extensions','org.apache.spark.sql.execution.adaptive.RuntimeCandidateExtension')
      .config('spark.local.dir',str(ROUND.parent/'spark-temp'))
      .config('spark.sql.warehouse.dir',str(ROOT/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');events=spark._jvm.org.apache.spark.sql.execution.adaptive.RuntimeCandidateEvents
    rows=[];summaries=[];rng=random.Random(90267)
    try:
        config={k:spark.conf.get(k) for k in ['spark.sql.adaptive.enabled','spark.sql.autoBroadcastJoinThreshold','spark.sql.shuffle.partitions','spark.sql.adaptive.skewJoin.enabled']}
        config['jvm_max_memory_bytes']=spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
        assert config['spark.sql.autoBroadcastJoinThreshold'] in ('10485760','10485760b')
        (out/'resolved_config.json').write_text(json.dumps(config,indent=2))
        for c in CASES:
            want=oracle(c);setup(spark,c);(out/(c['name']+'_oracle.json')).write_text(json.dumps(want))
            case_rows=[]
            for phase,reps in [('warmup',2),('measured',args.reps)]:
                for rep in range(reps):
                    arms=list(ARMS);rng.shuffle(arms)
                    for pos,arm in enumerate(arms):
                        if time.perf_counter()-started>400:raise RuntimeError('bounded_batch_stop')
                        tag=f'{args.label}_{c["name"]}_{phase}_{rep}_{arm}'
                        spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold',33554432) if arm=='TUNED_AQE' else spark.conf.unset('spark.sql.adaptive.autoBroadcastJoinThreshold')
                        spark.conf.set('spark.research.v4.mode','OFF' if arm=='TUNED_AQE' else arm)
                        spark.conf.set('spark.research.v4.run_id',tag)
                        st=json.loads(events.state())
                        r=run(spark,c,'BROADCAST' if arm=='PRE_BROADCAST' else 'NATIVE',want,tag)
                        en=json.loads(events.state());new=[e for e in en['events'] if e['run_id']==tag]
                        assert not en['errors'],en['errors']
                        r.update(arm=arm,phase=phase,rep=rep,position=pos,events=new,adaptive_broadcast_threshold_bytes=33554432 if arm=='TUNED_AQE' else 10485760,
                            candidate_self_s=(en['self_nanos']-st['self_nanos'])/1e9)
                        r['signatures']=[signature(e) for e in new]
                        append(out/'raw.jsonl',r);rows.append(r);case_rows.append(r)
                        if r['status']!='succeeded':raise RuntimeError('failed_result_preserved')
                    print(json.dumps({'case':c['name'],'phase':phase,'rep':rep,'completed':True}),flush=True)
            measured=[r for r in case_rows if r['phase']=='measured'];native={r['rep']:r for r in measured if r['arm']=='OFF'}
            summary={'case':c['name'],'arms':{}}
            for arm in ARMS:
                rr=[r for r in measured if r['arm']==arm];times=[r['to_result_s'] for r in rr]
                savings=[native[r['rep']]['to_result_s']-r['to_result_s'] for r in rr]
                summary['arms'][arm]={'n':len(rr),'median_s':statistics.median(times),'min_s':min(times),'max_s':max(times),
                    'paired_median_saving_vs_native_s':statistics.median(savings),'faster_pairs':sum(x>0 for x in savings),
                    'worst_ratio_vs_native':max(r['to_result_s']/native[r['rep']]['to_result_s'] for r in rr),
                    'queries_with_runtime_candidates':sum(bool(r['events']) for r in rr),
                    'median_candidate_self_s':statistics.median(r['candidate_self_s'] for r in rr),
                    'final_joins':sorted({op for r in rr for op in ['BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]})}
            paired=[]
            for rep in range(args.reps):
                a=next(r for r in measured if r['rep']==rep and r['arm']=='APPLY')
                s=next(r for r in measured if r['rep']==rep and r['arm']=='SHADOW')
                if a['signatures'] and s['signatures'] and a['signatures'][0]==s['signatures'][0]:
                    paired.append(s['to_result_s']-a['to_result_s'])
            summary['observed_state_matched_rounds']=len(paired)
            summary['matched_whole_query_median_saving_vs_shadow_s']=statistics.median(paired) if paired else None
            summary['matching_caveat']='Both join inputs materialized, exact recorded plan/statistics matched; cache/GC/OS state not matched. Full-query paired comparisons are primary.'
            summaries.append(summary);(out/'summary.json').write_text(json.dumps(summaries,indent=2));print(json.dumps(summary,indent=2),flush=True)
        assert all(hashlib.sha256(p.read_bytes()).hexdigest()==hashes[str(p.relative_to(ROUND))] for p in sourcefiles)
        completion={'status':'succeeded','runs':len(rows),'measured_runs':len(CASES)*len(ARMS)*args.reps,
            'all_correct':True,'new_jev_calls':0,'budget_before':before,'budget_after':Budget().count(),
            'applied_runtime_candidates':sum(e['rule_selected'] for r in rows for e in r['events']),
            'elapsed_s':time.perf_counter()-started}
        (out/'COMPLETED.json').write_text(json.dumps(completion,indent=2));print(json.dumps(completion),flush=True)
    finally:spark.stop()
if __name__=='__main__':main()
