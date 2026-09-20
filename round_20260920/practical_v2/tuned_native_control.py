"""Bounded native-only configuration control. No Jev calls or extra size oracle at planning.
A fixed 64 MiB broadcast threshold is compared to Spark's default 10 MiB on
new synthetic snapshots in the already tested numeric-schema dimension range.
This is not a recommendation to use 64 MiB on arbitrary production workloads.
"""
from pathlib import Path
import hashlib,json,random,statistics,time
import scale_benchmark as b
HERE=Path(__file__).resolve().parent
CASES=[
 dict(name='tuned_left_selective',n=9000000,m=2000000,keep=100,skew=False,seed=3197,join='LEFT'),
 dict(name='tuned_inner_half',n=9000000,m=2000000,keep=500,skew=False,seed=3313,join='INNER'),
 dict(name='tuned_left_skew',n=9000000,m=2000000,keep=500,skew=True,seed=3421,join='LEFT')]
ARMS={'DEFAULT_10MIB':10485760,'TUNED_64MIB':67108864}

def main():
    before=b.Budget().count();out=HERE/'tuned_native_v1';out.mkdir(exist_ok=False)
    manifest={'frozen_unix':time.time(),'cases':CASES,'arms':ARMS,'warmup_rounds':2,'measured_rounds':3,'order_seed':6389,
      'hypothesis':'The observed benefits may be obtained by a single static native Spark configuration rather than per-query prediction or synthetic row-count features.',
      'basis':'The larger development dimensions have two numeric columns and at most two million unfiltered rows. This control tests a fixed threshold, not an optimized threshold sweep.',
      'native_query':'No join hint; no runtime intervention; no extra generator row-count facts supplied to Spark.',
      'timing':'SQL construction, initial-plan capture and collect for both arms. Config switching and final plan logging excluded. Do not compare absolute medians across unrelated batches.',
      'environment':'local[2], requested 2 GiB driver, 32 shuffle partitions, AQE enabled. Only broadcast threshold differs between arms.',
      'limitations':'Three measured repetitions per arm per case; independent generator snapshots of the same families; no cluster broadcast distribution costs or production memory pressure tested.',
      'sources_sha256':{str(f.relative_to(b.WS)):hashlib.sha256(f.read_bytes()).hexdigest() for f in (Path(__file__),HERE/'scale_benchmark.py',b.ROUND/'baseline.py')}}
    (out/'frozen_manifest.json').write_text(json.dumps(manifest,indent=2))
    spark=b.session('JevTunedNativeControl');spark.sparkContext.setLogLevel('ERROR');rng=random.Random(6389)
    rows=[];summaries=[];started=time.perf_counter()
    try:
        assert spark.version=='4.0.1';assert spark.conf.get('spark.sql.adaptive.enabled')=='true'
        assert spark.conf.get('spark.sql.autoBroadcastJoinThreshold') in ('10485760','10485760b')
        (out/'resolved_config.json').write_text(json.dumps({'jvm_max_heap_bytes':spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions')},indent=2))
        for c in CASES:
            want=b.oracle(c);b.setup(spark,c);(out/(c['name']+'_oracle.json')).write_text(json.dumps(want))
            for phase,n in [('warmup',2),('measured',3)]:
                for rep in range(n):
                    arms=list(ARMS);rng.shuffle(arms)
                    for pos,arm in enumerate(arms):
                        if time.perf_counter()-started>400:raise RuntimeError('bounded_batch_time')
                        spark.conf.set('spark.sql.autoBroadcastJoinThreshold',str(ARMS[arm]))
                        r=b.run(spark,c,'NATIVE',want,c['name']+'_'+phase+'_'+str(rep)+'_'+arm)
                        r.update(phase=phase,rep=rep,position=pos,arm=arm,broadcast_threshold_bytes=ARMS[arm])
                        b.append(out/'raw.jsonl',r);rows.append(r)
                        if r['status']!='succeeded':raise RuntimeError('query_failure_preserved')
            group=[r for r in rows if r['case']==c['name'] and r['phase']=='measured']
            native={r['rep']:r['to_result_s'] for r in group if r['arm']=='DEFAULT_10MIB'}
            s={'case':c['name'],'arms':{}}
            for arm in ARMS:
                rs=[r for r in group if r['arm']==arm]
                s['arms'][arm]={'n':len(rs),'median_s':statistics.median(r['to_result_s'] for r in rs),
                  'faster_than_default_pairs':sum(r['to_result_s']<native[r['rep']] for r in rs),
                  'worst_ratio_to_default':max(r['to_result_s']/native[r['rep']] for r in rs),
                  'initial_joins':sorted({op for r in rs for op in b.OPS if op in r['initial_plan']}),
                  'final_joins':sorted({op for r in rs for op in b.OPS if op in r['final_plan'].split('== Initial Plan ==')[0]})}
            summaries.append(s);print(json.dumps(s),flush=True)
        assert b.Budget().count()==before
        report={'status':'succeeded','cases':summaries,'runs':len(rows),'measured_runs':18,'all_correct':True,
          'api_attempts_before':before,'api_attempts_after':b.Budget().count(),'new_jev_attempts':0,
          'scope':'Bounded static tuning control, not production validation or universally safe threshold'}
        (out/'summary.json').write_text(json.dumps(report,indent=2))
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','runs':len(rows),'measured':18,'all_correct':True,'new_api_attempts':0}))
        print(json.dumps(report,indent=2),flush=True)
    finally:
        spark.conf.set('spark.sql.autoBroadcastJoinThreshold','10485760');spark.stop()

if __name__=='__main__':main()
