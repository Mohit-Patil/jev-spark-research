"""Randomized native-vs-observation check. Uses stock returned costs; zero Jev calls."""
from pathlib import Path
import hashlib,json,random,statistics,sys,time
from pyspark.sql import SparkSession
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from baseline import append,expected,run_one,setup
from safe_jev import Budget
CLASS='org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV2'

def make_session(trace,endpoint=None):
    build=ROOT/'observer_build_v2';jar=build/'observer.jar';metadata=json.loads((build/'COMPLETED.json').read_text())
    assert hashlib.sha256(jar.read_bytes()).hexdigest()==metadata['jar_sha256']
    b=(SparkSession.builder.master('local[2]').appName('JevRuntimeBoundaryResearch')
       .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
       .config('spark.driver.memory','1g').config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','8')
       .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
       .config('spark.research.observer.path',str(trace)).config('spark.sql.warehouse.dir',str(ROOT/'warehouse')))
    if endpoint:b=b.config('spark.research.shadow.endpoint',endpoint)
    spark=b.getOrCreate();spark.sparkContext.setLogLevel('ERROR');assert spark.version=='4.0.1'
    return spark

def metrics(spark):return json.loads(spark._jvm.org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV2.metrics())

def main():
    out=ROOT/'observer_check_v2';out.mkdir(exist_ok=False);trace=out/'boundary_pairs.jsonl';before=Budget().count()
    cases=[dict(name='observer_selective',n=1200000,m=2000000,keep=100,skew=False,seed=313),
           dict(name='observer_skew',n=1200000,m=2000000,keep=500,skew=True,seed=313)]
    manifest={'cases':cases,'warmups':2,'repetitions':7,'randomization_seed':1845,'cache':'uncached Range input, warmed shared JVM',
      'phase':'observer correctness/overhead diagnostics, not policy-performance evaluation','api_calls':0,'interventions':0,
      'observer_build':json.loads((ROOT/'observer_build_v2'/'COMPLETED.json').read_text())}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2));rng=random.Random(1845)
    spark=make_session(trace);rows=[]
    try:
        for c in cases:
            setup(spark,c);want=expected(c)
            for phase,n in [('warmup',2),('measured',7)]:
                for rep in range(n):
                    modes=['control','observer'];rng.shuffle(modes)
                    for position,mode in enumerate(modes):
                        tag=c['name']+'_'+phase+'_'+str(rep)+'_'+mode
                        spark.conf.set('spark.research.run_id',tag)
                        if mode=='observer':spark.conf.set('spark.sql.adaptive.customCostEvaluatorClass',CLASS)
                        else:spark.conf.unset('spark.sql.adaptive.customCostEvaluatorClass')
                        a=metrics(spark);r=run_one(spark,c,'NATIVE',want);b=metrics(spark)
                        delta={k:b[k]-a[k] for k in b};r.update(run_id=tag,mode=mode,phase=phase,rep=rep,position=position,
                          observer_metrics=delta,finished_epoch_ms=int(time.time()*1000))
                        rows.append(r);append(out/'raw.jsonl',r)
                        if r['status']!='succeeded':raise RuntimeError('query_failure_preserved')
                        assert delta['errors']==delta['unmatched']==delta['dropped']==0
                        assert delta['calls']==2*delta['pairs']
                        assert (delta['pairs']>0)==(mode=='observer')
        pairs=[json.loads(x) for x in trace.read_text().splitlines()];bytag={r['run_id']:r for r in rows}
        for p in pairs:
            r=bytag[p['run_id']];assert r['mode']=='observer'
            assert p['current']['source_line']==365 and p['proposed']['source_line']==366
            assert p['cost_returned_unchanged'] and not p['intervention']
            old=p['current']['stock_cost_value'];new=p['proposed']['stock_cost_value']
            assert p['native_would_choose']==('PROPOSED' if new<old or (new==old and not p['plans_equal']) else 'CURRENT')
            assert p['proposed']['captured_epoch_ms']<=r['finished_epoch_ms']
        summaries=[]
        for c in cases:
            s={'case':c['name'],'modes':{}}
            for mode in ['control','observer']:
                rs=[r for r in rows if r['case']==c['name'] and r['phase']=='measured' and r['mode']==mode]
                s['modes'][mode]={'median_to_result_s':statistics.median(r['to_result_s'] for r in rs),
                  'median_instrumentation_self_s':statistics.median(r['observer_metrics']['observer_nanos']/1e9 for r in rs),
                  'all_correct':all(r['correct'] for r in rs),'final_joins':sorted({op for r in rs for op in ['BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]})}
            control={r['rep']:r['to_result_s'] for r in rows if r['case']==c['name'] and r['phase']=='measured' and r['mode']=='control'}
            ds=[r['to_result_s']-control[r['rep']] for r in rows if r['case']==c['name'] and r['phase']=='measured' and r['mode']=='observer']
            s['median_paired_observer_minus_control_s']=statistics.median(ds);summaries.append(s)
        result={'status':'succeeded','runs':len(rows),'measured_runs':28,'boundary_pairs':len(pairs),
          'metrics':metrics(spark),'summaries':summaries,'all_native_predicates_verified':True,
          'caveat':'observer self-time measured directly; paired end-to-end differences also include normal JVM/GC/scheduling variability',
          'new_jev_calls':0,'budget_used':Budget().count()}
        assert Budget().count()==before
        (out/'summary.json').write_text(json.dumps(result,indent=2));(out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','all_correct':True,'pairs':len(pairs)}))
        print(json.dumps(result,indent=2),flush=True)
    finally:spark.stop()
if __name__=='__main__':main()
