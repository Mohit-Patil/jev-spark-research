"""Compile project-local observer with already installed Scala/JDK jars; run bounded synthetic test."""
from pathlib import Path
import hashlib,json,subprocess,time
import pyspark
from pyspark.sql import SparkSession
from baseline import expected,run_one,setup
ROOT=Path(__file__).resolve().parent
OUT=ROOT/'aqe_observer_v1';OUT.mkdir(exist_ok=False)
classes=OUT/'classes';classes.mkdir();jars=Path(pyspark.__file__).parent/'jars'
source=ROOT/'NativeBoundaryObserver.scala';jar=OUT/'native-boundary-observer.jar'
commands=[['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source)],
          ['jar','--create','--file',str(jar),'-C',str(classes),'.']]
for i,cmd in enumerate(commands):
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=90)
    (OUT/('compile_'+str(i)+'.json')).write_text(json.dumps({'command':cmd,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr},indent=2))
    print(json.dumps({'compile_step':i,'returncode':p.returncode,'stderr':p.stderr[:10000]}),flush=True)
    if p.returncode:raise SystemExit(p.returncode)
trace=OUT/'boundary_pairs.jsonl'
spark=(SparkSession.builder.master('local[2]').appName('JevAQENativeObserver')
    .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
    .config('spark.driver.memory','1g').config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','8')
    .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
    .config('spark.research.observer.path',str(trace)).config('spark.sql.warehouse.dir',str(ROOT/'warehouse')).getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
c=dict(name='aqe_runtime_selective',n=1200000,m=2000000,keep=10,skew=False,seed=19)
try:
    setup(spark,c);want=expected(c)
    records=[]
    for mode in ['control','observation_only']:
        if mode=='observation_only':spark.conf.set('spark.sql.adaptive.customCostEvaluatorClass','org.apache.spark.sql.execution.adaptive.NativeBoundaryObserver')
        r=run_one(spark,c,'NATIVE',want);r['mode']=mode;records.append(r)
        (OUT/(mode+'_execution.json')).write_text(json.dumps(r,indent=2))
        if r['status']!='succeeded':raise RuntimeError('observer_query_failed')
    entries=[json.loads(x) for x in trace.read_text().splitlines()] if trace.exists() else []
    pairs=[x for x in entries if x['kind']=='native_aqe_cost_boundary_pair']
    summary={'status':'succeeded' if pairs else 'no_pairs_captured','spark_version':spark.version,'case':c,
             'release_commit':'29434ea766b0fc3c3bf6eaadb43a8f931133649e','observer_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
             'pairs_captured':len(pairs),'unmatched_call_sites':len(entries)-len(pairs),'all_results_correct':True,
             'interventions':0,'jev_calls':0,'timing_caveat':'one cold control then one warmed observed query; not an observer-overhead benchmark',
             'native_broadcast_threshold':spark.conf.get('spark.sql.autoBroadcastJoinThreshold'),
             'executions':[{'mode':r['mode'],'to_result_s':r['to_result_s'],'initial_join_operators':[op for op in ['SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin'] if op in r['initial_plan']],
                 'final_join_operators':[op for op in ['SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]]} for r in records],
             'pair_summaries':[{'native_would_choose':p['native_would_choose'],'current_stock_cost':p['current']['stock_cost'],'proposed_stock_cost':p['proposed']['stock_cost'],
                   'current_stages':p['current']['materialized_stages'],'proposed_stages':p['proposed']['materialized_stages']} for p in pairs]}
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2),flush=True)
    if not pairs:
        print(json.dumps({'unmatched':entries[:4]}));raise SystemExit(2)
    (OUT/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','pairs':len(pairs),'correct':True,'interventions':0}))
finally:spark.stop()
