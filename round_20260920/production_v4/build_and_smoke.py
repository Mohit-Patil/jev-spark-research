"""Compile only project-local source, then run a conservative zero-Jev integration check."""
from pathlib import Path
import hashlib,json,subprocess,sys,time
import pyspark
from pyspark.sql import SparkSession
ROOT=Path(__file__).resolve().parent
ROUND=ROOT.parent
sys.path[:0]=[str(ROUND/'practical_v2'),str(ROUND)]
from scale_benchmark import oracle,query,run,setup
from safe_jev import Budget

def main():
    out=ROOT/'build_smoke_v1';out.mkdir(exist_ok=False)
    before=Budget().count();classes=out/'classes';classes.mkdir()
    jars=Path(pyspark.__file__).parent/'jars';source=ROOT/'RuntimeCandidateExtension.scala';jar=out/'candidate.jar'
    for i,args in enumerate([
        ['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source)],
        ['jar','--create','--file',str(jar),'-C',str(classes),'.']]):
        p=subprocess.run(args,capture_output=True,text=True,timeout=90)
        rec={'command':args,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
        (out/f'compile_{i}.json').write_text(json.dumps(rec,indent=2));print(json.dumps(rec),flush=True)
        if p.returncode: raise SystemExit(1)
    c=dict(name='runtime_candidate_smoke',n=1200000,m=2000000,keep=500,skew=True,seed=313,join='LEFT')
    want=oracle(c)
    spark=(SparkSession.builder.master('local[2]').appName('RuntimeCandidateV4Smoke')
      .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1')
      .config('spark.driver.host','127.0.0.1').config('spark.driver.memory','2g')
      .config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','32')
      .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
      .config('spark.sql.extensions','org.apache.spark.sql.execution.adaptive.RuntimeCandidateExtension')
      .config('spark.local.dir',str(ROUND.parent/'spark-temp'))
      .config('spark.sql.warehouse.dir',str(ROOT/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    state=spark._jvm.org.apache.spark.sql.execution.adaptive.RuntimeCandidateEvents
    rows=[]
    try:
        setup(spark,c)
        for mode in ['OFF','SHADOW','APPLY']:
            spark.conf.set('spark.research.v4.mode',mode)
            spark.conf.set('spark.research.v4.run_id','smoke_'+mode)
            r=run(spark,c,'NATIVE',want,'smoke_'+mode);r['mode']=mode
            rows.append(r);(out/(mode+'_execution.json')).write_text(json.dumps(r,indent=2))
            if r['status']!='succeeded':raise RuntimeError('query_failed')
        records=json.loads(state.state());(out/'events.json').write_text(json.dumps(records,indent=2))
        summary={'status':'succeeded','correct':True,'runs':len(rows),'new_jev_calls':0,
          'budget_before':before,'budget_after':Budget().count(),'case':c,'errors':records['errors'],
          'events':len(records['events']),'applied_events':sum(e['rule_selected'] for e in records['events']),
          'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
          'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest(),
          'jvm_max_memory_bytes':spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
          'runs_summary':[{'mode':r['mode'],'seconds':r['to_result_s'],
            'initial_broadcast':'BroadcastHashJoin' in r['initial_plan'],
            'final_broadcast':'BroadcastHashJoin' in r['final_plan'].split('== Initial Plan ==')[0]} for r in rows],
          'caveat':'functional smoke, different warm states; timings are NOT an A/B benchmark'}
        if records['errors'] or not summary['applied_events']:summary['status']='no_valid_application'
        (out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2),flush=True)
        if summary['status']!='succeeded':raise SystemExit(2)
        (out/'COMPLETED.json').write_text(json.dumps(summary,indent=2))
    finally:spark.stop()
if __name__=='__main__':main()
