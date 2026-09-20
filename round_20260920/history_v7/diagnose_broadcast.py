"""Bounded read-only reproduction of the cache trial's Spark failure.
No Jev/key reads or new data; immutable saved synthetic Parquet only.
Preserve failure class/cause instead of guessing that it was memory pressure.
"""
import hashlib,json,threading,time
from history_core import *
from cache_trial import CASES_CACHE,identity

def java_cause(e):
    out=[];j=getattr(e,'java_exception',None)
    for _ in range(6):
        if j is None:break
        try:
            cls=str(j.getClass().getName());message=str(j.getMessage())
            out.append({'class':cls,'message':message[:1500]})
            j=j.getCause()
        except Exception:break
    return out

def main():
    out=R/'broadcast_diagnostic_v1';out.mkdir(exist_ok=False);before=AuthorizedBudget().count();c=CASES_CACHE['repeat_wide']
    source=R/'cache_trial_v1'/'repeat_wide';saved=json.loads((source/'IMMUTABLE_DATASET.json').read_text())
    snap,_=identity(source/'data');assert snap==saved['identity']
    want=oracle(c);spark=corpus.session();spark.sparkContext.setLogLevel('ERROR');rr=[];start=time.perf_counter()
    dump(out/'FROZEN.json',{'created_unix':time.time(),'case':c,'maximum_rounds':12,'maximum_queries':24,
      'reason':'Diagnose one previously saved Py4JJavaError; no performance-selection tuning.',
      'source_sha256':sha(R/'diagnose_broadcast.py'),'new_api_calls':0})
    try:
        spark.read.parquet(str(source/'data'/'fact')).createOrReplaceTempView('f')
        spark.read.parquet(str(source/'data'/'dim')).where(f'(k*17+{c["seed"]}) % 1000 < {c["keep"]}').createOrReplaceTempView('d')
        runtime=spark._jvm.java.lang.Runtime.getRuntime()
        for rep in range(12):
            for arm in ('NATIVE','BROADCAST'):
                if time.perf_counter()-start>150:raise TimeoutError('diagnostic_launch_bound')
                corpus.configure(spark,arm);tag='diag7_'+str(rep)+'_'+arm
                spark.sparkContext.setJobGroup(tag,'bounded failure reproduction',interruptOnCancel=True)
                timer=threading.Timer(30,spark.sparkContext.cancelJobGroup,args=(tag,));timer.start()
                r={'rep':rep,'arm':arm,'heap_used_before':int(runtime.totalMemory()-runtime.freeMemory()),'heap_max':int(runtime.maxMemory())}
                t=time.perf_counter()
                try:
                    df=spark.sql(query(c,arm));got=sorted([list(x) for x in df.collect()])
                    r.update(status='succeeded' if got==want else 'failed',correct=got==want,result=got,to_result_s=time.perf_counter()-t,
                      final_plan=df._jdf.queryExecution().executedPlan().toString())
                except Exception as e:
                    r.update(status='failed',error_type=type(e).__name__,java_causes=java_cause(e),elapsed_s=time.perf_counter()-t)
                finally:
                    timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
                r['heap_used_after']=int(runtime.totalMemory()-runtime.freeMemory());rr.append(r);append(out/'raw.jsonl',r)
                print(json.dumps({k:r[k] for k in ('rep','arm','status','heap_used_before','heap_used_after')}),flush=True)
                if r['status']!='succeeded':break
            if rr[-1]['status']!='succeeded':break
        report={'status':'diagnostic_completed','queries_attempted':len(rr),'correct_queries':sum(r.get('correct',False) for r in rr),
          'failed_queries':[r for r in rr if r['status']!='succeeded'],'new_api_calls':0,
          'budget_before':before,'budget_after':AuthorizedBudget().count(),
          'original_cache_failure':rows(source/'raw.jsonl')[-1]['error_type'],
          'causal_limit':'A fresh process may not reproduce the prior failure. Do not infer its cause merely from a generic Py4JJavaError.'}
        dump(out/'COMPLETED.json',report);print(json.dumps(report,indent=2),flush=True)
    finally:spark.stop()
if __name__=='__main__':main()
