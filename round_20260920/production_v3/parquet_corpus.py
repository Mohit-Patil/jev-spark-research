"""Bounded file-backed, variable-width development corpus. ZERO external requests.
All policies keep AQE. No initial-plan deduplication. No runtime intervention.
Payload-dependent aggregation keeps the wide string needed across the join.
"""
from pathlib import Path
import argparse, hashlib, json, random, statistics, sys, threading, time, shutil
from pyspark.sql import SparkSession
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
WS=ROUND.parent
sys.path.insert(0,str(ROUND))
from baseline import plan_info, append
from safe_jev import Budget
ARMS=('NATIVE','TUNED64','BROADCAST','MERGE','SHUFFLE_HASH')
CASES={
 'smoke':dict(n=10000,m=2000,width=512,keep=500,seed=347),
 'narrow':dict(n=2000000,m=100000,width=64,keep=1000,seed=347),
 'wide_small_probe':dict(n=100000,m=400000,width=512,keep=1000,seed=347),
 'wide_large_probe':dict(n=2000000,m=400000,width=512,keep=1000,seed=347),
 'wide_selective':dict(n=2000000,m=400000,width=512,keep=10,seed=347)}
OPS=('BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin')

def oracle(c):
    out={};cache={}
    for i in range(c['n']):
        k=None if i%31==0 else (i*97+c['seed'])%c['m']
        v=(i*13+c['seed'])%997;b=i%7
        match=k is not None and (k*17+c['seed'])%1000<c['keep']
        score=0
        if match:
            pos=v%c['width'];segment,offset=divmod(pos,64);key=(k,segment)
            if key not in cache:
                cache[key]=hashlib.sha256(f'{k}:{segment}:{c["seed"]}'.encode()).hexdigest()
            score=ord(cache[key][offset])
        a=out.setdefault(b,[0,0,0,0,0]);a[0]+=1;a[1]+=k or 0;a[2]+=v;a[3]+=score;a[4]+=int(match)
    return [[k,*out[k]] for k in sorted(out)]

def query(arm):
    hint={'NATIVE':'','TUNED64':'','BROADCAST':'BROADCAST(d)','MERGE':'MERGE(f,d)','SHUFFLE_HASH':'SHUFFLE_HASH(d)'}[arm]
    h='/*+ '+hint+' */ ' if hint else ''
    return ('SELECT '+h+'f.bucket, count(*) AS cnt, sum(f.k) AS keys, sum(f.v) AS vals, '+
     'sum(coalesce(ascii(substring(d.payload, CAST(pmod(f.v,length(d.payload))+1 AS INT),1)),0)) AS checksum, '+
     'count(d.k) AS matches FROM f LEFT JOIN d ON f.k=d.k GROUP BY f.bucket')

def session():
    return (SparkSession.builder.master('local[2]').appName('JevParquetWidthResearch')
     .config('spark.ui.enabled','false').config('spark.driver.memory','2g')
     .config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
     .config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','32')
     .config('spark.local.dir',str(WS/'spark-temp'))
     .config('spark.sql.warehouse.dir',str(HERE/'warehouse')).getOrCreate())

def configure(spark,arm):
    bound=67108864 if arm=='TUNED64' else 10485760
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold',str(bound))
    spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold',str(bound))

def write_inputs(spark,c,out):
    p=out/'data';p.mkdir()
    seed,m=c['seed'],c['m']
    fact=spark.range(c['n']).selectExpr(
      f'CASE WHEN id % 31 = 0 THEN CAST(NULL AS BIGINT) ELSE (id*97+{seed}) % {m} END AS k',
      f'(id*13+{seed}) % 997 AS v','id % 7 AS bucket')
    chunks=[f"sha2(concat(CAST(id AS STRING), ':{s}:{seed}'),256)" for s in range(c['width']//64)]
    dim=spark.range(c['m']).selectExpr('id AS k','concat('+','.join(chunks)+') AS payload')
    fact.write.mode('error').parquet(str(p/'fact'))
    dim.write.mode('error').parquet(str(p/'dim'))
    spark.read.parquet(str(p/'fact')).createOrReplaceTempView('f')
    spark.read.parquet(str(p/'dim')).where(f'(k*17+{seed}) % 1000 < {c["keep"]}').createOrReplaceTempView('d')
    files=[x for x in p.rglob('*.parquet')]
    total=sum(x.stat().st_size for x in files)
    if total>1073741824:raise RuntimeError('dataset_exceeded_1GiB_research_bound')
    return {'parquet_bytes':total,'files':[{ 'path':str(x.relative_to(out)),'bytes':x.stat().st_size} for x in files],
      'fact_estimate':spark.table('f')._jdf.queryExecution().optimizedPlan().stats().toString(),
      'dimension_estimate':spark.table('d')._jdf.queryExecution().optimizedPlan().stats().toString()}

def metrics(spark,plan):
    rows=[];seen=set()
    def walk(p):
        ident=spark._jvm.java.lang.System.identityHashCode(p)
        if ident in seen:return
        seen.add(ident);ms={}
        it=p.metrics().iterator()
        while it.hasNext():
            e=it.next();v=e._2();ms[str(e._1())]={'value':int(v.value()),'type':str(v.metricType())}
        if ms:rows.append({'node':p.nodeName(),'metrics':ms})
        name=p.nodeName()
        if name=='AdaptiveSparkPlan':walk(p.executedPlan())
        elif name.endswith('QueryStage'):walk(p.plan())
        else:
            cs=p.children().iterator()
            while cs.hasNext():walk(cs.next())
    walk(plan);return rows

def run(spark,arm,want,tag):
    configure(spark,arm);spark.sparkContext.setJobGroup(tag,'bounded file-backed benchmark',interruptOnCancel=True)
    timer=threading.Timer(75,spark.sparkContext.cancelJobGroup,args=(tag,));timer.start()
    t=time.perf_counter();r={'arm':arm,'run_id':tag,'started_unix':time.time()}
    try:
        df=spark.sql(query(arm));info=plan_info(df);t1=time.perf_counter()
        got=sorted([list(x) for x in df.collect()]);t2=time.perf_counter()
        p=df._jdf.queryExecution().executedPlan()
        r.update(info,result=got,correct=got==want,to_result_s=t2-t,planning_capture_s=t1-t,collect_s=t2-t1,
         status='succeeded' if got==want else 'failed',final_plan=p.toString(),sql_metrics=metrics(spark,p))
    except Exception as e:r.update(status='failed',error_type=type(e).__name__,elapsed_s=time.perf_counter()-t)
    finally:
        timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
    return r

def main():
    a=argparse.ArgumentParser();a.add_argument('--case',choices=CASES,required=True);a.add_argument('--label',required=True)
    a.add_argument('--reps',type=int,default=3);a.add_argument('--warmups',type=int,default=1);args=a.parse_args()
    if not args.label.replace('_','').isalnum() or not 1<=args.reps<=7 or not 0<=args.warmups<=2:raise ValueError('invalid_bounds')
    c=CASES[args.case];out=HERE/args.label;out.mkdir(exist_ok=False);start=time.perf_counter()
    if shutil.disk_usage(WS).free<8589934592:raise RuntimeError('insufficient_disk_headroom')
    before=Budget().count()
    manifest={'kind':'zero_API_file_backed_development','case_name':args.case,'case':c,
     'arms':ARMS,'warmups':args.warmups,'reps':args.reps,'random_seed':8127,
     'new_api_attempts':0,'actual_aqe_intervention':False,
     'cache':'No persisted DataFrames. Parquet reread each query; OS cache and JVM warmed, not reset.',
     'feature_source':'file-source Spark estimates; exact generator facts are correctness-only, not selection inputs',
     'timing':'SQL build, initial-plan capture and collect; excludes one-off data generation/oracle and final plan/metrics logging',
     'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
     'batch_launch_bound_seconds':380,'per_query_cancel_seconds':75}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
    want=oracle(c);(out/'oracle.json').write_text(json.dumps(want))
    spark=session();spark.sparkContext.setLogLevel('ERROR');rs=[]
    try:
        assert spark.version=='4.0.1' and spark.conf.get('spark.sql.adaptive.enabled')=='true'
        config={k:spark.conf.get(k) for k in ('spark.sql.shuffle.partitions','spark.sql.adaptive.enabled','spark.sql.adaptive.skewJoin.enabled')}
        config['jvm_max_heap_bytes']=spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
        t=time.perf_counter();data=write_inputs(spark,c,out);data['setup_s']=time.perf_counter()-t
        (out/'dataset.json').write_text(json.dumps(data,indent=2));(out/'config.json').write_text(json.dumps(config,indent=2))
        inv={}
        for arm in ARMS:
            configure(spark,arm);inv[arm]=plan_info(spark.sql(query(arm)))
        (out/'inventory.json').write_text(json.dumps(inv,indent=2))
        rng=random.Random(8127)
        for phase,count in [('warmup',args.warmups),('measured',args.reps)]:
            for rep in range(count):
                order=list(ARMS);rng.shuffle(order)
                for pos,arm in enumerate(order):
                    if time.perf_counter()-start>380:raise RuntimeError('stop_new_launches_at_batch_deadline')
                    r=run(spark,arm,want,args.label+'_'+phase+'_'+str(rep)+'_'+arm)
                    r.update(case=args.case,phase=phase,rep=rep,position=pos);append(out/'raw.jsonl',r);rs.append(r)
                    if r['status']!='succeeded':raise RuntimeError('failed_query_preserved')
                print(json.dumps({'case':args.case,'phase':phase,'rep':rep,'completed_arms':len(order)}),flush=True)
        measured=[r for r in rs if r['phase']=='measured'];native={r['rep']:r['to_result_s'] for r in measured if r['arm']=='NATIVE'}
        summary={'status':'succeeded','case':args.case,'queries':len(rs),'measured':len(measured),'all_correct':True,'arms':{},'data':data}
        for arm in ARMS:
            xs=[r for r in measured if r['arm']==arm];ts=[r['to_result_s'] for r in xs]
            summary['arms'][arm]={'median_s':statistics.median(ts),'min_s':min(ts),'max_s':max(ts),
             'faster_than_native_pairs':sum(r['to_result_s']<native[r['rep']] for r in xs),
             'median_paired_saving_s':statistics.median(native[r['rep']]-r['to_result_s'] for r in xs),
             'worst_ratio_to_native':max(r['to_result_s']/native[r['rep']] for r in xs),
             'final_joins':sorted({op for r in xs for op in OPS if op in r['final_plan'].split('== Initial Plan ==')[0]})}
        summary['api_before']=before;summary['api_after']=Budget().count();summary['new_api_attempts_by_this_script']=0
        (out/'summary.json').write_text(json.dumps(summary,indent=2))
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','queries':len(rs),'all_correct':True,'new_api_requests':0}))
        print(json.dumps(summary,indent=2),flush=True)
    finally:spark.stop()

if __name__=='__main__':main()
