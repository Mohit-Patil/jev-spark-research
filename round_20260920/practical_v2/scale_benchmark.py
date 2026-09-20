"""Larger bounded, four-arm development experiments. No model calls.
All hint policies are retained, including initial-plan collisions. The LEFT family
preserves the fact input instead of allowing inner-join predicate propagation to
filter away most of the work. No native optimizer rule is disabled.
"""
from pathlib import Path
import argparse, hashlib, json, random, statistics, sys, threading, time
from pyspark.sql import SparkSession
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
WS=ROUND.parent
sys.path.insert(0,str(ROUND))
from baseline import STRATEGIES, append, plan_info, setup
from safe_jev import Budget

CASES={
 'recheck_inner_10': dict(name='recheck_inner_10',n=1200000,m=2000000,keep=100,skew=False,seed=313,join='INNER'),
 'left_uniform_10': dict(name='left_uniform_10',n=12000000,m=2000000,keep=100,skew=False,seed=313,join='LEFT'),
 'inner_uniform_50': dict(name='inner_uniform_50',n=12000000,m=2000000,keep=500,skew=False,seed=313,join='INNER'),
 'left_skew80_50': dict(name='left_skew80_50',n=12000000,m=2000000,keep=500,skew=True,seed=313,join='LEFT'),
}
OPS=('BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin')

def query(c,strategy):
    if strategy not in STRATEGIES or c['join'] not in ('INNER','LEFT'): raise ValueError('unsupported_candidate')
    hint=STRATEGIES[strategy]
    return ('SELECT '+('/*+ '+hint+' */ ' if hint else '')+
      'f.bucket, count(*) AS cnt, sum(f.v * coalesce(d.w, CAST(0 AS BIGINT))) AS total, '+
      'sum(f.k) AS key_sum, count(d.k) AS matches FROM f '+c['join']+
      ' JOIN d ON f.k=d.k GROUP BY f.bucket')

def oracle(c):
    out={};n,m,seed=c['n'],c['m'],c['seed'];left=c['join']=='LEFT'
    for i in range(n):
        k=0 if c['skew'] and i%10<8 else (i*97+seed)%m
        match=(k*17+seed)%1000<c['keep']
        if match or left:
            b=i%7;a=out.setdefault(b,[0,0,0,0]);a[0]+=1
            a[1]+=((i*13+seed)%997)*((k*7+seed)%101) if match else 0
            a[2]+=k;a[3]+=int(match)
    return [[b,*out[b]] for b in sorted(out)]

def features(c):
    full,remainder=divmod(c['m'],1000)
    dim=full*c['keep']+sum((k*17+c['seed'])%1000<c['keep'] for k in range(remainder))
    return {'source':'synthetic generator metadata, not AQE runtime statistics','fact_rows':c['n'],
      'filtered_dimension_rows':dim,'dimension_unsafe_row_bytes_estimate':dim*24,
      'in_memory_hash_table_bytes':None,'join':c['join'],'hot_key_input_fraction':0.8 if c['skew'] else 0,
      'hot_key_survives_dimension_filter':bool(c['seed']%1000<c['keep']),
      'broadcast_threshold_bytes':10485760}

def rule(c):
    f=features(c)
    return 'BROADCAST' if f['dimension_unsafe_row_bytes_estimate']<=f['broadcast_threshold_bytes'] else 'NATIVE'

def session(name):
    return (SparkSession.builder.master('local[2]').appName(name)
      .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1')
      .config('spark.driver.host','127.0.0.1').config('spark.driver.memory','2g')
      .config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','32')
      .config('spark.local.dir',str(WS/'spark-temp'))
      .config('spark.sql.warehouse.dir',str(HERE/'warehouse')).getOrCreate())

def inventory(spark,c):
    plans={k:plan_info(spark.sql(query(c,k))) for k in STRATEGIES}
    groups={}
    for k,p in plans.items(): groups.setdefault(p['fingerprint'],[]).append(k)
    return plans,[xs for xs in groups.values() if len(xs)>1]

def run(spark,c,strategy,expected,tag):
    record={'case':c['name'],'strategy':strategy,'run_id':tag,'started_unix':time.time()}
    spark.sparkContext.setJobGroup(tag,'bounded synthetic query',interruptOnCancel=True)
    timer=threading.Timer(90.0,spark.sparkContext.cancelJobGroup,args=(tag,))
    timer.start();begin=time.perf_counter()
    try:
        df=spark.sql(query(c,strategy));built=time.perf_counter()
        info=plan_info(df);planned=time.perf_counter()
        result=sorted([list(r) for r in df.collect()]);collected=time.perf_counter()
        record.update(info,build_s=built-begin,plan_capture_s=planned-built,collect_s=collected-planned,
          to_result_s=collected-begin,result=result,correct=result==expected,
          status='succeeded' if result==expected else 'failed',finished_unix=time.time(),
          final_plan=df._jdf.queryExecution().executedPlan().toString())
    except Exception as e:
        record.update(status='failed',error_type=type(e).__name__,elapsed_s=time.perf_counter()-begin)
    finally:
        timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
    return record

def summarize(rows):
    measured=[r for r in rows if r['phase']=='measured'];native={r['rep']:r['to_result_s'] for r in measured if r['strategy']=='NATIVE'}
    result={}
    for strategy in STRATEGIES:
        rs=[r for r in measured if r['strategy']==strategy];times=[r['to_result_s'] for r in rs]
        diffs=[native[r['rep']]-r['to_result_s'] for r in rs]
        result[strategy]={'n':len(rs),'median_s':statistics.median(times),'min_s':min(times),'max_s':max(times),
          'paired_median_saving_s':statistics.median(diffs),'faster_than_native_pairs':sum(d>0 for d in diffs),
          'worst_ratio_to_same_round_native':max(r['to_result_s']/native[r['rep']] for r in rs),
          'initial_joins':sorted({op for r in rs for op in OPS if op in r['initial_plan']}),
          'final_joins':sorted({op for r in rs for op in OPS if op in r['final_plan'].split('== Initial Plan ==')[0]})}
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--case',choices=list(CASES),required=True)
    p.add_argument('--label',required=True);p.add_argument('--reps',type=int,default=5);a=p.parse_args()
    if not a.label.replace('_','').isalnum() or not 3<=a.reps<=7: raise ValueError('invalid_bounds')
    c=CASES[a.case];assert c['n']<=12000000 and c['m']<=2000000
    out=HERE/a.label;out.mkdir(exist_ok=False);before=Budget().count();started=time.perf_counter()
    frozen={'kind':'scaled_development_not_holdout','case':c,'warmups':2,'reps':a.reps,'seed':6021,
      'candidate_ids':list(STRATEGIES),'initial_plan_deduplication':False,'new_model_requests':0,
      'cache':'uncached generated Range data; one fresh Spark process per batch, warmed shared JVM within batch; no OS/JIT reset',
      'timing':'SQL construction, initial-plan capture and collect. Candidate enumeration and final-plan logging excluded.',
      'memory':'local[2], 2 GiB requested driver heap, 32 shuffle partitions; same settings for every arm',
      'timeouts':'90 seconds per Spark job group; stop launching queries after 400 seconds; bridge timeout 540 seconds',
      'features':features(c),'transparent_rule':rule(c),
      'sources_sha256':{str(f.relative_to(WS)):hashlib.sha256(f.read_bytes()).hexdigest() for f in (Path(__file__),ROUND/'baseline.py')}}
    (out/'frozen_manifest.json').write_text(json.dumps(frozen,indent=2))
    want=oracle(c);(out/'oracle.json').write_text(json.dumps(want))
    spark=session('JevPractical_'+a.case);spark.sparkContext.setLogLevel('ERROR');rows=[]
    try:
        assert spark.version=='4.0.1'
        assert spark.conf.get('spark.sql.adaptive.enabled')=='true'
        assert spark.conf.get('spark.sql.autoBroadcastJoinThreshold') in ('10485760','10485760b')
        config={k:spark.conf.get(k) for k in ('spark.sql.adaptive.enabled','spark.sql.autoBroadcastJoinThreshold','spark.sql.shuffle.partitions','spark.sql.adaptive.skewJoin.enabled')}
        config['jvm_max_memory_bytes']=spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
        (out/'resolved_config.json').write_text(json.dumps(config,indent=2))
        setup(spark,c);plans,collisions=inventory(spark,c)
        (out/'candidates.json').write_text(json.dumps({'plans':plans,'collisions_not_removed':collisions},indent=2))
        rng=random.Random(6021)
        for phase,count in [('warmup',2),('measured',a.reps)]:
            for rep in range(count):
                arms=list(STRATEGIES);rng.shuffle(arms)
                for pos,strategy in enumerate(arms):
                    if time.perf_counter()-started>400: raise RuntimeError('batch_launch_time_budget')
                    r=run(spark,c,strategy,want,a.label+'_'+phase+'_'+str(rep)+'_'+strategy)
                    r.update(phase=phase,rep=rep,position=pos);append(out/'raw.jsonl',r);rows.append(r)
                    if r['status']!='succeeded': raise RuntimeError('failed_execution_preserved')
                print(json.dumps({'case':a.case,'phase':phase,'rep':rep,'round_complete':True}),flush=True)
        summary={'case':c,'runs':len(rows),'measured_runs':4*a.reps,'all_correct':True,
          'candidate_statistics':summarize(rows),'initial_collisions_not_removed':collisions,
          'transparent_rule':rule(c),'api_attempts_before':before,'api_attempts_after':Budget().count(),
          'elapsed_batch_s':time.perf_counter()-started,'scope':'development candidate headroom, NOT policy end-to-end or AQE counterfactual'}
        assert Budget().count()==before
        (out/'summary.json').write_text(json.dumps(summary,indent=2))
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','runs':len(rows),'all_correct':True,'new_jev_attempts':0}))
        print(json.dumps(summary,indent=2),flush=True)
    finally: spark.stop()

if __name__=='__main__': main()
