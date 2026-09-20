"""Independent synthetic pilot. No external requests; no secret-file reads.
Not the missing earlier lab and NOT an AQE intervention.
"""
from pathlib import Path
import argparse, hashlib, json, random, re, statistics, time, traceback
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent
STRATEGIES = {'NATIVE': '', 'BROADCAST': 'BROADCAST(d)', 'MERGE': 'MERGE(f, d)', 'SHUFFLE_HASH': 'SHUFFLE_HASH(d)'}
DEV_CASES = [
    dict(name='uniform_selective', n=300000, m=40000, keep=10, skew=False, seed=19),
    dict(name='uniform_half', n=300000, m=40000, keep=500, skew=False, seed=19),
    dict(name='skew80_half', n=300000, m=40000, keep=500, skew=True, seed=19),
]

def append(path, obj):
    import os
    with path.open('a') as f:
        f.write(json.dumps(obj, sort_keys=True) + '\n'); f.flush(); os.fsync(f.fileno())

def session(name):
    return (SparkSession.builder.master('local[2]').appName(name)
        .config('spark.ui.enabled', 'false')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .config('spark.driver.host', '127.0.0.1')
        .config('spark.driver.memory', '1g')
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '8')
        .config('spark.sql.warehouse.dir', str(ROOT/'warehouse'))
        .getOrCreate())

def setup(spark, c):
    n,m,seed = c['n'],c['m'],c['seed']
    key = f'(id * 97 + {seed}) % {m}'
    if c['skew']: key = f'CASE WHEN id % 10 < 8 THEN 0 ELSE {key} END'
    spark.range(n).selectExpr(f'{key} AS k', f'(id * 13 + {seed}) % 997 AS v', 'id % 7 AS bucket').createOrReplaceTempView('f')
    spark.range(m).selectExpr('id AS k', f'(id * 7 + {seed}) % 101 AS w').where(f'(k * 17 + {seed}) % 1000 < {c["keep"]}').createOrReplaceTempView('d')

def expected(c):
    out = {}
    for i in range(c['n']):
        k = 0 if c['skew'] and i % 10 < 8 else (i*97+c['seed']) % c['m']
        if (k*17+c['seed']) % 1000 >= c['keep']: continue
        b = i % 7; a = out.setdefault(b, [0,0,0]); a[0] += 1
        a[1] += ((i*13+c['seed']) % 997) * ((k*7+c['seed']) % 101)
        a[2] += k
    return [[b,*out[b]] for b in sorted(out)]

def sql(strategy):
    hint = STRATEGIES[strategy]
    return f'SELECT {"/*+ " + hint + " */" if hint else ""} f.bucket, count(*) AS cnt, sum(f.v*d.w) AS total, sum(f.k) AS key_sum FROM f JOIN d ON f.k=d.k GROUP BY f.bucket'

def normalize(plan):
    plan = re.sub(r'#\d+L?', '#_', plan)
    plan = re.sub(r'\[plan_id=\d+\]', '[plan_id=_]', plan)
    plan = re.sub(r'\*\(\d+\)', '*(_)', plan)
    return plan

def plan_info(df):
    p = df._jdf.queryExecution().executedPlan()
    initial = p.initialPlan() if p.nodeName() == 'AdaptiveSparkPlan' else p
    raw = initial.toString()
    canonical = normalize(initial.canonicalized().toString())
    return {'initial_plan': raw, 'canonical_plan': canonical,
            'fingerprint': hashlib.sha256(canonical.encode()).hexdigest(),
            'logical_statistics_estimate': df._jdf.queryExecution().optimizedPlan().stats().toString()}

def candidates(spark):
    out, aliases, seen = {}, {}, {}
    for k in STRATEGIES:
        df = spark.sql(sql(k)); p = plan_info(df)
        if p['fingerprint'] in seen:
            aliases[k] = seen[p['fingerprint']]
        else:
            seen[p['fingerprint']] = k; out[k] = p
    return out, aliases

def run_one(spark, c, strategy, want):
    start = time.perf_counter(); row = {'case': c['name'], 'strategy': strategy, 'status':'running'}
    try:
        df = spark.sql(sql(strategy)); after_build = time.perf_counter()
        info = plan_info(df); after_plan = time.perf_counter()
        got = sorted([list(r) for r in df.collect()]); after_result = time.perf_counter()
        row.update(info)
        row.update(build_s=after_build-start, planning_and_capture_s=after_plan-after_build,
                   collect_s=after_result-after_plan, to_result_s=after_result-start,
                   result=got, correct=(got == want), final_plan=df._jdf.queryExecution().executedPlan().toString())
        if got != want: raise AssertionError('aggregate_result_mismatch')
        row['status'] = 'succeeded'
    except Exception as e:
        row.update(status='failed', error_type=type(e).__name__, elapsed_s=time.perf_counter()-start)
    return row

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--label',default='baseline_v1'); parser.add_argument('--reps',type=int,default=5)
    a=parser.parse_args(); assert re.fullmatch(r'[a-zA-Z0-9_-]+',a.label) and 1 <= a.reps <= 10
    out = ROOT/a.label; out.mkdir(exist_ok=False)
    manifest = {'kind':'pre_execution_pilot_baseline', 'cases':DEV_CASES, 'seed':1842,
                'repetitions':a.reps, 'warmup_rounds':2, 'aqe_intervention':False,
                'cache':'uncached generated Range inputs; same JVM; two per-strategy warmups; no persisted DataFrames',
                'timing':'to_result_s includes SQL construction, initial-plan capture, and collect; final-plan capture excluded',
                'holdout':'not generated or evaluated', 'jev_requests':0}
    spark = session('JevResearchBaseline'); spark.sparkContext.setLogLevel('ERROR')
    try:
        manifest['spark_version']=spark.version
        manifest['config']={k:spark.conf.get(k) for k in ['spark.sql.adaptive.enabled','spark.sql.shuffle.partitions','spark.sql.autoBroadcastJoinThreshold','spark.sql.join.preferSortMergeJoin','spark.sql.adaptive.skewJoin.enabled']}
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
        rng=random.Random(1842); all_rows=[]; all_summary=[]
        for c in DEV_CASES:
            setup(spark,c); want=expected(c); plans, aliases=candidates(spark)
            (out/(c['name']+'_candidates.json')).write_text(json.dumps({'case':c,'candidates':plans,'aliases':aliases,'expected_result':want},indent=2))
            for phase,count in [('warmup',2),('measured',a.reps)]:
                for rep in range(count):
                    order=list(plans); rng.shuffle(order)
                    for position,k in enumerate(order):
                        rec=run_one(spark,c,k,want); rec.update(phase=phase,rep=rep,position=position)
                        append(out/'raw.jsonl',rec); all_rows.append(rec)
                        if rec['status'] != 'succeeded':
                            raise RuntimeError('failed_run_preserved_stop_batch')
            summary={'case':c['name'],'unique_initial_plans':len(plans),'aliases':aliases,'strategies':{}}
            for k in plans:
                rs=[r for r in all_rows if r['case']==c['name'] and r['strategy']==k and r['phase']=='measured']
                ts=[r['to_result_s'] for r in rs]
                summary['strategies'][k]={'n':len(ts),'median_s':statistics.median(ts),'min_s':min(ts),'max_s':max(ts),
                   'final_join_operators':sorted(set(op for r in rs for op in ('BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin') if op in r['final_plan'].split('== Initial Plan ==')[0]))}
            all_summary.append(summary); print(json.dumps(summary),flush=True)
        (out/'summary.json').write_text(json.dumps(all_summary,indent=2))
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','runs':len(all_rows),'all_correct':True,'jev_requests':0}))
    finally:
        spark.stop()

if __name__=='__main__': main()
