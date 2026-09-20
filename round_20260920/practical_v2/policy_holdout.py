"""Frozen end-to-end whole-query policy study on new synthetic snapshots.
NOT an AQE-boundary intervention and NOT a production-readiness claim.
No timings, oracle outputs, native labels or candidate outcomes are sent to Jev.
"""
from pathlib import Path
import hashlib,json,random,statistics,sys,threading,time
import scale_benchmark as b
from bounded_client import BoundedClient,Budget,MODEL
HERE=Path(__file__).resolve().parent
CASES=[
 dict(name='held_left_selective',n=10000000,m=2000000,keep=100,skew=False,seed=1019,join='LEFT'),
 dict(name='held_inner_half',n=10000000,m=2000000,keep=500,skew=False,seed=1063,join='INNER'),
 dict(name='held_left_skew',n=10000000,m=2000000,keep=500,skew=True,seed=1073,join='LEFT'),
]
ARMS=['NATIVE','RULE','JEV_UNGATED']
INSTRUCTIONS=('Select the supplied semantically equivalent whole-query strategy expected to finish fastest on this CPU-only Spark local[2] machine. '
 'AQE remains enabled. This decision happens BEFORE execution, with no completed stages. Use only the supplied plan and synthetic input metadata. '
 'Do not invent timings or observations. Choose ABSTAIN if the evidence is insufficient to distinguish the alternatives. '
 'A supplied hint is a strategy preference, not proof that Spark must use that physical operator.')


def make_payload(c,plans,heap):
    state={'mode':'whole-query selection before execution; NOT runtime AQE intervention',
      'engine':'Apache Spark 4.0.1','resources':{'master':'local[2]','jvm_max_heap_bytes':heap,'shuffle_partitions':32},
      'completed_stages':[], 'statistics':b.features(c),
      'candidate_initial_physical_plans':{k:p['initial_plan'] for k,p in plans.items()}}
    criteria={'NATIVE':'Unhinted native Spark SQL with normal AQE',
      'BROADCAST':'Whole-query BROADCAST(d) hint, with AQE enabled',
      'MERGE':'Whole-query MERGE(f,d) hint, with AQE enabled',
      'SHUFFLE_HASH':'Whole-query SHUFFLE_HASH(d) hint, with AQE enabled',
      'ABSTAIN':'Use the unhinted native Spark query'}
    return {'model':MODEL,'state':state,'questions':{'plan':{'type':'choice','instructions':INSTRUCTIONS,'criteria':criteria}}}


def workflow(spark,c,arm,client,heap,want,tag,out):
    spark.sparkContext.setJobGroup(tag,'bounded synthetic policy query',interruptOnCancel=True)
    timer=threading.Timer(90.0,spark.sparkContext.cancelJobGroup,args=(tag,));timer.start()
    start=time.perf_counter();began=time.time();decision=None;payload=None;selected='NATIVE'
    prep_done=start;advice_done=start;record={'case':c['name'],'arm':arm,'run_id':tag,'started_unix':began}
    try:
        if arm=='NATIVE':
            df=spark.sql(b.query(c,'NATIVE'));prep_done=time.perf_counter();advice_done=prep_done
        elif arm=='RULE':
            selected=b.rule(c);df=spark.sql(b.query(c,selected));prep_done=time.perf_counter();advice_done=prep_done
        elif arm=='JEV_UNGATED':
            dfs={k:spark.sql(b.query(c,k)) for k in b.STRATEGIES}
            plans={k:b.plan_info(df) for k,df in dfs.items()}
            payload=make_payload(c,plans,heap);prep_done=time.perf_counter()
            try:decision=client.request(payload,tag)
            except Exception as e:decision={'status':'failed','choice':'ABSTAIN','error_type':type(e).__name__,'completed_unix':time.time()}
            if decision['status']=='succeeded' and decision.get('choice') in dfs:
                selected=decision['choice']
            else:selected='NATIVE'
            # Persist decision and payload before the first action on any candidate.
            b.append(out/'decisions.jsonl',{'case':c['name'],'run_id':tag,'record':decision,'payload':payload,'selected':selected})
            advice_done=time.perf_counter();df=dfs[selected]
        else:raise ValueError('unsupported_arm')
        execution_started=time.time()
        result=sorted([list(r) for r in df.collect()]);end=time.perf_counter();finished=time.time()
        record.update(selected=selected,result=result,correct=result==want,status='succeeded' if result==want else 'failed',
          workflow_s=end-start,preparation_s=prep_done-start,advice_and_recording_s=advice_done-prep_done,
          collect_s=end-advice_done,execution_started_unix=execution_started,finished_unix=finished,
          decision=decision,initial_plan=b.plan_info(df)['initial_plan'],
          final_plan=df._jdf.queryExecution().executedPlan().toString())
        if decision is not None:assert decision['completed_unix']<=execution_started
    except Exception as e:
        record.update(status='failed',error_type=type(e).__name__,elapsed_s=time.perf_counter()-start,decision=decision)
    finally:
        timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
    return record


def main():
    before=Budget().count()
    if before+9>50:raise RuntimeError('insufficient_shared_budget')
    out=HERE/'policy_holdout_v1';out.mkdir(exist_ok=False);started=time.perf_counter()
    sources=[Path(__file__),HERE/'scale_benchmark.py',HERE/'bounded_client.py',b.ROUND/'baseline.py',b.ROUND/'safe_jev.py']
    manifest={'kind':'frozen_end_to_end_pre_execution_policy_evaluation','frozen_unix':time.time(),'cases':CASES,
      'arms':ARMS,'measured_rounds':3,'spark_candidate_warmups_per_case':2,'model_warmups':0,'maximum_new_api_attempts':9,
      'order_seed':6151,'requested_heap':'2g','master':'local[2]','shuffle_partitions':32,'aqe_enabled':True,
      'input_source':'uncached synthetic Range data, shared warmed JVM; independent new generator snapshots, NOT new unseen query families or cluster workloads',
      'policy_frozen':'RULE broadcasts iff known synthetic filtered dimension rows * 24 <= 10 MiB; otherwise NATIVE. JEV_UNGATED accepts a validated supplied candidate, with no calibrated confidence threshold. Any abstention, invalid output, late rejection or error falls back to NATIVE.',
      'safety_scope':'research only; closed legal query variants; synthetic filtered dimension <= one million rows. Serialized-size rule is NOT a hash-table memory safety proof.',
      'metadata_caveat':'exact dimension row counts come from the synthetic generator, not an available general-purpose production catalog estimate. The rule and Jev receive the same source of extra size evidence.',
      'timing':'wall time before features and SQL construction through collect return; Jev includes ALL candidate planning, request, durable decision logging and execution. Native/RULE plan logging happens AFTER the timed interval. Pool setup is one-time; the first API handshake is included in its actual query.',
      'sources_sha256':{str(p.relative_to(b.WS)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}}
    (out/'frozen_manifest.json').write_text(json.dumps(manifest,indent=2))
    spark=b.session('JevPracticalHeldOutPolicy');spark.sparkContext.setLogLevel('ERROR');client=BoundedClient()
    rows=[];warmups=[];rng=random.Random(6151);summaries=[]
    try:
        assert spark.version=='4.0.1';assert spark.conf.get('spark.sql.adaptive.enabled')=='true'
        assert spark.conf.get('spark.sql.autoBroadcastJoinThreshold') in ('10485760b','10485760')
        heap=spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
        (out/'resolved_config.json').write_text(json.dumps({'spark':spark.version,'jvm_max_heap_bytes':heap,'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'),'api_attempts_before':before},indent=2))
        for c in CASES:
            assert b.features(c)['filtered_dimension_rows']<=1000000
            want=b.oracle(c);b.setup(spark,c)
            (out/(c['name']+'_oracle.json')).write_text(json.dumps(want))
            for rep in range(2):
                strategies=list(b.STRATEGIES);rng.shuffle(strategies)
                for strategy in strategies:
                    r=b.run(spark,c,strategy,want,'policy_warm_'+c['name']+'_'+str(rep)+'_'+strategy)
                    r.update(phase='warmup',rep=rep);b.append(out/'warmups.jsonl',r);warmups.append(r)
                    if r['status']!='succeeded':raise RuntimeError('warmup_failure_preserved')
            for rep in range(3):
                arms=list(ARMS);rng.shuffle(arms)
                for position,arm in enumerate(arms):
                    if time.perf_counter()-started>400:raise RuntimeError('batch_launch_time_budget')
                    tag='policy_'+c['name']+'_'+str(rep)+'_'+arm.lower()
                    r=workflow(spark,c,arm,client,heap,want,tag,out)
                    r.update(phase='measured',rep=rep,position=position)
                    b.append(out/'raw.jsonl',r);rows.append(r)
                    if r['status']!='succeeded':raise RuntimeError('policy_query_failure_preserved')
                print(json.dumps({'case':c['name'],'round':rep,'completed':True}),flush=True)
            rs=[r for r in rows if r['case']==c['name']]
            native={r['rep']:r['workflow_s'] for r in rs if r['arm']=='NATIVE'}
            s={'case':c['name'],'policies':{}}
            for arm in ARMS:
                group=[r for r in rs if r['arm']==arm];ts=[r['workflow_s'] for r in group]
                s['policies'][arm]={'n':len(group),'median_workflow_s':statistics.median(ts),'min_workflow_s':min(ts),'max_workflow_s':max(ts),
                  'paired_median_saving_s':statistics.median(native[r['rep']]-r['workflow_s'] for r in group),
                  'faster_than_native_pairs':sum(r['workflow_s']<native[r['rep']] for r in group),
                  'worst_ratio_to_same_round_native':max(r['workflow_s']/native[r['rep']] for r in group),
                  'selected':[r['selected'] for r in group],
                  'model_choices':[r['decision']['choice'] for r in group if r['decision']],
                  'confidences':[r['decision'].get('confidence') for r in group if r['decision']]}
            summaries.append(s);print(json.dumps(s),flush=True)
        actual=sum(r['decision'] is not None and 'attempt' in r['decision'] for r in rows)
        assert Budget().count()==before+actual
        result={'status':'succeeded','cases':summaries,'measured_workflows':len(rows),'spark_warmups':len(warmups),
          'all_results_correct':True,'actual_new_api_attempts':actual,'attempts_before':before,'attempts_after':Budget().count(),
          'scope':'direct end-to-end pre-execution policy timings on independent synthetic snapshots; no runtime AQE overrides; no confidence calibration',
          'cold_first_request_included':True,'elapsed_batch_s':time.perf_counter()-started}
        (out/'summary.json').write_text(json.dumps(result,indent=2))
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','measured':len(rows),'warmups':len(warmups),'all_correct':True,'new_api_attempts':actual}))
        print(json.dumps(result,indent=2),flush=True)
    finally:client.close();spark.stop()

if __name__=='__main__':main()
