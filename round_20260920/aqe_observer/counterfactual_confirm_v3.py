"""Preregistered single-boundary CURRENT versus PROPOSED trial on synthetic data.
Matches the exact normalized plans and visible stage state from a previous live shadow
snapshot. It is not whole-query join-hint labeling and makes zero Jev calls.
"""
from pathlib import Path
import hashlib,json,random,statistics,subprocess,sys,time
import pyspark
from pyspark.sql import SparkSession
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from baseline import append,expected,normalize,plan_info,setup,sql
from safe_jev import Budget
CLASS='org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV3'

def signature(pair):
    state={k:{'plan':normalize(pair[k]['remaining_physical_plan']),'stages':pair[k]['stages']} for k in ('current','proposed')}
    data=json.dumps(state,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
    return hashlib.sha256(data).hexdigest()

def main():
    out=ROOT/'counterfactual_confirm_v3';out.mkdir(exist_ok=False);before=Budget().count()
    assert pyspark.__version__=='4.0.1'
    source=ROOT/'NativeBoundaryObserverV3.scala'
    live=ROOT/'live_shadow_v2'
    previous=[json.loads(x) for x in (live/'callback_snapshots.jsonl').read_text().splitlines()]
    target=next(p for p in previous if p['run_id']=='live_uniform_medium')
    assert target['native_would_choose']=='PROPOSED' and not target['plans_equal']
    assert all(not target[k]['plan_truncated'] for k in ('current','proposed'))
    anchor=signature(target)
    c=next(c for c in json.loads((live/'manifest.json').read_text())['cases'] if c['name']=='live_uniform_medium')
    frozen={'kind':'fixed 30-round confirmation of counterfactual_trial_v3; same anchor, data and observer','frozen_unix':time.time(),
      'case':c,'target_signature':anchor,'target_snapshot':target,'arms':['CURRENT','PROPOSED'],'seed':1847,
      'calibration_runs':1,'warmup_rounds':2,'measured_rounds':30,'maximum_overrides_per_query':1,
      'match_rule':'normalized current/proposed remaining plans plus ALL visible stage fields, including materialization flags, row counts, size statistics and reported partition byte arrays',
      'not_matched':'in-flight task progress not exposed here, JVM heap occupancy, GC state, OS scheduling and caches. These remain limitations even when the observed-state hash matches.',
      'on_mismatch':'execute native unchanged, preserve the run, exclude only from state-matched branch comparison; retain in all-trial totals',
      'timing':'remaining time from proposed-cost call entry through Python collect return plus one JVM clock RPC; whole-query timing stops at collect return before that RPC and final-plan capture',
      'scope':'one decision only; later AQE decisions are native. CURRENT does not force a join strategy for the whole query.',
      'cache':'uncached generated Range inputs, shared warmed JVM, no OS-cache reset','api_calls':0,
      'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),source,ROOT.parent/'baseline.py']}}
    (out/'frozen_trial.json').write_text(json.dumps(frozen,indent=2))
    classes=out/'classes';classes.mkdir();jars=Path(pyspark.__file__).parent/'jars';jar=out/'counterfactual.jar'
    for i,cmd in enumerate([['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source)],
                            ['jar','--create','--file',str(jar),'-C',str(classes),'.']]):
        p=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=90)
        (out/f'compile_{i}.json').write_text(json.dumps({'command':cmd,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr},indent=2))
        print(json.dumps({'compile_step':i,'returncode':p.returncode,'stderr':p.stderr[:6000]}),flush=True)
        if p.returncode:raise RuntimeError('compiler_failure')
    trace=out/'boundary_pairs.jsonl'
    spark=(SparkSession.builder.master('local[2]').appName('JevObservedStateCounterfactualV3')
       .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
       .config('spark.driver.memory','1g').config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','8')
       .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
       .config('spark.research.observer.path',str(trace)).config('spark.sql.warehouse.dir',str(ROOT/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');rows=[];rng=random.Random(1847)
    try:
        assert spark.version=='4.0.1'
        spark.conf.set('spark.sql.adaptive.customCostEvaluatorClass',CLASS)
        spark.conf.set('spark.research.target_signature',anchor)
        js=spark._jvm.org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV3
        setup(spark,c);want=expected(c)
        def run(phase,rep,arm,position):
            tag='cf3_'+phase+'_'+str(rep)+'_'+arm
            spark.conf.set('spark.research.run_id',tag);spark.conf.set('spark.research.arm',arm)
            metrics_a=json.loads(js.metrics());begin=time.perf_counter()
            df=spark.sql(sql('NATIVE'));info=plan_info(df)
            got=sorted([list(r) for r in df.collect()]);to_result=time.perf_counter()-begin
            clock_start=time.perf_counter();end_nanos=js.clockNanos();clock_rpc=time.perf_counter()-clock_start
            final=df._jdf.queryExecution().executedPlan().toString();metrics_b=json.loads(js.metrics())
            pairs=[json.loads(s) for s in trace.read_text().splitlines() if s.strip()]
            pairs=[p for p in pairs if p['run_id']==tag]
            chosen=[p for p in pairs if p['target_selected_once']]
            assert all(signature(p)==p['state_signature'] for p in pairs), 'cross_language_signature_mismatch'
            assert len(chosen)<=1
            r={'case':c['name'],'run_id':tag,'phase':phase,'rep':rep,'arm':arm,'position':position,'to_result_s':to_result,
              'clock_rpc_wall_s':clock_rpc,'result':got,'correct':got==want,'status':'succeeded' if got==want else 'failed',
              'final_plan':final,**info,'target_reached':bool(chosen),'remaining_s':None,
              'observer_metrics':{k:metrics_b[k]-metrics_a[k] for k in metrics_b},
              'interventions':sum(p['intervention'] for p in pairs),'pair_count':len(pairs)}
            if chosen:
                p=chosen[0];assert p['state_signature']==anchor
                r.update(remaining_s=(end_nanos-p['decision_entry_nano_time'])/1e9,matched_pair=p,
                         effective_choice=p['effective_choice'])
            append(out/'raw.jsonl',r);rows.append(r)
            assert r['correct'] and r['remaining_s'] is None or r['correct'] and r['remaining_s']>=0
            for k in ('errors','unmatched','dropped'):assert r['observer_metrics'][k]==0
            assert r['observer_metrics']['calls']==2*r['observer_metrics']['pairs']
            assert r['interventions']==(1 if chosen and arm=='CURRENT' else 0)
            if chosen:assert r['effective_choice']==('CURRENT' if arm=='CURRENT' else 'PROPOSED')
            for p in pairs:
                if not p['target_selected_once']:assert p['effective_choice']==p['native_would_choose'] and not p['intervention']
            return r
        calibration=run('calibration',0,'CONTROL',0)
        print(json.dumps({'calibration_target_reached':calibration['target_reached'],'signature':anchor}),flush=True)
        for phase,reps in [('warmup',2),('measured',30)]:
            for rep in range(reps):
                arms=['CURRENT','PROPOSED'];rng.shuffle(arms)
                for position,arm in enumerate(arms):run(phase,rep,arm,position)
        result={'status':'succeeded','target_signature':anchor,'runs':len(rows),'measured_runs':60,'all_correct':True,
          'interventions':sum(r['interventions'] for r in rows),'new_jev_calls':0,'budget_used':Budget().count(),
          'arms':{},'scope':'single decision, observed-state matched exploratory trial; no Jev-directed intervention and no Jev end-to-end speedup'}
        for arm in ['CURRENT','PROPOSED']:
            all_rows=[r for r in rows if r['phase']=='measured' and r['arm']==arm]
            matched=[r for r in all_rows if r['target_reached']]
            result['arms'][arm]={'attempted':len(all_rows),'matched':len(matched),'unmatched_native_fallback':len(all_rows)-len(matched),
              'all_trials_whole_query_median_s':statistics.median(r['to_result_s'] for r in all_rows),
              'matched_remaining_median_s':statistics.median(r['remaining_s'] for r in matched) if matched else None,
              'matched_remaining_min_s':min((r['remaining_s'] for r in matched),default=None),
              'matched_remaining_max_s':max((r['remaining_s'] for r in matched),default=None),
              'matched_whole_query_median_s':statistics.median(r['to_result_s'] for r in matched) if matched else None,
              'final_join_operators':sorted({op for r in all_rows for op in ['BroadcastHashJoin','SortMergeJoin','ShuffledHashJoin'] if op in r['final_plan'].split('== Initial Plan ==')[0]})}
        paired=[]
        for rep in range(30):
            group={r['arm']:r for r in rows if r['phase']=='measured' and r['rep']==rep and r['target_reached']}
            if len(group)==2:paired.append(group['PROPOSED']['remaining_s']-group['CURRENT']['remaining_s'])
        result['matched_randomized_rounds']=len(paired)
        result['paired_remaining_median_saving_current_s']=statistics.median(paired) if paired else None
        result['paired_current_faster_count']=sum(d>0 for d in paired)
        result['median_clock_rpc_s']=statistics.median(r['clock_rpc_wall_s'] for r in rows)
        result['limitations']=frozen['not_matched']
        assert Budget().count()==before
        (out/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2),flush=True)
        (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','runs':len(rows),'all_correct':True,'interventions':result['interventions'],'api_calls':0}))
    finally:spark.stop()
if __name__=='__main__':main()
