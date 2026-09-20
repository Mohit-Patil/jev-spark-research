"""Build historical evidence from earlier measurements; verify ten small queries;
freeze policies, cases and sources before any evaluation query or live request.
"""
import json, statistics, subprocess, sys, time
from history_core import *

def main():
    out=R/'prepare_v1';out.mkdir(exist_ok=False);before=AuthorizedBudget().count()
    p=subprocess.run([sys.executable,str(R/'test_history.py')],capture_output=True,text=True,timeout=90)
    (out/'tests.log').write_text(p.stdout+p.stderr);print(p.stdout+p.stderr,flush=True)
    if p.returncode:raise RuntimeError('offline_tests_failed')
    spark=corpus.session();spark.sparkContext.setLogLevel('ERROR');history=[];smoke=[]
    try:
        for name in TRAIN_BATCHES:
            base=ROUND/'production_v3'/name;manifest=json.loads((base/'manifest.json').read_text())
            old=rows(base/'raw.jsonl');assert json.loads((base/'COMPLETED.json').read_text())['status']=='succeeded'
            want=corpus.oracle(manifest['case']);assert all(r['status']=='succeeded' and r['result']==want for r in old)
            f=metadata_features(spark,manifest['case'],base/'data')
            med={k:statistics.median(r['to_result_s'] for r in old if r['arm']==k and r['phase']=='measured') for k in ACTIONS}
            history.append({'historical_id':name,'features':f,'candidate_median_s':med,'repetitions':3,
              'measurement':'earlier whole-query wall times, no advisor calls, warmed local[2] JVM; three repetitions each'})
        c=dict(n=10000,m=2048,width=128,keep=100,seed=941,family='LEFT_LOOKUP')
        corpus.write_inputs(spark,c,out)
        for family in ('LEFT_LOOKUP','LEFT_RESIDUAL'):
            cc={**c,'family':family};want=oracle(cc)
            for arm in corpus.ARMS:
                corpus.configure(spark,arm);df=spark.sql(query(cc,arm));got=sorted([list(x) for x in df.collect()])
                rec={'family':family,'arm':arm,'result':got,'correct':got==want,'final_plan':df._jdf.queryExecution().executedPlan().toString()}
                append(out/'smoke.jsonl',rec);smoke.append(rec);assert got==want
        f=metadata_features(spark,c,out/'data');assert f['left']['file_rows']==10000 and f['right']['file_rows']==2048
        plans={k:plan_info(spark.sql(query(c,k))) for k in ACTIONS}
        sizes={v:len(encode(payload(f,plans,history if v=='HISTORY' else None))) for v in ('ZERO','HISTORY')}
        dump(R/'HISTORY.json',{'created_unix':time.time(),'observations':history,
          'source_hashes':{str((ROUND/'production_v3'/n/'raw.jsonl').relative_to(WS)):sha(ROUND/'production_v3'/n/'raw.jsonl') for n in TRAIN_BATCHES}})
        files=[R/'history_core.py',R/'experiment.py',R/'test_history.py',ROUND/'production_v3'/'parquet_corpus.py',
          ROUND/'production_v4'/'authorized_budget.py',ROUND/'safe_jev.py',ROUND/'baseline.py']
        protocol={'created_unix':time.time(),'kind':'prospective pre-execution history study; not AQE runtime intervention',
          'evaluation_cases':CASES,'policies':POLICIES,'repetitions':5,'per_case_model_attempts':10,
          'history_sha256':sha(R/'HISTORY.json'),'source_hashes':{str(p.relative_to(WS)):sha(p) for p in files},
          'history_policy':'Four prior development snapshots only; never append evaluation feedback.',
          'local_policy':'1-nearest log-distance over measured/estimated input size features; family match required; choose alternative only for historical >10% and >50ms advantage; distance <=4.',
          'native_control':'10 MiB initial/adaptive default and separate fixed 64 MiB native comparator; no optimizer rules disabled.',
          'scope':'Three independent new parameter/data snapshots of LEFT_LOOKUP and one previously unused LEFT_RESIDUAL SQL family. Not a statistically broad unseen-family benchmark.',
          'deadline_s':1.25,'feedback_isolation':'Other arms and warmups can precede a live prediction, but their outputs/times are excluded and prompts/history remain frozen.',
          'selection_quality_limit':'Full-query workflow results primary; no accuracy inferred from agreement with Spark or unexecuted counterfactual.'}
        if (R/'FROZEN_PROTOCOL.json').exists():raise ValueError('protocol_already_frozen')
        dump(R/'FROZEN_PROTOCOL.json',protocol)
        report={'status':'succeeded','offline_tests':json.loads((R/'OFFLINE_TESTS.json').read_text()),
          'smoke_queries':len(smoke),'all_correct':True,'historical_cases':len(history),'request_sizes_smoke':sizes,
          'new_api_calls':0,'budget_before':before,'budget_after':AuthorizedBudget().count(),'protocol_sha256':sha(R/'FROZEN_PROTOCOL.json')}
        dump(out/'COMPLETED.json',report);print(json.dumps(report,indent=2))
    finally:spark.stop()
if __name__=='__main__':main()
