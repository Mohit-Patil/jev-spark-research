"""Fixed online representation ablation on fresh synthetic snapshots.
54 queries (9 execution warmups + 45 measured), at most 18 paid requests,
no prompt/threshold adaptation, same compiled V5 intervention and fallbacks.
"""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib,json,random,secrets,statistics,sys,threading,time
from pyspark.sql import SparkSession
R=Path(__file__).resolve().parent;ROUND=R.parent
sys.path[:0]=[str(ROUND/'practical_v2'),str(ROUND)]
from authorized_budget import AuthorizedBudget,BatchBudget
from bounded_client import BoundedClient
from representation_inputs import build
from scale_benchmark import oracle,run,setup
from baseline import append
from runtime_benchmark import signature
CASES=[dict(name='v6_small_uniform',n=300000,m=2000000,keep=500,skew=False,seed=227,join='LEFT'),
       dict(name='v6_large_uniform',n=24000000,m=2000000,keep=500,skew=False,seed=419,join='LEFT'),
       dict(name='v6_large_skew',n=24000000,m=2000000,keep=500,skew=True,seed=163,join='LEFT')]
ARMS=['OFF','APPLY','TUNED_AQE','JEV_VERBOSE','JEV_COMPACT']
DEADLINE=1.8

class Service:
    def __init__(self,out):
        self.out=out;self.allowed={};self.seen=set();self.lock=threading.Lock();self.calls=[];self.errors=[]
        self.batch=BatchBudget(18);self.client=BoundedClient(budget=self.batch,late_after_s=DEADLINE)
        self.path='/r/'+secrets.token_urlsafe(24);owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                self.connection.settimeout(4.0)
                try:
                    if self.path!=owner.path:self.send_error(404);return
                    n=int(self.headers.get('Content-Length','0'))
                    if not 0<n<=32768:self.send_error(413);return
                    event=json.loads(self.rfile.read(n));tag=event['run_id']
                    with owner.lock:
                        if tag not in owner.allowed or tag in owner.seen:self.send_error(409);return
                        owner.seen.add(tag);view=owner.allowed[tag]
                        t=time.perf_counter();p=build(event,view);prep=time.perf_counter()-t
                        try:rec=owner.client.request(p,tag)
                        except Exception as e:rec={'status':'blocked','choice':'ABSTAIN','error_type':type(e).__name__}
                        result={'run_id':tag,'view':view,'snapshot':event,'payload':p,'preparation_s':prep,'record':rec}
                        owner.calls.append(result);append(owner.out/'decisions.jsonl',result)
                        chosen=rec['choice'] if rec['status']=='succeeded' and rec['latency_s']<=DEADLINE else 'ABSTAIN'
                    data=json.dumps({'choice':chosen}).encode()
                    self.send_response(200);self.send_header('Content-Type','application/json')
                    self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
                except (BrokenPipeError,ConnectionResetError):owner.errors.append('reply_after_caller_closed')
                except Exception as e:
                    owner.errors.append(type(e).__name__)
                    try:
                        data=b'{"choice":"ABSTAIN"}';self.send_response(200);self.send_header('Content-Length',str(len(data)))
                        self.end_headers();self.wfile.write(data)
                    except Exception:pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=False
        self.thread=threading.Thread(target=self.server.serve_forever,name='BoundedRepresentationService',daemon=False);self.thread.start()
    @property
    def url(self):return f'http://127.0.0.1:{self.server.server_port}'+self.path
    def close(self):
        self.server.shutdown();self.thread.join();self.server.server_close();self.client.close()

def main():
    out=R/'runtime_representation_v6';out.mkdir(exist_ok=False);started=time.monotonic()
    before=AuthorizedBudget().status()
    if before['remaining']<18:raise RuntimeError('insufficient_global_request_allowance')
    assert json.loads((R/'REPRESENTATION_TESTS.json').read_text())['passed']
    assert json.loads((R/'BUDGET_TEST_RESULTS.json').read_text())['passed']
    build_dir=R/'advisor_build_v5';jar=build_dir/'advisor.jar';build_manifest=json.loads((build_dir/'COMPLETED.json').read_text())
    assert hashlib.sha256(jar.read_bytes()).hexdigest()==build_manifest['jar_sha256']
    assert hashlib.sha256((R/'RuntimeCandidateAdviceExtension.scala').read_bytes()).hexdigest()==build_manifest['source_sha256']
    sources=[Path(__file__),R/'representation_inputs.py',R/'authorized_budget.py',R/'runtime_advisor_v5.py',
        ROUND/'practical_v2'/'bounded_client.py',ROUND/'practical_v2'/'scale_benchmark.py',R/'RuntimeCandidateAdviceExtension.scala']
    hashes={str(p.relative_to(ROUND)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    manifest={'frozen_unix':time.time(),'cases':CASES,'arms':ARMS,'reps':3,'random_seed':337191,
        'execution_warmups_per_case':['OFF','APPLY','TUNED_AQE'],'max_live_attempts':18,
        'request_budget_at_start':before,'hashes':hashes,'jar_sha256':build_manifest['jar_sha256'],
        'policy':'NATIVE/BROADCAST/ABSTAIN; raw validated choice within 1.8s response gate; no confidence tuning; errors/late/invalid fall back to unmodified native',
        'baseline':'TUNED_AQE uses 32MiB adaptive-only broadcast threshold, identical to fixed-rule bound; default auto broadcast stays 10MiB; all other settings identical',
        'measurement':'Whole SQL, initial-plan capture, query stages, candidate building, callback/model wall time, and result collection are timed together. Oracle and final logging excluded equally.',
        'feature_ablation':'Identical question and option descriptions; COMPACT replaces complete join plan text and ordered partition arrays with verified join semantics and numerical summaries. This loses information; not a pure lossless encoding comparison.',
        'limits':'Three repetitions in each of three new snapshots from same LEFT-join family; warmed shared JVM, uncontrolled OS/GC, first model call pays connection setup, server caching unknown; do not call this a production or unseen-family benchmark.'}
    (out/'FROZEN_MANIFEST.json').write_text(json.dumps(manifest,indent=2))
    spark=(SparkSession.builder.master('local[2]').appName('JevRuntimeRepresentationV6')
      .config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false')
      .config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
      .config('spark.driver.memory','2g').config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','32')
      .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
      .config('spark.sql.extensions','org.apache.spark.sql.execution.adaptive.RuntimeCandidateAdviceExtension')
      .config('spark.local.dir',str(ROUND.parent/'spark-temp')).config('spark.sql.warehouse.dir',str(R/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');events=spark._jvm.org.apache.spark.sql.execution.adaptive.RuntimeCandidateAdviceEvents
    service=None;rows=[];summaries=[];rng=random.Random(337191)
    try:
        assert spark.version=='4.0.1' and spark._jvm.java.lang.Runtime.getRuntime().maxMemory()==2147483648
        assert spark.conf.get('spark.sql.autoBroadcastJoinThreshold') in ('10485760','10485760b')
        service=Service(out)
        def execute(c,arm,tag,want,phase,rep):
            if time.monotonic()-started>430:raise RuntimeError('bounded_batch_launch_deadline')
            spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold',33554432) if arm=='TUNED_AQE' else spark.conf.unset('spark.sql.adaptive.autoBroadcastJoinThreshold')
            mode='JEV' if arm.startswith('JEV_') else 'OFF' if arm=='TUNED_AQE' else arm
            spark.conf.set('spark.research.v5.mode',mode);spark.conf.set('spark.research.v5.run_id',tag)
            if mode=='JEV':
                service.allowed[tag]=arm.split('_',1)[1];spark.conf.set('spark.research.v5.callback',service.url)
            st=json.loads(events.state());r=run(spark,c,'NATIVE',want,tag);en=json.loads(events.state())
            ee=[e for e in en['events'] if e['run_id']==tag]
            r.update(arm=arm,phase=phase,rep=rep,events=ee,signatures=[signature(e) for e in ee],
                candidate_and_advisor_s=(en['self_nanos']-st['self_nanos'])/1e9,
                adaptive_broadcast_threshold_bytes=33554432 if arm=='TUNED_AQE' else 10485760)
            append(out/'raw.jsonl',r);rows.append(r)
            if r['status']!='succeeded' or en['errors']:raise RuntimeError('query_or_extension_error_preserved')
            if mode in ('APPLY','JEV') and len(ee)!=1:raise RuntimeError('unexpected_runtime_candidate_count')
            print(json.dumps({'case':c['name'],'phase':phase,'rep':rep,'arm':arm,'seconds':r['to_result_s'],
                'advice':[e.get('advice') for e in ee]}),flush=True)
        for c in CASES:
            want=oracle(c);setup(spark,c);(out/(c['name']+'_oracle.json')).write_text(json.dumps(want))
            for arm in ('OFF','APPLY','TUNED_AQE'):execute(c,arm,c['name']+'_warm_'+arm,want,'warmup',-1)
            for rep in range(3):
                arms=list(ARMS);rng.shuffle(arms)
                for arm in arms:execute(c,arm,c['name']+'_'+str(rep)+'_'+arm,want,'measured',rep)
            measured=[r for r in rows if r['case']==c['name'] and r['phase']=='measured'];ss={'case':c['name'],'arms':{}}
            baseline={r['rep']:r for r in measured if r['arm']=='OFF'}
            tuned={r['rep']:r for r in measured if r['arm']=='TUNED_AQE'}
            fixed={r['rep']:r for r in measured if r['arm']=='APPLY'}
            for arm in ARMS:
                ar=[r for r in measured if r['arm']==arm];times=[r['to_result_s'] for r in ar]
                saved=[baseline[r['rep']]['to_result_s']-r['to_result_s'] for r in ar]
                ss['arms'][arm]={'n':len(ar),'median_s':statistics.median(times),'times_s':times,
                    'median_paired_saving_vs_default_s':statistics.median(saved),'faster_than_default_pairs':sum(x>0 for x in saved),
                    'worst_ratio_vs_default':max(r['to_result_s']/baseline[r['rep']]['to_result_s'] for r in ar),
                    'median_paired_saving_vs_tuned_s':statistics.median(tuned[r['rep']]['to_result_s']-r['to_result_s'] for r in ar),
                    'median_paired_saving_vs_fixed_s':statistics.median(fixed[r['rep']]['to_result_s']-r['to_result_s'] for r in ar),
                    'applied':sum(e['rule_selected'] for r in ar for e in r['events']),
                    'state_matches_fixed_rule':sum(r['signatures']==fixed[r['rep']]['signatures'] for r in ar) if arm.startswith('JEV_') else None}
            summaries.append(ss);(out/'partial_summary.json').write_text(json.dumps(summaries,indent=2));print(json.dumps(ss,indent=2),flush=True)
        service.close();calls=service.calls;service_errors=service.errors;attempts=service.batch.used;service=None
        assert attempts<=18 and len(calls)==18
        bytag={r['run_id']:r for r in rows};view_stats={}
        for c in calls:
            record=c['record'];r=bytag[c['run_id']]
            if record['status']=='succeeded':assert record['completed_unix']<=r['finished_unix']
            body=json.dumps(c['payload'],separators=(',',':'),allow_nan=False).encode()
            assert hashlib.sha256(body).hexdigest()==record['request_sha256']
            c['applied']=bool(r['events'][0]['rule_selected'])
        for view in ('VERBOSE','COMPACT'):
            cs=[c for c in calls if c['view']==view];lat=[c['record']['latency_s'] for c in cs]
            view_stats[view]={'calls':len(cs),'successful':sum(c['record']['status']=='succeeded' for c in cs),
                'choices':{k:sum(c['record']['choice']==k for c in cs) for k in ('NATIVE','BROADCAST','ABSTAIN')},
                'applied':sum(c['applied'] for c in cs),'median_http_s':statistics.median(lat),'min_http_s':min(lat),'max_http_s':max(lat),
                'input_tokens':sum(c['record'].get('usage',{}).get('input_tokens',0) for c in cs),
                'output_tokens':sum(c['record'].get('usage',{}).get('output_tokens',0) for c in cs),
                'median_request_bytes':statistics.median(c['record']['request_bytes'] for c in cs),
                'median_preparation_s':statistics.median(c['preparation_s'] for c in cs)}
        for p in sources:assert hashes[str(p.relative_to(ROUND))]==hashlib.sha256(p.read_bytes()).hexdigest()
        after=AuthorizedBudget().status();assert after['attempts_used']-before['attempts_used']==attempts
        result={'status':'succeeded','runs':len(rows),'measured_runs':45,'all_correct':True,'new_live_attempts':attempts,
            'budget':after,'views':view_stats,'cases':summaries,'service_errors':service_errors,
            'actual_jev_applications':sum(c['applied'] for c in calls),'elapsed_s':time.monotonic()-started}
        (out/'summary.json').write_text(json.dumps(result,indent=2));(out/'COMPLETED.json').write_text(json.dumps(result,indent=2))
        print(json.dumps(result,indent=2),flush=True)
    finally:
        if service:service.close()
        spark.stop()
if __name__=='__main__':main()
