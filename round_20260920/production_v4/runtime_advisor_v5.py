"""Test fail-open runtime advice, then at most FOUR genuine Jev decisions.
The live arm accepts a valid supplied candidate within the response gate, without
pretending confidence is calibrated. It is an experimental policy, not deployable.
Every server/thread belongs to this bounded bridge job and is joined before exit.
"""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib,json,random,secrets,statistics,sys,threading,time
from pyspark.sql import SparkSession
R=Path(__file__).resolve().parent;ROUND=R.parent
sys.path[:0]=[str(ROUND/'practical_v2'),str(ROUND)]
from scale_benchmark import oracle,run,setup
from bounded_client import BoundedClient,MODEL
from safe_jev import Budget
from baseline import append
MAX_LIVE=4
DEADLINE=1.8

def payload(event):
    def extract(s):
        assert s['materialized'] is True and s['is_runtime'] is True
        return {k:s[k] for k in ('rows','runtime_size_bytes','serialized_partition_bytes')}
    left,right=extract(event['left']),extract(event['right'])
    assert 0<right['rows']<=1250000 and 0<right['runtime_size_bytes']<=33554432
    state={'decision':'after BOTH input shuffle stages completed; choose how to execute remaining join',
      'engine':'Spark 4.0.1','resources':{'cpu_slots':2,'driver_max_heap_bytes':2147483648,'available_heap_bytes':None},
      'join_inputs':{'left':left,'right':right},'native_remaining_join':event['original_join'],
      'broadcast_remaining_join':event['candidate_join'],
      'work_accounting':'Both candidates reuse already completed shuffle outputs. BROADCAST adds a broadcast exchange/hash build; it cannot avoid prior shuffles.',
      'measurement_units':'runtime_size_bytes is Spark runtime row-size statistic; serialized_partition_bytes are shuffle bytes. Neither is an in-memory hash-table size.'}
    return {'model':MODEL,'state':state,'questions':{'plan':{'type':'choice',
      'instructions':'Choose the supplied candidate expected to have lower remaining wall time using only this evidence. Do not invent plans, sizes, or performance observations. Select ABSTAIN when evidence is insufficient. No future execution outcomes are provided.',
      'criteria':{'NATIVE':'Keep the unmodified native remaining plan and ordinary AQE behavior.',
                  'BROADCAST':'Use the supplied validated broadcast-right candidate, reusing both completed input shuffles and paying the additional broadcast/hash-build cost.',
                  'ABSTAIN':'Use unmodified native planning because evidence is insufficient.'}}}}

class Service:
    def __init__(self,out,live=False):
        self.out=out;self.live=live;self.allowed=set();self.seen=set();self.lock=threading.Lock()
        self.forced='NATIVE';self.calls=[];self.callback_count=0;self.errors=[]
        self.path='/r/'+secrets.token_urlsafe(24)
        self.client=BoundedClient(late_after_s=DEADLINE) if live else None
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                self.connection.settimeout(4.0)
                choice='ABSTAIN'
                try:
                    if self.path!=owner.path: self.send_error(404);return
                    n=int(self.headers.get('Content-Length','0'))
                    if not 0<n<=32768:self.send_error(413);return
                    event=json.loads(self.rfile.read(n));tag=event['run_id']
                    with owner.lock:
                        if tag not in owner.allowed or tag in owner.seen:self.send_error(409);return
                        owner.seen.add(tag);owner.callback_count+=1
                        if owner.live:
                            if len(owner.calls)>=MAX_LIVE:raise RuntimeError('local_request_limit')
                            p=payload(event)
                            try:rec=owner.client.request(p,tag)
                            except Exception as e:rec={'status':'blocked','choice':'ABSTAIN','error_type':type(e).__name__}
                            obj={'run_id':tag,'snapshot':event,'payload':p,'record':rec}
                            owner.calls.append(obj);append(owner.out/'model_decisions.jsonl',obj)
                            if rec['status']=='succeeded' and rec['latency_s']<=DEADLINE:
                                choice=rec['choice']
                        else:
                            choice=owner.forced
                            if choice=='SLOW':time.sleep(2.6);choice='BROADCAST'
                    data=json.dumps({'choice':choice}).encode()
                    self.send_response(200);self.send_header('Content-Type','application/json')
                    self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
                except (BrokenPipeError,ConnectionResetError):
                    owner.errors.append('reply_after_caller_closed')
                except Exception as e:
                    owner.errors.append(type(e).__name__)
                    try:
                        data=b'{"choice":"ABSTAIN"}'
                        self.send_response(200);self.send_header('Content-Length',str(len(data)))
                        self.end_headers();self.wfile.write(data)
                    except Exception:pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=False
        self.thread=threading.Thread(target=self.server.serve_forever,name='ResearchLoopbackService',daemon=False)
        self.thread.start()
    @property
    def url(self):return f'http://127.0.0.1:{self.server.server_port}'+self.path
    def close(self):
        self.server.shutdown();self.thread.join();self.server.server_close()
        if self.client:self.client.close()

def main():
    out=R/'runtime_jev_v5';out.mkdir(exist_ok=False);started=time.monotonic();budget_before=Budget().count()
    jar=R/'advisor_build_v5'/'advisor.jar'
    assert json.loads((R/'advisor_build_v5'/'COMPLETED.json').read_text())['status']=='succeeded'
    spark=(SparkSession.builder.master('local[2]').appName('JevRuntimeCandidateV5')
      .config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false')
      .config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
      .config('spark.driver.memory','2g').config('spark.sql.adaptive.enabled','true')
      .config('spark.sql.shuffle.partitions','32').config('spark.driver.extraClassPath',str(jar))
      .config('spark.jars',str(jar)).config('spark.sql.extensions','org.apache.spark.sql.execution.adaptive.RuntimeCandidateAdviceExtension')
      .config('spark.local.dir',str(ROUND.parent/'spark-temp')).config('spark.sql.warehouse.dir',str(R/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');events=spark._jvm.org.apache.spark.sql.execution.adaptive.RuntimeCandidateAdviceEvents
    service=None;rows=[];tests=[]
    def execute(c,arm,tag,want,rep=-1):
        spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold',33554432) if arm=='TUNED_AQE' else spark.conf.unset('spark.sql.adaptive.autoBroadcastJoinThreshold')
        spark.conf.set('spark.research.v5.mode','OFF' if arm=='TUNED_AQE' else arm)
        spark.conf.set('spark.research.v5.run_id',tag)
        if service:
            service.allowed.add(tag);spark.conf.set('spark.research.v5.callback',service.url)
        before=json.loads(events.state())
        r=run(spark,c,'NATIVE',want,tag)
        after=json.loads(events.state());ee=[e for e in after['events'] if e['run_id']==tag]
        assert after['errors']==before['errors'],after['errors']
        r.update(arm=arm,rep=rep,events=ee,candidate_and_advice_self_s=(after['self_nanos']-before['self_nanos'])/1e9)
        append(out/'raw.jsonl',r);rows.append(r)
        assert r['status']=='succeeded'
        return r
    try:
        assert spark._jvm.java.lang.Runtime.getRuntime().maxMemory()==2147483648
        c=dict(name='callback_fault_fixture',n=1200000,m=2000000,keep=500,skew=True,seed=313,join='LEFT')
        want=oracle(c);setup(spark,c);service=Service(out,False)
        for forced in ['NATIVE','BROADCAST','INVALID','SLOW']:
            service.forced=forced
            r=execute(c,'JEV','mock_'+forced.lower(),want)
            assert len(r['events'])==1
            actual=r['events'][0]['rule_selected'];assert actual==(forced=='BROADCAST')
            final_broadcast='BroadcastHashJoin' in r['final_plan'].split('== Initial Plan ==')[0]
            assert final_broadcast==(forced=='BROADCAST')
            tests.append({'test':forced,'passed':True,'advice':r['events'][0]['advice'],'actual_application':actual})
        service.close();service=None
        cap=dict(name='oversize_build_guard',n=1200000,m=4000000,keep=500,skew=False,seed=313,join='LEFT')
        setup(spark,cap);r=execute(cap,'JEV','mock_oversize',oracle(cap));assert not r['events']
        tests.append({'test':'oversize_build_no_callback','passed':True})
        setup(spark,c);spark.conf.set('spark.research.v5.callback','http://not-loopback.invalid/r/test')
        r=execute(c,'JEV','mock_nonloopback',want)
        assert len(r['events'])==1 and r['events'][0]['advice']=='ABSTAIN' and not r['events'][0]['rule_selected']
        tests.append({'test':'nonloopback_destination_rejected_without_request','passed':True})
        assert Budget().count()==budget_before
        (out/'FAULT_TESTS.json').write_text(json.dumps({'tests':tests,'all_passed':True,'new_jev_calls':0},indent=2))
        print(json.dumps({'fault_tests_passed':len(tests),'budget_used':Budget().count()}),flush=True)
        if Budget().count()+MAX_LIVE>50:
            result={'status':'fault_tests_only_budget_limited','budget_used':Budget().count(),'live_api_calls':0,'tests':tests}
            (out/'COMPLETED.json').write_text(json.dumps(result,indent=2));print(json.dumps(result));return
        cases=[dict(name='jev_skew18m',n=18000000,m=2000000,keep=500,skew=True,seed=193,join='LEFT'),
               dict(name='jev_uniform18m',n=18000000,m=2000000,keep=500,skew=False,seed=412,join='LEFT')]
        arms=['OFF','APPLY','TUNED_AQE','JEV'];rng=random.Random(91173)
        manifest={'frozen_unix':time.time(),'cases':cases,'arms':arms,'reps':2,'seed':91173,'planned_max_requests':MAX_LIVE,
          'source_hashes':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),R/'RuntimeCandidateAdviceExtension.scala',ROUND/'practical_v2'/'bounded_client.py',ROUND/'practical_v2'/'scale_benchmark.py']},
          'policy':'Raw validated NATIVE/BROADCAST choice before 1.8-second API response gate; no calibrated-confidence claim. ABSTAIN/error/invalid/late preserves unmodified native path.',
          'callback_timeout':'200 ms connect and 2200 ms read; not cancellation of a remote billed request. Late answers cannot modify the running plan.',
          'cache':'same JVM warmed by different small fault fixtures; no dedicated per-case warmups; first live HTTP request pays connection setup',
          'scope':'illustrative directly measured runtime-policy pilot, not a statistically powered accuracy study or unseen-family production test',
          'prediction_order':'Every live choice uses only its own captured runtime inputs, before its query completes; no candidate runtime labels in request.'}
        (out/'FROZEN_LIVE_MANIFEST.json').write_text(json.dumps(manifest,indent=2))
        service=Service(out,True);summaries=[]
        for c in cases:
            setup(spark,c);want=oracle(c)
            for rep in range(2):
                order=list(arms);rng.shuffle(order)
                for arm in order:
                    if time.monotonic()-started>400:raise RuntimeError('bounded_batch_deadline')
                    r=execute(c,arm,'jev5_'+c['name']+'_'+str(rep)+'_'+arm,want,rep)
                    print(json.dumps({'case':c['name'],'rep':rep,'arm':arm,'seconds':r['to_result_s'],'advice':[e.get('advice') for e in r['events']]}),flush=True)
            rr=[r for r in rows if r['case']==c['name']];native={r['rep']:r for r in rr if r['arm']=='OFF'}
            ss={'case':c['name'],'arms':{}}
            for arm in arms:
                ar=[r for r in rr if r['arm']==arm]
                ss['arms'][arm]={'n':len(ar),'median_s':statistics.median(r['to_result_s'] for r in ar),
                    'times_s':[r['to_result_s'] for r in ar],
                    'paired_savings_vs_native_s':[native[r['rep']]['to_result_s']-r['to_result_s'] for r in ar],
                    'applied_candidates':sum(e['rule_selected'] for r in ar for e in r['events'])}
            summaries.append(ss)
        service.close();calls=service.calls;service_errors=service.errors;service=None
        bytag={r['run_id']:r for r in rows}
        for x in calls:
            x['reply_before_query_completed']=x['record'].get('completed_unix',float('inf'))<=bytag[x['run_id']]['finished_unix']
        end=Budget().count();assert end-budget_before<=MAX_LIVE
        result={'status':'succeeded','runs':len(rows),'fault_tests_passed':len(tests),'live_measured_runs':16,
          'all_correct':True,'new_jev_attempts':end-budget_before,'budget_used':end,'remaining':50-end,
          'model_choices':[{'run_id':x['run_id'],'choice':x['record']['choice'],'status':x['record']['status'],
              'confidence':x['record'].get('confidence'),'latency_s':x['record'].get('latency_s'),
              'reply_before_query_completed':x['reply_before_query_completed']} for x in calls],
          'service_errors':service_errors,'summaries':summaries,'elapsed_s':time.monotonic()-started}
        (out/'summary.json').write_text(json.dumps(result,indent=2));(out/'COMPLETED.json').write_text(json.dumps(result,indent=2))
        print(json.dumps(result,indent=2),flush=True)
    finally:
        if service:service.close()
        spark.stop()
if __name__=='__main__':main()
