"""Scaled native AQE coverage and bounded pooled live shadow diagnostics.
No running plan is modified. At most one STATS request on the first changed
current/proposed pair per synthetic query, at most three new requests total.
"""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
import hashlib,json,secrets,subprocess,sys,threading,time
import pyspark
from pyspark.sql import SparkSession
import scale_benchmark as b
from bounded_client import BoundedClient,Budget,MODEL
HERE=Path(__file__).resolve().parent
AQE=b.ROUND/'aqe_observer'
sys.path.insert(0,str(AQE))
from runtime_features import CRITERIA,INSTRUCTIONS,project
CLASS='org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV2'
CASES=[dict(b.CASES[name]) for name in ('left_uniform_10','inner_uniform_50','left_skew80_50')]

def main():
    before=Budget().count()
    if before+3>50:raise RuntimeError('insufficient_shared_budget')
    out=HERE/'scaled_aqe_shadow_v1';out.mkdir(exist_ok=False)
    source=AQE/'NativeBoundaryObserverV2.scala';classes=out/'classes';classes.mkdir()
    jar=out/'observer.jar';jars=Path(pyspark.__file__).parent/'jars'
    sources=[Path(__file__),source,AQE/'runtime_features.py',HERE/'bounded_client.py',HERE/'scale_benchmark.py',b.ROUND/'baseline.py',b.ROUND/'safe_jev.py']
    manifest={'kind':'scaled_actual_AQE_shadow_and_candidate_coverage_not_speedup_trial','cases':CASES,
      'maximum_new_api_attempts':3,'budget_before':before,'master':'local[2]','requested_heap':'2g','shuffle_partitions':32,
      'gate':'first structurally changed current/proposed pair in each query; identical pairs make no request',
      'model':MODEL,'view':'STATS only','interventions':0,'cold_first_call_included':True,
      'caveat':'blocking observation diagnostics, no overhead comparison; no counterfactual branch labels; visible stages are not a complete execution history',
      'source_sha256':{str(f.relative_to(b.WS)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sources}}
    (out/'frozen_manifest.json').write_text(json.dumps(manifest,indent=2))
    assert pyspark.__version__=='4.0.1'
    for i,cmd in enumerate([
        ['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source)],
        ['jar','--create','--file',str(jar),'-C',str(classes),'.']]):
        r=subprocess.run(cmd,cwd=HERE,capture_output=True,text=True,timeout=90)
        (out/f'compile_{i}.json').write_text(json.dumps({'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'command':cmd},indent=2))
        print(json.dumps({'compile_step':i,'returncode':r.returncode}),flush=True)
        if r.returncode:raise RuntimeError('observer_compile_failure')
    tags={c['name'] for c in CASES};seen=set();decisions=[];handler_errors=[]
    callback_path='/shadow/'+secrets.token_hex(16);client=BoundedClient()
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup();self.connection.settimeout(5.0)
        def log_message(self,*args):pass
        def do_POST(self):
            try:
                size=int(self.headers.get('Content-Length','0'))
                if self.path!=callback_path or not 1<=size<=131072:raise ValueError('invalid_callback')
                pair=json.loads(self.rfile.read(size));tag=pair.get('run_id')
                if tag not in tags:raise ValueError('unrecognized_run')
                if tag not in seen and not pair.get('plans_equal',True):
                    seen.add(tag);received=time.time();state=project(pair,'STATS')
                    assert 'native_would_choose' not in json.dumps(state) and 'stock_cost' not in json.dumps(state)
                    payload={'model':MODEL,'state':state,'questions':{'plan':{'type':'choice','instructions':INSTRUCTIONS,'criteria':CRITERIA}}}
                    b.append(out/'callback_snapshots.jsonl',pair)
                    r=client.request(payload,'scaled_aqe_'+tag)
                    entry={'run_id':tag,'view':'STATS','received_unix':received,'record':r,'payload':payload,
                      'native_would_choose_for_audit_only':pair['native_would_choose']}
                    decisions.append(entry);b.append(out/'decisions.jsonl',entry)
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}')
            except Exception as e:
                handler_errors.append({'error_type':type(e).__name__})
                try:self.send_error(500)
                except Exception:pass
    server=HTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.05},name='bounded-scaled-aqe-shadow')
    worker.start();spark=None;rows=[]
    try:
        endpoint='http://127.0.0.1:'+str(server.server_port)+callback_path
        spark=(SparkSession.builder.master('local[2]').appName('JevScaledAQEShadow')
          .config('spark.ui.enabled','false').config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
          .config('spark.driver.memory','2g').config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','32')
          .config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))
          .config('spark.research.observer.path',str(out/'boundary_pairs.jsonl'))
          .config('spark.research.shadow.endpoint',endpoint)
          .config('spark.sql.warehouse.dir',str(HERE/'warehouse')).getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        assert spark.version=='4.0.1';assert spark.conf.get('spark.sql.adaptive.enabled')=='true'
        assert spark.conf.get('spark.sql.autoBroadcastJoinThreshold') in ('10485760','10485760b')
        spark.conf.set('spark.sql.adaptive.customCostEvaluatorClass',CLASS)
        js=spark._jvm.org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV2
        (out/'resolved_config.json').write_text(json.dumps({'jvm_max_heap_bytes':spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
          'auto_broadcast_threshold':spark.conf.get('spark.sql.autoBroadcastJoinThreshold'),'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions')},indent=2))
        for c in CASES:
            want=b.oracle(c);b.setup(spark,c);spark.conf.set('spark.research.run_id',c['name'])
            metrics_a=json.loads(js.metrics());r=b.run(spark,c,'NATIVE',want,c['name']);metrics_b=json.loads(js.metrics())
            r['observer_metrics']={k:metrics_b[k]-metrics_a[k] for k in metrics_b}
            b.append(out/'raw.jsonl',r);rows.append(r)
            if r['status']!='succeeded':raise RuntimeError('query_failure_preserved')
            for key in ('errors','unmatched','dropped'):assert r['observer_metrics'][key]==0
        spark.stop();spark=None
    finally:
        if spark is not None:spark.stop()
        server.shutdown();server.server_close();worker.join(timeout=5);client.close()
    assert not worker.is_alive()
    pairs=[json.loads(x) for x in (out/'boundary_pairs.jsonl').read_text().splitlines()]
    for pair in pairs:
        assert pair['cost_returned_unchanged'] and not pair['intervention']
        old=pair['current']['stock_cost_value'];new=pair['proposed']['stock_cost_value']
        assert pair['native_would_choose']==('PROPOSED' if new<old or (new==old and not pair['plans_equal']) else 'CURRENT')
        for side in ('current','proposed'):
            for stage in pair[side]['stages']:
                sizes=stage.get('shuffle_serialized_partition_bytes')
                if sizes is not None:assert sum(sizes)==stage['partition_bytes_total'] and len(sizes)==stage['partition_count']
    bytag={r['case']:r for r in rows}
    for decision in decisions:
        assert bytag[decision['run_id']]['started_unix']<=decision['received_unix']<=decision['record']['completed_unix']<=bytag[decision['run_id']]['finished_unix']
    coverage=[]
    for c in CASES:
        ps=[p for p in pairs if p['run_id']==c['name']]
        coverage.append({'case':c['name'],'pairs':len(ps),'changed_pairs':sum(not p['plans_equal'] for p in ps),
          'pairs_with_broadcast_anywhere_in_current_or_proposed_text':sum(any('BroadcastHashJoin' in p[side]['remaining_physical_plan'] for side in ('current','proposed')) for p in ps),
          'unavailable_broadcast_in_every_pair':all(all('BroadcastHashJoin' not in p[side]['remaining_physical_plan'] for side in ('current','proposed')) for p in ps),
          'scope':'absence across full plan text proves unavailable in these snapshots; presence alone does not prove it is remaining unexecuted work'})
    after=Budget().count();assert 0<=after-before<=3
    report={'status':'succeeded','queries':len(rows),'all_correct':True,'pairs':len(pairs),'coverage':coverage,
      'handler_errors':handler_errors,'callback_errors':[p.get('shadow_error_type') for p in pairs if p.get('shadow_error_type')],
      'costs_unchanged':True,'interventions':0,'all_replies_before_query_end':True,
      'attempts_before':before,'attempts_after':after,'actual_new_api_attempts':after-before,
      'live_decisions':[{'case':d['run_id'],'native_for_audit_only':d['native_would_choose_for_audit_only'],
         **{k:d['record'].get(k) for k in ('attempt','status','choice','confidence','latency_s')}} for d in decisions],
      'query_diagnostics':[{'case':r['case'],'to_result_with_shadow_s':r['to_result_s'],
        'observer_self_s_including_callback':r['observer_metrics']['observer_nanos']/1e9} for r in rows],
      'not_established':'No changed-state counterfactual timing or Jev-directed speedup. No complete executed-stage history or atomic in-flight task snapshot.'}
    (out/'summary.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2),flush=True)
    if handler_errors or report['callback_errors']:raise RuntimeError('shadow_callback_validation_failed')
    (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','queries':len(rows),'new_api_attempts':after-before,'all_correct':True,'interventions':0}))

if __name__=='__main__':main()
