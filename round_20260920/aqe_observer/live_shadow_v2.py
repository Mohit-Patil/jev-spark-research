"""Bounded LIVE shadow Jev calls while Spark is inside its actual AQE comparison.
A loopback callback transmits only synthetic snapshots. Costs are never altered.
At most two views on the first changed pair per query: six new attempts total.
"""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
import hashlib,json,secrets,statistics,sys,threading,time
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from baseline import append,expected,run_one,setup
from safe_jev import Budget,choose
from observer_check_v2 import CLASS,make_session,metrics
from runtime_features import CRITERIA,INSTRUCTIONS,project
CASES=[dict(name='live_uniform_small',n=1200000,m=2000000,keep=10,skew=False,seed=503),
       dict(name='live_uniform_medium',n=1200000,m=2000000,keep=100,skew=False,seed=601),
       dict(name='live_skew',n=1200000,m=2000000,keep=500,skew=True,seed=733)]

def main():
    out=ROOT/'live_shadow_v2';out.mkdir(exist_ok=False);before=Budget().count()
    if 50-before<6:raise RuntimeError('insufficient_remaining_round_budget')
    tags={c['name'] for c in CASES};seen=set();decisions=[];handler_errors=[]
    callback_path='/shadow/'+secrets.token_hex(16)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            try:
                size=int(self.headers.get('Content-Length','0'))
                if self.path!=callback_path or not 1<=size<=131072:
                    self.send_error(400);return
                pair=json.loads(self.rfile.read(size));tag=pair.get('run_id')
                if tag not in tags:raise ValueError('unknown_run_tag')
                if tag not in seen and not pair.get('plans_equal',True):
                    seen.add(tag);received=time.time()
                    append(out/'callback_snapshots.jsonl',pair)
                    # The native decision/stock costs in the audit record are NOT supplied to Jev.
                    for view in ('PLANS','STATS'):
                        state=project(pair,view)
                        started=time.time();r,payload=choose(state,CRITERIA,'aqe2_'+tag+'_'+view.lower(),instructions=INSTRUCTIONS)
                        rec={'run_id':tag,'view':view,'received_unix':received,'started_unix':started,
                             'record':r,'payload':payload,'native_would_choose_for_later_audit_only':pair['native_would_choose']}
                        decisions.append(rec);append(out/'decisions.jsonl',rec)
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}')
            except Exception as e:
                handler_errors.append({'error_type':type(e).__name__})
                try:self.send_error(500)
                except Exception:pass
    server=HTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.05},name='bounded-local-shadow-callback')
    worker.start();spark=None;rows=[]
    manifest={'kind':'real_AQE_boundary_live_shadow_NOT_intervention_or_speedup_evaluation','cases':CASES,'max_new_attempts':6,'budget_before':before,
      'selection':'first structurally changed native current/proposed pair per query; both feature views use the same captured state',
      'client':'existing bounded adapter, fresh HTTP client each call; no retries','model':'jev-1.13.0',
      'observer_callback_timeout_s':20,'late_reply_policy':'native Spark continues on timeout; late advice is reported, never applied',
      'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),ROOT/'runtime_features.py',ROOT/'observer_check_v2.py',ROOT/'NativeBoundaryObserverV2.scala',ROOT.parent/'safe_jev.py',ROOT.parent/'baseline.py']}}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
    try:
        endpoint='http://127.0.0.1:'+str(server.server_port)+callback_path
        spark=make_session(out/'boundary_pairs.jsonl',endpoint)
        spark.conf.set('spark.sql.adaptive.customCostEvaluatorClass',CLASS)
        for c in CASES:
            setup(spark,c);want=expected(c)
            spark.conf.set('spark.research.run_id',c['name']);a=metrics(spark)
            started=time.time();r=run_one(spark,c,'NATIVE',want);finished=time.time();b=metrics(spark)
            r.update(started_unix=started,finished_unix=finished,observer_metrics={k:b[k]-a[k] for k in b})
            rows.append(r);append(out/'raw.jsonl',r)
            if r['status']!='succeeded':raise RuntimeError('query_failed_preserved')
        spark.stop();spark=None
    finally:
        if spark is not None:spark.stop()
        server.shutdown();server.server_close();worker.join(timeout=5)
    bytag={r['case']:r for r in rows}
    for d in decisions:
        d['reply_before_query_finished']=d['record']['completed_unix']<=bytag[d['run_id']]['finished_unix']
    (out/'verified_decisions.json').write_text(json.dumps(decisions,indent=2))
    after=Budget().count();assert 0<=after-before<=6
    pairs=[json.loads(x) for x in (out/'boundary_pairs.jsonl').read_text().splitlines()]
    for p in pairs:
        assert p['cost_returned_unchanged'] and not p['intervention']
        a=p['current']['stock_cost_value'];b=p['proposed']['stock_cost_value']
        assert p['native_would_choose']==('PROPOSED' if b<a or (b==a and not p['plans_equal']) else 'CURRENT')
    summary={'status':'succeeded','query_runs':len(rows),'all_results_correct':all(r['correct'] for r in rows),'boundary_pairs':len(pairs),
      'interventions':0,'new_api_attempts':after-before,'budget_used':after,'budget_remaining':50-after,
      'handler_errors':handler_errors,'all_replies_before_query_finished':all(d['reply_before_query_finished'] for d in decisions),
      'shadow_callback_errors':[p.get('shadow_error_type') for p in pairs if p.get('shadow_error_type')],
      'model_calls':[{'case':d['run_id'],'view':d['view'],'native_would_choose':d['native_would_choose_for_later_audit_only'],
       **{k:d['record'].get(k) for k in ('attempt','status','choice','confidence','latency_s','usage')},'reply_before_query_finished':d['reply_before_query_finished']} for d in decisions],
      'query_times_with_shadow_overhead':[{'case':r['case'],'to_result_s':r['to_result_s'],'observer_self_s_including_callback':r['observer_metrics']['observer_nanos']/1e9} for r in rows],
      'interpretation':'agreement with native is NOT accuracy; unexecuted alternative remaining runtimes are unknown; no counterfactual performance labels exist'}
    (out/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2),flush=True)
    if handler_errors or not decisions:raise RuntimeError('shadow_validation_incomplete')
    if not summary['all_replies_before_query_finished'] or summary['shadow_callback_errors']:raise RuntimeError('late_or_failed_shadow_callback')
    (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','queries':len(rows),'new_api_attempts':after-before,'interventions':0}))
if __name__=='__main__':main()
