"""Fixed-request transport experiment: 10 total API attempts, original shared cap.
Two warmups, then four randomized FRESH/POOLED blocks. Reuses a previously
captured synthetic runtime request solely to study latency, not prediction quality.
"""
from pathlib import Path
import hashlib,json,random,statistics,time
import httpx
from bounded_client import BoundedClient,Budget,MODEL,encode,ROUND
from baseline import append
HERE=Path(__file__).resolve().parent

def inspect_public_docs(out):
    report=[]
    with httpx.Client(timeout=8.0,trust_env=False,follow_redirects=False) as client:
        for name in ('api','models'):
            url='https://docs.typesafe.ai/'+name
            rec={'url':url}
            try:
                with client.stream('GET',url) as response:
                    rec['status']=response.status_code;chunks=[];size=0
                    for part in response.iter_bytes():
                        size+=len(part)
                        if size>2000000:raise ValueError('public_doc_size_bound')
                        chunks.append(part)
                data=b''.join(chunks);text=data.decode('utf-8','replace')
                rec.update(bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),
                  endpoint_present='api.typesafe.ai/v1/systemone' in text,
                  pinned_model_present=MODEL in text)
                if rec['status']==200:(out/('official_'+name+'.html')).write_bytes(data)
            except Exception as e:rec['error_type']=type(e).__name__
            report.append(rec)
    (out/'official_document_checks.json').write_text(json.dumps(report,indent=2))
    return report

def main():
    before=Budget().count()
    if before+10>50:raise RuntimeError('insufficient_shared_budget_for_fixed_probe')
    out=HERE/'transport_v1';out.mkdir(exist_ok=False)
    previous=[json.loads(x) for x in (ROUND/'aqe_observer/live_shadow_v2/decisions.jsonl').read_text().splitlines()]
    saved=next(d for d in previous if d['run_id']=='live_uniform_medium' and d['view']=='STATS')
    payload=saved['payload'];body=encode(payload)
    for forbidden in ('native_would_choose','stock_cost','oracle_remaining_s'):
        assert forbidden not in json.dumps(payload['state'])
    rng=random.Random(6071);schedule=[]
    for phase,n in [('warmup',1),('measured',4)]:
        for block in range(n):
            modes=['FRESH','POOLED'];rng.shuffle(modes)
            schedule.extend({'phase':phase,'block':block,'mode':mode} for mode in modes)
    manifest={'kind':'fixed_payload_transport_latency_only','api_attempts_before':before,'maximum_new_attempts':10,
      'seed':6071,'schedule':schedule,'payload_sha256':hashlib.sha256(body).hexdigest(),
      'payload':payload,'model':MODEL,'shared_budget_limit':50,'requests_are_not_optimization_evidence':True,
      'caveats':'four measured requests per arm; no isolated server inference timing; server-side cache behavior unknown; existing-state replay only',
      'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__),HERE/'bounded_client.py')}}
    (out/'frozen_manifest.json').write_text(json.dumps(manifest,indent=2))
    docs=inspect_public_docs(out)
    rows=[];shared=BoundedClient();started=time.perf_counter()
    try:
        for i,item in enumerate(schedule):
            if time.perf_counter()-started>230:raise RuntimeError('bounded_batch_wall_budget')
            begin=time.perf_counter();client=BoundedClient() if item['mode']=='FRESH' else shared
            creation=time.perf_counter()-begin
            try:r=client.request(payload,'transport_'+str(i)+'_'+item['mode'].lower())
            finally:
                if item['mode']=='FRESH':client.close()
            r.update(item,client_creation_s=creation,sample_wall_s=time.perf_counter()-begin)
            names=[e['event'] for e in r['trace_events']]
            r['tcp_connects']=names.count('connection.connect_tcp.started');r['tls_handshakes']=names.count('connection.start_tls.started')
            append(out/'raw.jsonl',r);rows.append(r)
            print(json.dumps({k:r.get(k) for k in ('attempt','phase','mode','status','latency_s','sample_wall_s','tcp_connects','tls_handshakes')}),flush=True)
            if r['status']!='succeeded':raise RuntimeError('failed_probe_preserved_no_retry')
    finally:shared.close()
    result={'status':'succeeded','actual_new_attempts':len(rows),'attempts_before':before,'attempts_after':Budget().count(),
      'official_doc_checks':docs,'arms':{},'scope':'transport latency only, repeated existing state, NOT optimization accuracy or AQE execution'}
    for mode in ('FRESH','POOLED'):
        rs=[r for r in rows if r['phase']=='measured' and r['mode']==mode]
        result['arms'][mode]={'n':len(rs),'median_roundtrip_s':statistics.median(r['latency_s'] for r in rs),
          'min_roundtrip_s':min(r['latency_s'] for r in rs),'max_roundtrip_s':max(r['latency_s'] for r in rs),
          'median_sample_wall_s':statistics.median(r['sample_wall_s'] for r in rs),
          'tcp_connects':sum(r['tcp_connects'] for r in rs),'tls_handshakes':sum(r['tls_handshakes'] for r in rs)}
    paired=[]
    for block in range(4):
        pair={r['mode']:r for r in rows if r['phase']=='measured' and r['block']==block}
        paired.append(pair['FRESH']['sample_wall_s']-pair['POOLED']['sample_wall_s'])
    result['paired_median_wall_saving_s']=statistics.median(paired)
    assert Budget().count()==before+10
    (out/'summary.json').write_text(json.dumps(result,indent=2))
    (out/'COMPLETED.json').write_text(json.dumps({'status':'succeeded','attempts':10,'attempts_after':Budget().count()}))
    print(json.dumps(result,indent=2),flush=True)

if __name__=='__main__':main()
