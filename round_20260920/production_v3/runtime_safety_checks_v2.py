"""Loopback-only deadline/fallback tests plus guarded-observer Spark smoke checks.
No remote HTTP requests, secrets, paid API calls, or ledger modifications.
"""
from pathlib import Path
import asyncio, contextlib, hashlib, json, sys, time
import httpx
from pyspark.sql import SparkSession
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
WS=ROUND.parent
sys.path.insert(0,str(ROUND))
from baseline import append, expected, setup, run_one
from safe_jev import Budget
STATE='synthetic-snapshot-1842'

async def guarded_reply(client,url,native):
    assert native in ('CURRENT','PROPOSED') and url.startswith('http://127.0.0.1:')
    start=time.perf_counter();record={'native':native,'effective':native,'status':'fallback'}
    try:
        async with asyncio.timeout(0.15):
            async with client.stream('GET',url) as response:
                if response.status_code!=200:raise ValueError('unexpected_status')
                buf=bytearray()
                async for chunk in response.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf)>4096:raise ValueError('response_bound')
            obj=json.loads(buf)
            if obj.get('snapshot')!=STATE:raise ValueError('stale_state')
            choice=obj.get('choice')
            if choice not in ('CURRENT','PROPOSED','ABSTAIN'):raise ValueError('invalid_choice')
            record.update(effective=native if choice=='ABSTAIN' else choice,
                          status='abstained' if choice=='ABSTAIN' else 'accepted')
    except Exception as e:record['error_type']=type(e).__name__
    record['wall_s']=time.perf_counter()-start
    return record

async def network_fault_checks():
    tasks=set()
    async def handle(reader,writer):
        task=asyncio.current_task();tasks.add(task)
        try:
            head=await reader.readuntil(b'\r\n\r\n')
            mode=head.split(b'\r\n',1)[0].split()[1].decode().strip('/')
            if mode=='stall':await asyncio.sleep(1)
            obj={'snapshot':STATE,'choice':'PROPOSED'}
            if mode=='abstain':obj['choice']='ABSTAIN'
            if mode=='invalid':obj['choice']='INVENTED'
            if mode=='stale':obj['snapshot']='another-snapshot'
            body=json.dumps(obj).encode()
            if mode=='oversized':body=b'x'*5000
            if mode=='trickle':body=b' '*100
            status=b'500 Error' if mode=='http500' else b'200 OK'
            writer.write(b'HTTP/1.1 '+status+b'\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n')
            await writer.drain()
            if mode=='trickle':
                for value in body:
                    writer.write(bytes([value]));await writer.drain();await asyncio.sleep(.02)
            else:writer.write(body);await writer.drain()
        except (ConnectionError,asyncio.IncompleteReadError,asyncio.LimitOverrunError):pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):await writer.wait_closed()
            tasks.discard(task)
    server=await asyncio.start_server(handle,'127.0.0.1',0,limit=8192)
    port=server.sockets[0].getsockname()[1];records=[]
    try:
        transport=httpx.AsyncHTTPTransport(retries=0)
        async with httpx.AsyncClient(transport=transport,trust_env=False,follow_redirects=False,
            timeout=httpx.Timeout(.07,connect=.1)) as client:
            for native in ('CURRENT','PROPOSED'):
                for mode in ('success','abstain','invalid','stale','http500','oversized','stall','trickle'):
                    r=await guarded_reply(client,f'http://127.0.0.1:{port}/{mode}',native);r['mode']=mode
                    assert r['effective']==('PROPOSED' if mode=='success' else native)
                    if mode=='trickle':
                        assert r.get('error_type')=='TimeoutError' and .12<=r['wall_s']<.6
                    if mode not in ('success','abstain'):assert r['status']=='fallback'
                    records.append(r)
    finally:
        server.close();await server.wait_closed()
        remaining=list(tasks)
        for task in remaining:task.cancel()
        if remaining:await asyncio.gather(*remaining,return_exceptions=True)
    return records

def spark_checks(out):
    jar=HERE/'observer_safety_v1'/'guarded-observer.jar';assert jar.is_file()
    trace=out/'boundary_pairs.jsonl'
    cls='org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV6'
    spark=(SparkSession.builder.master('local[2]').appName('GuardedObserverVerification')
        .config('spark.ui.enabled','false').config('spark.driver.memory','1g')
        .config('spark.driver.bindAddress','127.0.0.1').config('spark.driver.host','127.0.0.1')
        .config('spark.sql.adaptive.enabled','true').config('spark.sql.shuffle.partitions','8')
        .config('spark.driver.extraClassPath',str(jar)+':'+str(HERE/'runtime_fault_helper'/'fault-helper.jar')).config('spark.jars',str(jar)+','+str(HERE/'runtime_fault_helper'/'fault-helper.jar'))
        .config('spark.research.observer.path',str(trace)).config('spark.sql.adaptive.customCostEvaluatorClass',cls)
        .config('spark.sql.warehouse.dir',str(HERE/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    c=dict(name='guarded_runtime_smoke',n=1200000,m=2000000,keep=100,skew=False,seed=19)
    want=expected(c);rows=[]
    try:
        setup(spark,c);spark.conf.set('spark.research.run_id','guard_v6_control')
        r=run_one(spark,c,'NATIVE',want);r['mode']='guarded_pass_through';rows.append(r)
        append(out/'spark_raw.jsonl',r);assert r['status']=='succeeded'
        obj=spark._jvm.org.apache.spark.sql.execution.adaptive.NativeBoundaryObserverV6
        first=json.loads(obj.metrics());assert first['pairs']>0 and first['errors']==0 and first['unmatched']==0
        entries=[json.loads(x) for x in trace.read_text().splitlines()]
        assert all(p['cost_returned_unchanged'] and not p['intervention'] for p in entries)
        # Saturate only the observer's test trace counter, NOT any request budget.
        spark._jvm.org.apache.spark.sql.execution.adaptive.ObserverFaultInjector.saturateAuditCounterForTest()
        spark.conf.set('spark.research.run_id','guard_v6_audit_full')
        r=run_one(spark,c,'NATIVE',want);r['mode']='audit_capacity_saturated';rows.append(r)
        append(out/'spark_raw.jsonl',r);assert r['status']=='succeeded'
        second=json.loads(obj.metrics());assert second['dropped']>first['dropped']
        assert second['errors']==0 and second['unmatched']==0
        assert len(trace.read_text().splitlines())==len(entries)
        return {'queries':2,'all_correct':True,'normal_metrics':first,'fault_metrics':second,
          'trace_counter_artificially_saturated':True,'new_jev_attempts':0,'runtime_interventions':0,
          'caveat':'Pass-through Spark checks plus audit-saturation fault. Counterfactual benefit and complete prefix coverage are not tested.'}
    finally:spark.stop()

def main():
    out=HERE/'runtime_safety_v2';out.mkdir(exist_ok=False);before=Budget().count()
    records=asyncio.run(network_fault_checks())
    (out/'loopback_faults.json').write_text(json.dumps(records,indent=2))
    smoke=spark_checks(out)
    summary={'status':'succeeded','loopback_checks_passed':len(records),
      'trickle_deadline_observations':[r for r in records if r['mode']=='trickle'],
      'fallback_tracks_native_current_or_proposed':True,'spark':smoke,
      'remote_api_requests':0,'budget_before':before,'budget_after':Budget().count(),
      'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
      'deadline_scope':'Async HTTP deadline under local fault injection; not a hard-real-time OS guarantee or deployed Jev client integration.'}
    (out/'COMPLETED.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
if __name__=='__main__':main()
