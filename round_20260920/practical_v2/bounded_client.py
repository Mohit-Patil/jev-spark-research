"""Persistent HTTP transport sharing the original durable 50-attempt ledger.
No retries, redirects, credential logging, SDK dependency or environment inheritance.
Trace storage contains event NAMES and relative times only, never callback info,
request headers, response headers or exception messages.
"""
from pathlib import Path
import hashlib,json,math,re,sys,time
import httpx
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
sys.path.insert(0,str(ROUND))
from safe_jev import Budget,BudgetError,ENDPOINT,MODEL,MAX_REQUEST_BYTES,MAX_RESPONSE_BYTES,load_key,validate_response
from baseline import append

ALLOWED={'CURRENT','PROPOSED','ABSTAIN','NATIVE','BROADCAST','MERGE','SHUFFLE_HASH'}

def encode(payload):
    if set(payload)!= {'model','state','questions'} or payload['model']!=MODEL: raise ValueError('payload_shape')
    if set(payload['questions'])!={'plan'}: raise ValueError('question_shape')
    q=payload['questions']['plan']
    if q.get('type')!='choice' or not isinstance(q.get('criteria'),dict): raise ValueError('choice_shape')
    if not 2<=len(q['criteria'])<=5 or not set(q['criteria'])<=ALLOWED: raise ValueError('choice_options')
    if 'ABSTAIN' not in q['criteria']: raise ValueError('abstention_required')
    body=json.dumps(payload,separators=(',',':'),allow_nan=False).encode()
    if not 0<len(body)<=MAX_REQUEST_BYTES: raise BudgetError('request_size_bound')
    return body

def resolve_advice(record,native,minimum_confidence=0.8,maximum_latency_s=0.5):
    if native not in ('CURRENT','PROPOSED'): raise ValueError('native_choice_required')
    choice=record.get('choice');confidence=record.get('confidence');latency=record.get('latency_s')
    if (record.get('status')!='succeeded' or choice not in ('CURRENT','PROPOSED') or
        type(confidence) not in (float,int) or not math.isfinite(confidence) or confidence<minimum_confidence or
        type(latency) not in (float,int) or not math.isfinite(latency) or latency>maximum_latency_s):
        return native
    return choice

class BoundedClient:
    def __init__(self,*,transport=None,budget=None,key_loader=load_key,log_path=ROUND/'jev_calls.jsonl',late_after_s=12.0):
        limits=httpx.Limits(max_connections=1,max_keepalive_connections=1,keepalive_expiry=60.0)
        self.client=httpx.Client(transport=transport or httpx.HTTPTransport(retries=0,limits=limits),
            timeout=httpx.Timeout(8.0,connect=4.0,write=4.0,pool=2.0),follow_redirects=False,trust_env=False)
        self.budget=budget if budget is not None else Budget();self.key_loader=key_loader
        self.log_path=log_path;self.late_after_s=late_after_s
    def close(self): self.client.close()
    def request(self,payload,purpose):
        wall_start=time.perf_counter();body=encode(payload)
        if not re.fullmatch('[A-Za-z0-9_-]{1,80}',purpose): raise ValueError('purpose')
        key,metadata=self.key_loader()
        if key.encode() in body: raise ValueError('secret_in_payload_rejected')
        digest=hashlib.sha256(body).hexdigest()
        attempt=self.budget.reserve(len(body),digest,purpose)
        record={'attempt':attempt,'purpose':purpose,'request_bytes':len(body),'request_sha256':digest,
          'secret_permissions_warning':bool(metadata.get('group_or_other_readable',False)),'transport':'bounded_reusable_http1',
          'late_response_gate_s':self.late_after_s}
        start=time.perf_counter();events=[]
        def trace(name,info):
            # Deliberately never examine or serialize info, which can include credentials.
            if len(events)<64 and re.fullmatch('[a-z0-9_.]+',name):
                events.append({'event':name,'elapsed_s':time.perf_counter()-start})
        try:
            with self.client.stream('POST',ENDPOINT,content=body,
              headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},extensions={'trace':trace}) as response:
                record['http_status']=response.status_code;record['http_version']=response.http_version
                record['headers_available_s']=time.perf_counter()-start
                if response.status_code!=200: raise RuntimeError('http_failure')
                chunks=[];size=0
                for chunk in response.iter_bytes():
                    size+=len(chunk)
                    if size>MAX_RESPONSE_BYTES: raise RuntimeError('response_size_bound')
                    if time.perf_counter()-start>self.late_after_s: raise TimeoutError('late_response')
                    chunks.append(chunk)
                record.update(validate_response(json.loads(b''.join(chunks)),'plan',payload['questions']['plan']['criteria']))
                if time.perf_counter()-start>self.late_after_s: raise TimeoutError('late_response')
                record['status']='succeeded'
        except Exception as e:
            record.update(status='failed',choice='ABSTAIN',error_type=type(e).__name__)
        finally:
            key=None
        record.update(latency_s=time.perf_counter()-start,completed_unix=time.time(),trace_events=events,
          adapter_wall_s=time.perf_counter()-wall_start)
        if self.log_path is not None: append(Path(self.log_path),record)
        return record
