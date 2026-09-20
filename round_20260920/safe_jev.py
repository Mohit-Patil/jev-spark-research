"""Bounded direct HTTP adapter. No SDK retries, no secret output.
Ledger is workspace-wide and durable across batches/restarts. A reservation is
charged before network I/O, including failed or interrupted requests.
"""
from pathlib import Path
import fcntl, hashlib, json, math, os, re, shlex, stat, time
import httpx

ROOT=Path(__file__).resolve().parent
WORKSPACE=ROOT.parent
LEDGER=WORKSPACE/'.jev_initial_round_attempts.jsonl'
MODEL='jev-1.13.0'
ENDPOINT='https://api.typesafe.ai/v1/systemone'
LIMIT=50
MAX_REQUEST_BYTES=24576
MAX_RESPONSE_BYTES=65536

class BudgetError(RuntimeError): pass
class SecretError(RuntimeError): pass

class Budget:
    def __init__(self,path=LEDGER): self.path=Path(path)
    def reserve(self,payload_size,request_hash,purpose):
        if not 0 < payload_size <= MAX_REQUEST_BYTES: raise BudgetError('request_size_bound')
        if not re.fullmatch('[a-zA-Z0-9_-]{1,80}',purpose): raise BudgetError('invalid_purpose')
        flags=os.O_RDWR|os.O_CREAT|os.O_APPEND|getattr(os,'O_NOFOLLOW',0)
        fd=os.open(self.path,flags,0o600)
        with os.fdopen(fd,'r+',encoding='utf-8') as f:
            fcntl.flock(f.fileno(),fcntl.LOCK_EX)
            s=os.fstat(f.fileno())
            if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid(): raise BudgetError('ledger_owner_or_type')
            f.seek(0); text=f.read(1048577)
            if len(text)>1048576 or (text and not text.endswith('\n')): raise BudgetError('damaged_ledger')
            try: entries=[json.loads(x) for x in text.splitlines()]
            except Exception: raise BudgetError('damaged_ledger') from None
            if entries and entries[0] != {'kind':'budget','limit':50,'scope':'initial_research_round'}:
                raise BudgetError('unexpected_ledger_header')
            attempts=entries[1:] if entries else []
            if any(e.get('kind')!='attempt' or e.get('attempt')!=i+1 for i,e in enumerate(attempts)):
                raise BudgetError('damaged_attempt_sequence')
            if len(attempts)>=LIMIT: raise BudgetError('budget_exhausted')
            f.seek(0,2)
            if not entries: f.write(json.dumps({'kind':'budget','limit':50,'scope':'initial_research_round'})+'\n')
            n=len(attempts)+1
            f.write(json.dumps({'kind':'attempt','attempt':n,'reserved_unix':time.time(),'request_bytes':payload_size,'request_sha256':request_hash,'purpose':purpose})+'\n')
            f.flush(); os.fsync(f.fileno())
            return n
    def count(self):
        if not self.path.exists(): return 0
        fd=os.open(self.path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        with os.fdopen(fd) as f:
            fcntl.flock(f.fileno(),fcntl.LOCK_SH)
            entries=[json.loads(x) for x in f]
            if not entries or entries[0] != {'kind':'budget','limit':50,'scope':'initial_research_round'}: raise BudgetError('damaged_ledger')
            if any(e.get('attempt')!=i+1 for i,e in enumerate(entries[1:])): raise BudgetError('damaged_ledger')
            return len(entries)-1

def parse_key(text):
    found=[]
    for line in text.splitlines():
        m=re.fullmatch(r'\s*(?:export\s+)?TYPESAFE_API_KEY\s*=\s*(.*?)\s*',line)
        if m:
            try: parts=shlex.split(m.group(1),comments=True,posix=True)
            except Exception: raise SecretError('invalid_secret_syntax') from None
            if len(parts)!=1: raise SecretError('invalid_secret_syntax')
            value=parts[0]
            if not 8<=len(value)<=4096 or any(ord(c)<33 or ord(c)>126 for c in value): raise SecretError('invalid_secret_value')
            found.append(value)
    if len(found)!=1: raise SecretError('missing_or_duplicate_key_variable')
    return found[0]

def load_key():
    allowed=[WORKSPACE/'.env',ROOT/'.env',Path.home()/'jev-mac-native'/'.env']
    for p in allowed:
        if not p.exists(): continue
        if p.is_symlink(): raise SecretError('secret_symlink_rejected')
        fd=os.open(p,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        with os.fdopen(fd) as f:
            s=os.fstat(f.fileno())
            if not stat.S_ISREG(s.st_mode) or s.st_uid!=os.getuid() or s.st_size>65536: raise SecretError('secret_owner_type_or_size')
            key=parse_key(f.read(65537))
        # Existing bridge secret permissions are reported, never changed here.
        return key,{'source':str(p),'mode':oct(stat.S_IMODE(s.st_mode)),'group_or_other_readable':bool(s.st_mode & 0o077)}
    raise SecretError('configured_secret_not_found')

def validate_response(obj,question_id,options):
    if obj.get('model')!=MODEL: raise ValueError('model_version_mismatch')
    a=obj['answers'][question_id]
    if a.get('type')!='choice' or a.get('choice') not in options: raise ValueError('invalid_choice')
    p=a['probabilities']; confidence=a['confidence']
    if set(p)!=set(options): raise ValueError('probability_options_mismatch')
    nums=list(p.values())+[confidence]
    if any(isinstance(v,bool) or not isinstance(v,(float,int)) or not math.isfinite(v) or not 0<=v<=1 for v in nums): raise ValueError('invalid_probability')
    if abs(sum(p.values())-1)>0.02: raise ValueError('probability_sum')
    usage=obj.get('usage',{})
    if any(type(usage.get(k)) is not int or usage[k]<0 for k in ('input_tokens','output_tokens')): raise ValueError('invalid_usage')
    return {'model':MODEL,'choice':a['choice'],'confidence':confidence,'probabilities':p,
            'usage':{k:usage[k] for k in ('input_tokens','output_tokens')}}

def choose(state,criteria,purpose,instructions='Choose the candidate expected to minimize total Spark query latency on this CPU-only local[2] machine. AQE remains on. Use only the supplied evidence. Prefer ABSTAIN when there is insufficient evidence to improve on native Spark. Do not invent plans or observations.'):
    payload={'model':MODEL,'state':state,'questions':{'plan':{'type':'choice','instructions':instructions,'criteria':criteria}}}
    data=json.dumps(payload,separators=(',',':'),allow_nan=False).encode()
    if len(data)>MAX_REQUEST_BYTES: raise BudgetError('request_size_bound')
    key,secret_metadata=load_key()
    attempt=Budget().reserve(len(data),hashlib.sha256(data).hexdigest(),purpose)
    record={'attempt':attempt,'purpose':purpose,'request_bytes':len(data),'request_sha256':hashlib.sha256(data).hexdigest(),'secret_permissions_warning':secret_metadata['group_or_other_readable']}
    start=time.perf_counter()
    try:
        transport=httpx.HTTPTransport(retries=0)
        with httpx.Client(transport=transport,timeout=httpx.Timeout(15.0,connect=5.0),follow_redirects=False,trust_env=False) as client:
            with client.stream('POST',ENDPOINT,content=data,headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'}) as response:
                record['http_status']=response.status_code
                if response.status_code != 200: raise RuntimeError('http_failure')
                chunks=[]; length=0
                for chunk in response.iter_bytes():
                    length+=len(chunk)
                    if length>MAX_RESPONSE_BYTES: raise RuntimeError('response_size_bound')
                    chunks.append(chunk)
                obj=json.loads(b''.join(chunks))
                record.update(validate_response(obj,'plan',criteria))
                record['status']='succeeded'
    except Exception as e:
        # Never include exception messages or raw HTTP bodies/headers.
        record.update(status='failed',error_type=type(e).__name__,choice='ABSTAIN')
    finally:
        key=None
    record['latency_s']=time.perf_counter()-start
    record['completed_unix']=time.time()
    with (ROOT/'jev_calls.jsonl').open('a') as f:
        f.write(json.dumps(record)+'\n'); f.flush(); os.fsync(f.fileno())
    return record,payload

if __name__=='__main__':
    try:
        rec,payload=choose('Connectivity test only: select NATIVE. This is not a Spark performance prediction.',{'NATIVE':'Native Spark','ABSTAIN':'Abstain'},'connectivity_probe',instructions='Select NATIVE for this connectivity test.')
        (ROOT/'jev_probe.json').write_text(json.dumps(rec,indent=2))
        print(json.dumps({'probe':rec,'attempts_used':Budget().count(),'limit':LIMIT},indent=2))
    except Exception as e:
        print(json.dumps({'status':'blocked','error_type':type(e).__name__,'attempts_used':Budget().count()})); raise SystemExit(1)
