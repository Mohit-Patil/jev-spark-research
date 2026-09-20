"""Explicit 2026-09-20T17:21:22Z user-authorized cumulative cap of 10,000.
Appends to the ORIGINAL workspace ledger under the SAME flock as the legacy
adapter. Never rewrites its historical header/attempts or resets usage.
Legacy frozen adapters remain unchanged and stop at their original cap.
No secret loading, network requests, or implicit authorization creation here.
"""
from pathlib import Path
import fcntl,hashlib,json,os,re,stat,sys,time
ROOT=Path(__file__).resolve().parent
WS=ROOT.parents[1]
sys.path.insert(0,str(ROOT.parent))
from safe_jev import BudgetError,MAX_REQUEST_BYTES
LEDGER=WS/'.jev_initial_round_attempts.jsonl'
POLICY=WS/'.jev_budget_authorization.json'
MAX_AUTHORIZED=10000
MAX_LEDGER_BYTES=8388608
HEADER={'kind':'budget','limit':50,'scope':'initial_research_round'}
AUTHORIZATION_TIME='2026-09-20T17:21:22Z'

def _checked_file(fd,maximum):
    s=os.fstat(fd)
    if not stat.S_ISREG(s.st_mode) or s.st_uid!=os.getuid() or s.st_size>maximum:
        raise BudgetError('file_owner_type_or_size')
    return s

def _read_policy(path):
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(fd,'rb') as f:
        _checked_file(f.fileno(),16384);b=f.read(16385)
    try:p=json.loads(b)
    except Exception:raise BudgetError('invalid_authorization') from None
    if (p.get('kind')!='user_authorized_cumulative_budget_v2' or
        p.get('authorized_at_utc')!=AUTHORIZATION_TIME or
        type(p.get('limit')) is not int or p['limit']!=MAX_AUTHORIZED or
        type(p.get('preserved_prefix_bytes')) is not int or p['preserved_prefix_bytes']<=0 or
        type(p.get('preserved_attempts')) is not int or not 0<=p['preserved_attempts']<=MAX_AUTHORIZED or
        not re.fullmatch('[a-f0-9]{64}',str(p.get('preserved_prefix_sha256','')))):
        raise BudgetError('invalid_authorization')
    return p

def _entries(raw):
    if not raw or len(raw)>MAX_LEDGER_BYTES or not raw.endswith(b'\n'):
        raise BudgetError('damaged_ledger')
    try:entries=[json.loads(x) for x in raw.splitlines()]
    except Exception:raise BudgetError('damaged_ledger') from None
    if entries[0]!=HEADER:raise BudgetError('unexpected_ledger_header')
    attempts=entries[1:]
    if any(e.get('kind')!='attempt' or type(e.get('attempt')) is not int or e['attempt']!=i+1 for i,e in enumerate(attempts)):
        raise BudgetError('damaged_attempt_sequence')
    if len(attempts)>MAX_AUTHORIZED:raise BudgetError('usage_exceeds_authorized_cap')
    return attempts

class AuthorizedBudget:
    def __init__(self,path=LEDGER,policy_path=POLICY):
        self.path=Path(path);self.policy_path=Path(policy_path)
    def _validated(self,f):
        _checked_file(f.fileno(),MAX_LEDGER_BYTES)
        p=_read_policy(self.policy_path)
        f.seek(0);raw=f.read(MAX_LEDGER_BYTES+1)
        attempts=_entries(raw)
        prefix=p['preserved_prefix_bytes']
        if (len(raw)<prefix or len(attempts)<p['preserved_attempts'] or
            hashlib.sha256(raw[:prefix]).hexdigest()!=p['preserved_prefix_sha256']):
            raise BudgetError('historical_usage_modified_or_reset')
        return p,attempts,raw
    def status(self):
        fd=os.open(self.path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        with os.fdopen(fd,'rb') as f:
            fcntl.flock(f.fileno(),fcntl.LOCK_SH)
            p,attempts,raw=self._validated(f)
            return {'limit':p['limit'],'attempts_used':len(attempts),'remaining':p['limit']-len(attempts),
                'authorized_at_utc':p['authorized_at_utc'],'historical_attempts_preserved':p['preserved_attempts'],
                'historical_ledger_prefix_unchanged':True}
    def count(self):return self.status()['attempts_used']
    def reserve(self,payload_size,request_hash,purpose):
        if type(payload_size) is not int or not 0<payload_size<=MAX_REQUEST_BYTES:raise BudgetError('request_size_bound')
        if not isinstance(request_hash,str) or not re.fullmatch('[a-f0-9]{64}',request_hash):raise BudgetError('invalid_request_hash')
        if not isinstance(purpose,str) or not re.fullmatch('[a-zA-Z0-9_-]{1,80}',purpose):raise BudgetError('invalid_purpose')
        # No O_CREAT: disappearance of an initialized ledger must fail closed.
        fd=os.open(self.path,os.O_RDWR|os.O_APPEND|getattr(os,'O_NOFOLLOW',0))
        with os.fdopen(fd,'r+b') as f:
            fcntl.flock(f.fileno(),fcntl.LOCK_EX)
            p,attempts,raw=self._validated(f)
            if len(attempts)>=p['limit']:raise BudgetError('budget_exhausted')
            n=len(attempts)+1
            entry={'kind':'attempt','attempt':n,'reserved_unix':time.time(),'request_bytes':payload_size,
                'request_sha256':request_hash,'purpose':purpose,'authorization_revision':AUTHORIZATION_TIME}
            line=(json.dumps(entry,separators=(',',':'))+'\n').encode()
            if len(raw)+len(line)>MAX_LEDGER_BYTES:raise BudgetError('ledger_size_bound')
            f.seek(0,2);f.write(line);f.flush();os.fsync(f.fileno())
            return n

class BatchBudget:
    """Per-batch reservations also count globally. No batch resets the allowance."""
    def __init__(self,maximum,global_budget=None):
        import threading
        if type(maximum) is not int or not 1<=maximum<=100:raise ValueError('batch_limit')
        self.global_budget=global_budget or AuthorizedBudget();self.maximum=maximum;self.used=0;self.lock=threading.Lock()
    def reserve(self,*args):
        with self.lock:
            if self.used>=self.maximum:raise BudgetError('batch_budget_exhausted')
            n=self.global_budget.reserve(*args);self.used+=1;return n
    def count(self):return self.global_budget.count()
