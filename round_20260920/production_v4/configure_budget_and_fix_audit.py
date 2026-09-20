"""Apply explicit user budget authorization and prepare corrected OFFLINE audit.
No request reservation, secret-file access, Spark execution or network calls.
Original ledger bytes and all frozen experiment implementations stay unchanged.
"""
from pathlib import Path
import fcntl,hashlib,json,os,stat,sys,time
R=Path(__file__).resolve().parent
from authorized_budget import (AuthorizedBudget,LEDGER,POLICY,MAX_AUTHORIZED,AUTHORIZATION_TIME,
    _entries,_checked_file,MAX_LEDGER_BYTES)
fd=os.open(LEDGER,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
with os.fdopen(fd,'rb') as f:
    fcntl.flock(f.fileno(),fcntl.LOCK_EX)
    _checked_file(f.fileno(),MAX_LEDGER_BYTES);raw=f.read(MAX_LEDGER_BYTES+1);attempts=_entries(raw)
    if not POLICY.exists():
        authorization={'kind':'user_authorized_cumulative_budget_v2','limit':MAX_AUTHORIZED,
            'authorized_at_utc':AUTHORIZATION_TIME,'configured_unix':time.time(),
            'preserved_attempts':len(attempts),'preserved_prefix_bytes':len(raw),
            'preserved_prefix_sha256':hashlib.sha256(raw).hexdigest(),
            'scope':'cumulative across all research batches; supersedes initial 50, does not reset usage'}
        pfd=os.open(POLICY,os.O_CREAT|os.O_EXCL|os.O_WRONLY|getattr(os,'O_NOFOLLOW',0),0o600)
        with os.fdopen(pfd,'w') as p:
            json.dump(authorization,p,indent=2);p.flush();os.fsync(p.fileno())
status=AuthorizedBudget().status()
assert status['attempts_used']==len(attempts)
assert LEDGER.read_bytes()==raw
(R/'BUDGET_STATUS_AT_AUTHORIZATION.json').write_text(json.dumps(status,indent=2))
(R/'BUDGET_AUTHORIZATION.md').write_text('''# Cumulative API budget authorization

At 2026-09-20T17:21:22Z the user explicitly increased the cumulative TypeSafe Jev request allowance from 50 to 10,000 attempts. This is not 10,000 additional attempts. Existing attempts, failures, and interrupted reservations remain charged.

`authorized_budget.py` appends under the same exclusive file lock to the original workspace ledger. It validates a private authorization record and a cryptographic digest of every pre-existing ledger byte before counting/reserving. It fails closed if the ledger disappears, truncates, changes its historical prefix, has broken sequence numbers, exceeds request size bounds, or reaches the cap. No implicit resets or retries are performed. A separate per-batch cap keeps experiments bounded.

The original frozen `safe_jev.py` remains unchanged for reproducibility. Its legacy writer still refuses requests at 50; new runners must explicitly pass `AuthorizedBudget` or `BatchBudget` to `BoundedClient`. Reading the old ledger is compatible because its original header and all attempt records are preserved. The historical header's 50 describes the first authorization, not the active amended cap.

Private accounting/authorization files are kept outside the tracked artifact allowlist. No API key was needed for this update. The 600-second native bridge job limit and all other safety constraints are unchanged.
''')
s=(R/'audit_completed_results.py').read_text()
a="from safe_jev import Budget";b="from authorized_budget import AuthorizedBudget as Budget\nfrom runtime_advisor_v5 import payload as rebuild_request"
assert s.count(a)==1;s=s.replace(a,b)
a="        body=json.dumps(p,separators=(',',':'),allow_nan=False).encode()"
b="        rebuilt=rebuild_request(d['snapshot'])\n        check(d['run_id']+'_rebuilt_semantics_match_saved_payload',rebuilt==p)\n        body=json.dumps(rebuilt,separators=(',',':'),allow_nan=False).encode()\n        check(d['run_id']+'_serialized_request_size_matches',len(body)==rec['request_bytes'])"
assert s.count(a)==1;s=s.replace(a,b)
a="'global_limit':50,'global_remaining':50-after"
b="'global_limit':Budget().status()['limit'],'global_remaining':Budget().status()['limit']-after"
assert s.count(a)==1;s=s.replace(a,b)
s=s.replace("R/'AUDIT_COMPLETED.json'","R/'AUDIT_COMPLETED_V2.json'").replace("R/'AUDIT_CHECKS.json'","R/'AUDIT_CHECKS_V2.json'")
compile(s,'audit_completed_results_v2.py','exec')
p=R/'audit_completed_results_v2.py'
if p.exists():assert p.read_text()==s
else:
    with p.open('x') as f:f.write(s)
print(json.dumps({'budget':status,'ledger_unchanged':True,'new_api_attempts':0,
    'audit_fix':'Reconstruct original request insertion order with frozen builder; verify semantic equality, size and SHA-256, not sorted log serialization.',
    'audit_prepared_not_executed':True},indent=2))
