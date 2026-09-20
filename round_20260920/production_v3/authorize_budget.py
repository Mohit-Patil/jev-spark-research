"""Record the user's increased cumulative allowance without editing prior ledger bytes."""
from pathlib import Path
import fcntl, hashlib, json, os, stat, time
HERE=Path(__file__).resolve().parent
WS=HERE.parent.parent
LEDGER=WS/'.jev_initial_round_attempts.jsonl'
assert WS.resolve()==(Path.home()/'JevResearch').resolve()
fd=os.open(LEDGER,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
with os.fdopen(fd,'rb') as f:
    fcntl.flock(f.fileno(),fcntl.LOCK_SH)
    s=os.fstat(f.fileno())
    assert stat.S_ISREG(s.st_mode) and s.st_uid==os.getuid()
    data=f.read(8388609)
    assert len(data)<=8388608 and data.endswith(b'\n')
    entries=[json.loads(x) for x in data.splitlines()]
    assert entries[0]=={'kind':'budget','limit':50,'scope':'initial_research_round'}
    assert all(e.get('kind')=='attempt' and e.get('attempt')==i for i,e in enumerate(entries[1:],1))
    authorization={'schema_version':1,'kind':'user_authorized_cumulative_budget_increase',
      'authorized_at_user_message_utc':'2026-09-20T17:21:23Z',
      'authorized_max_attempts':10000,'previous_authorized_max_attempts':50,
      'ledger_relative_path':'.jev_initial_round_attempts.jsonl',
      'attempts_at_authorization':len(entries)-1,'prior_ledger_bytes':len(data),
      'prior_ledger_sha256':hashlib.sha256(data).hexdigest(),
      'registered_unix':time.time(),
      'meaning':'10000 TOTAL including all prior, failed, cancelled and successful reservations. No reset. No additional paid APIs authorized.',
      'legacy_header_retained':True,'new_network_requests':0}
    with (HERE/'BUDGET_AUTHORIZATION.json').open('x') as out:
        json.dump(authorization,out,indent=2);out.flush();os.fsync(out.fileno())
print(json.dumps(authorization,indent=2))
