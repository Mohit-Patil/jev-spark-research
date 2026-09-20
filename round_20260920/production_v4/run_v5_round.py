"""One bounded attached job: compile revised hook, fault-test, then <=4 live calls."""
from pathlib import Path
import subprocess,sys,time,json
R=Path(__file__).resolve().parent;started=time.monotonic()
s=(R/'prepare_advisor_v5.py').read_text()
needle="p=R/'RuntimeCandidateAdviceExtension.scala'"
assert s.count(needle)==1
s=s.replace(needle,"s=s.replace('No network or secret access.','Loopback callback only; no remote endpoint or credential in the JVM.')\ns=s.replace('not a trained or Jev-based policy','with a separately gated experimental Jev policy')\n"+needle)
reviewed=R/'prepare_advisor_v5_reviewed.py'
with reviewed.open('x') as f:f.write(s)
steps=[(reviewed,120,[]),(R/'runtime_advisor_v5.py',430,[]),
       (R/'checkpoint_scoped.py',160,['--message','research: validate bounded runtime advice and preserve native fallbacks'])]
for p,cap,args in steps:
    remaining=580-(time.monotonic()-started)
    if remaining<20:raise RuntimeError('round_time_budget')
    x=subprocess.run([sys.executable,str(p),*args],cwd=R,timeout=min(cap,remaining))
    print(json.dumps({'step':p.name,'returncode':x.returncode}),flush=True)
    if x.returncode:raise SystemExit(x.returncode)
