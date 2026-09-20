"""Attached bounded round: validate offline, checkpoint fixed design, then execute.
Never schedules detached jobs or changes original source files.
"""
from pathlib import Path
import hashlib,json,subprocess,sys,time
R=Path(__file__).resolve().parent;started=time.monotonic()
from authorized_budget import AuthorizedBudget
from runtime_representation_v6 import CASES,ARMS
freeze={'frozen_unix':time.time(),'cases':CASES,'arms':ARMS,'reps':3,'max_live_attempts':18,
    'budget':AuthorizedBudget().status(),'code_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [R/'runtime_representation_v6.py',R/'representation_inputs.py',R/'authorized_budget.py']},
    'no_tuning_on_this_batch':True}
with (R/'REPRESENTATION_PREREGISTRATION.json').open('x') as f:json.dump(freeze,f,indent=2)
for p,cap,args in [('test_representations.py',60,[]),('checkpoint_scoped.py',120,['--message','research: preregister runtime feature ablation under cumulative authorized budget']),
                   ('runtime_representation_v6.py',460,[])]:
    remain=580-(time.monotonic()-started)
    if remain<20:raise RuntimeError('bounded_round_deadline')
    result=subprocess.run([sys.executable,str(R/p),*args],cwd=R,timeout=min(cap,remain))
    print(json.dumps({'step':p,'returncode':result.returncode}),flush=True)
    if result.returncode:raise SystemExit(result.returncode)
print(json.dumps({'status':'succeeded','budget':AuthorizedBudget().status(),'elapsed_s':time.monotonic()-started}),flush=True)
