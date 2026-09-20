"""One bounded attached job. Offline budget tests, corrected saved-data audit,
then a scoped Git checkpoint. No Spark queries or paid model requests.
"""
from pathlib import Path
import json,subprocess,sys,time
R=Path(__file__).resolve().parent;WS=R.parents[1]
from authorized_budget import AuthorizedBudget
started=time.monotonic();before=AuthorizedBudget().status();results=[]
# Keep private accounting policy out of any ordinary `git add`, project locally.
exclude=WS/'.git'/'info'/'exclude'
if not exclude.parent.is_dir():raise RuntimeError('expected_git_metadata_missing')
old=exclude.read_text() if exclude.exists() else ''
entry='/.jev_budget_authorization.json'
if entry not in old.splitlines():exclude.write_text(old+'\n# Local research request accounting\n'+entry+'\n')
for name,cap,args in [('test_authorized_budget.py',90,[]),('audit_completed_results_v2.py',180,[]),
  ('checkpoint_scoped.py',160,['--message','research: preserve cumulative 10000-attempt authorization and verify completed runtime results'])]:
    p=subprocess.run([sys.executable,str(R/name),*args],cwd=R,timeout=cap)
    results.append({'script':name,'returncode':p.returncode});print(json.dumps(results[-1]),flush=True)
    if p.returncode:raise SystemExit(p.returncode)
after=AuthorizedBudget().status();assert before['attempts_used']==after['attempts_used']
(R/'BUDGET_AND_AUDIT_COMPLETED.json').write_text(json.dumps({'status':'succeeded','steps':results,
    'budget':after,'live_requests':0,'elapsed_s':time.monotonic()-started},indent=2))
print(json.dumps({'status':'succeeded','budget':after,'live_requests':0},indent=2),flush=True)
