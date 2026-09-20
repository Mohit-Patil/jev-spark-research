"""Audit already completed work, document late fallbacks separately, commit.
No new Spark queries or model requests. Does not retry blocked cluster work.
"""
from pathlib import Path
import collections,json,subprocess,sys,time
R=Path(__file__).resolve().parent
from authorized_budget import AuthorizedBudget
before=AuthorizedBudget().count()
p=subprocess.run([sys.executable,str(R/'audit_representation_v6.py')],cwd=R,timeout=180)
if p.returncode:raise SystemExit(p.returncode)
calls=[json.loads(x) for x in (R/'runtime_representation_v6'/'decisions.jsonl').read_text().splitlines()]
diag={}
for view in ('VERBOSE','COMPACT'):
    cs=[c for c in calls if c['view']==view];ok=[c for c in cs if c['record']['status']=='succeeded'];bad=[c for c in cs if c['record']['status']!='succeeded']
    diag[view]={'valid_model_choices':dict(collections.Counter(c['record']['choice'] for c in ok)),
        'failed_or_late_attempts':[{'attempt':c['record']['attempt'],'http_status':c['record'].get('http_status'),
            'error_type':c['record'].get('error_type'),'http_latency_s':c['record'].get('latency_s')} for c in bad],
        'warning':'summary choice counts include ABSTAIN imposed by the error/late-response fallback, not solely actual model answers'}
(R/'CALL_AND_FALLBACK_DIAGNOSTIC_V6.json').write_text(json.dumps(diag,indent=2))
report=R/'RESULTS_BUDGET_AND_V6.md';text=report.read_text()
text+='''
## Late-response diagnostic

The valid-model-answer counts are separate from effective fallback choices: 16 requests completed validly and all 16 answered ABSTAIN. Two full-plan attempts failed the late-response handling and also used the native fallback. They remain charged; they are not two additional valid model abstentions. See CALL_AND_FALLBACK_DIAGNOSTIC_V6.json for sanitized statuses.

The 1.8-second response gate is a rejection threshold, not a strict wall-clock cancellation guarantee. The loopback callback has its own 2.2-second read timeout, while the HTTP operation can complete later; the longest recorded full-plan request was about 4.32 seconds. Late results cannot retroactively alter an already resumed query. This tail behavior remains a production limitation and is included in measured workflow costs.

The earlier V5 test did apply one live Jev BROADCAST recommendation. V6 applied none. These are different batches, not inconsistent reports. The model has not yet shown dependable net improvement over the cheap baselines.
'''
report.write_text(text)
assert before==AuthorizedBudget().count()
p=subprocess.run([sys.executable,str(R/'checkpoint_scoped.py'),'--message','research: audit bounded feature ablation and preserve latency failures'],cwd=R,timeout=160)
if p.returncode:raise SystemExit(p.returncode)
print(json.dumps({'status':'succeeded','budget':AuthorizedBudget().status(),'new_api_requests':0,
    'report':'round_20260920/production_v4/RESULTS_BUDGET_AND_V6.md','diagnostics':diag},indent=2),flush=True)
