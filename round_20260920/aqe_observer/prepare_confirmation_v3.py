"""Prepare a fixed-size replication; preserve original trial, observer and anchor.
Only output directory, seed, sample counts, and experiment description change.
"""
from pathlib import Path
import hashlib,json
ROOT=Path(__file__).resolve().parent
original=ROOT/'counterfactual_trial_v3.py'
frozen=json.loads((ROOT/'counterfactual_trial_v3'/'frozen_trial.json').read_text())
assert hashlib.sha256(original.read_bytes()).hexdigest()==frozen['source_sha256'][original.name]
for p in [ROOT/'NativeBoundaryObserverV3.scala',ROOT.parent/'baseline.py']:
    assert hashlib.sha256(p.read_bytes()).hexdigest()==frozen['source_sha256'][p.name]
s=original.read_text()
changes=[("out=ROOT/'counterfactual_trial_v3'","out=ROOT/'counterfactual_confirm_v3'"),
 ("'seed':1846","'seed':1847"),("rng=random.Random(1846)","rng=random.Random(1847)"),
 ("'measured_rounds':10","'measured_rounds':30"),("('measured',10)","('measured',30)"),
 ("'measured_runs':20","'measured_runs':60"),("for rep in range(10):","for rep in range(30):"),
 ("'kind':'single-boundary observed-state-matched randomized counterfactual trial'",
  "'kind':'fixed 30-round confirmation of counterfactual_trial_v3; same anchor, data and observer'")]
for old,new in changes:
    assert s.count(old)==1,(old,s.count(old))
    s=s.replace(old,new)
p=ROOT/'counterfactual_confirm_v3.py'
with p.open('x') as f:f.write(s)
print(json.dumps({'created':p.name,'sha256':hashlib.sha256(s.encode()).hexdigest(),'changes':changes,
 'anchor_preserved':frozen['target_signature'],'new_measured_rounds':30,'new_api_calls':0,'executed':False},indent=2))
