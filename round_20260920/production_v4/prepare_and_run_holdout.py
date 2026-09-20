"""Preregister independent snapshots and an aligned native AQE baseline before execution."""
from pathlib import Path
import hashlib,json,subprocess,sys,time
R=Path(__file__).resolve().parent
started=time.monotonic();s=(R/'runtime_benchmark.py').read_text()
def replace(a,b):
    global s
    assert s.count(a)==1,a
    s=s.replace(a,b)
replace("ARMS=['OFF','SHADOW','APPLY','PRE_BROADCAST']","ARMS=['OFF','SHADOW','APPLY','TUNED_AQE']")
start=s.index('CASES=');end=s.index('\ndef signature',start)
newcases="CASES=[dict(name='heldout_skew18m',n=18000000,m=2000000,keep=500,skew=True,seed=81283,join='LEFT'),\n       dict(name='heldout_uniform18m',n=18000000,m=2000000,keep=500,skew=False,seed=37121,join='LEFT')]\n"
s=s[:start]+newcases+s[end:]
replace("default='runtime_dev_v1'","default='runtime_holdout_v1'")
replace("'scope':'runtime fixed-rule development, no model-driven choice'","'scope':'frozen independent snapshots; same query families, no model choice; aligned native AQE baseline added'")
replace("'new runtime families not yet evaluated; these are development cases'","'independent 18-million-row snapshots; same LEFT family, not an unseen-template holdout; no retuning'")
replace("spark.conf.set('spark.research.v4.mode','OFF' if arm=='PRE_BROADCAST' else arm)",
        "spark.conf.set('spark.sql.adaptive.autoBroadcastJoinThreshold',33554432) if arm=='TUNED_AQE' else spark.conf.unset('spark.sql.adaptive.autoBroadcastJoinThreshold')\n                        spark.conf.set('spark.research.v4.mode','OFF' if arm=='TUNED_AQE' else arm)")
replace("r.update(arm=arm,phase=phase,rep=rep,position=pos,events=new,", "r.update(arm=arm,phase=phase,rep=rep,position=pos,events=new,adaptive_broadcast_threshold_bytes=33554432 if arm=='TUNED_AQE' else 10485760,")
compile(s,'runtime_holdout.py','exec')
f=R/'runtime_holdout.py'
with f.open('x') as out:out.write(s)
freeze={'frozen_unix':time.time(),'source_sha256':hashlib.sha256(s.encode()).hexdigest(),
        'extension_sha256':hashlib.sha256((R/'RuntimeCandidateExtension.scala').read_bytes()).hexdigest(),
        'holdout_generated':False,'config':'TUNED_AQE changes adaptive broadcast threshold only, to the rule-aligned 32 MiB; other arms use defaults',
        'cases':[{'name':'heldout_skew18m','n':18000000,'m':2000000,'keep':500,'skew':True,'seed':81283},
                 {'name':'heldout_uniform18m','n':18000000,'m':2000000,'keep':500,'skew':False,'seed':37121}],
        'arms':['OFF','SHADOW','APPLY','TUNED_AQE'],'repetitions':5,'new_model_calls':0}
with (R/'PREREGISTERED_HOLDOUT.json').open('x') as out:json.dump(freeze,out,indent=2)
p=subprocess.run([sys.executable,str(R/'checkpoint_scoped.py'),'--message','research: verify runtime candidates and preregister aligned AQE holdout'],cwd=R,timeout=170)
if p.returncode:raise SystemExit(p.returncode)
p=subprocess.run([sys.executable,str(f)],cwd=R,timeout=min(420,570-(time.monotonic()-started)))
raise SystemExit(p.returncode)
