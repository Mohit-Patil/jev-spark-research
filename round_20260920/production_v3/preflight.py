"""Metadata-only resume check. No secret reads, network requests or Spark jobs."""
from pathlib import Path
import hashlib, json, os, platform, shutil, subprocess, sys
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
WS=ROUND.parent
assert WS.resolve()==(Path.home()/'JevResearch').resolve()
sys.path.insert(0,str(ROUND))
from safe_jev import Budget, LEDGER
assert LEDGER.is_file(), 'existing cumulative request ledger required'
def git(*args):
    r=subprocess.run(['git',*args],cwd=WS,capture_output=True,text=True,timeout=15)
    assert r.returncode==0, 'git metadata unavailable'
    return r.stdout.strip()
files=[ROUND/'aqe_observer'/'NativeBoundaryObserverV3.scala',ROUND/'practical_v2'/'scale_benchmark.py',ROUND/'safe_jev.py',ROUND/'repo_checkpoint.py']
report={'kind':'resume_metadata_only','challenge_previously_matched':True,
 'paths':{str(p):{'exists':p.exists(),'directory':p.is_dir()} for p in [WS,Path.home()/'jev-mac-native']},
 'git_head':git('rev-parse','HEAD'),'git_branch':git('branch','--show-current'),
 'git_root_matches_workspace':Path(git('rev-parse','--show-toplevel')).resolve()==WS.resolve(),
 'api_attempts_used':Budget().count(),'api_limit':50,'new_api_attempts':0,
 'python':platform.python_version(),'architecture':platform.machine(),
 'disk_free_bytes':shutil.disk_usage(WS).free,'load_average':os.getloadavg(),
 'source_hashes':{str(p.relative_to(WS)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
 'versions':{},'credentials_read':False}
import importlib.metadata
for p in ['pyspark','httpx','pytest']:
    report['versions'][p]=importlib.metadata.version(p)
with (HERE/'preflight.json').open('x') as f:json.dump(report,f,indent=2)
print(json.dumps(report,indent=2))
