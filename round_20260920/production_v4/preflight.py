"""Read-only preflight and bounded public-source retrieval. No secret reads/API calls."""
from pathlib import Path
import hashlib, importlib.metadata as md, json, os, shutil, subprocess, sys, time
import httpx, pyspark
ROOT=Path(__file__).resolve().parent
WS=ROOT.parents[1]
sys.path.insert(0,str(ROOT.parent))
from safe_jev import Budget

def run(args):
    p=subprocess.run(args,cwd=WS,capture_output=True,text=True,timeout=20)
    return {'returncode':p.returncode,'stdout':p.stdout[:18000],'stderr':p.stderr[:1000]}

def main():
    assert WS.resolve()==Path.home()/'JevResearch'
    report={'workspace_exists':WS.is_dir(),'kit_exists':(Path.home()/'jev-mac-native').is_dir(),
        'budget_used':Budget().count(),'limit':50,'git_head':run(['git','rev-parse','HEAD']),
        'versions':{n:md.version(n) for n in ('pyspark','httpx')},'disk_free_bytes':shutil.disk_usage(WS).free,
        'ram':run(['/usr/sbin/sysctl','-n','hw.memsize']),'memory':run(['/usr/bin/vm_stat']),
        'git_status':run(['git','status','--porcelain']), 'new_jev_calls':0}
    source=ROOT/'sources';source.mkdir(exist_ok=True)
    commit='29434ea766b0fc3c3bf6eaadb43a8f931133649e'
    paths=['sql/core/src/main/scala/org/apache/spark/sql/SparkSessionExtensions.scala',
           'sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala',
           'sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BroadcastHashJoinExec.scala',
           'sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashJoin.scala']
    report['sources']=[]
    with httpx.Client(timeout=20,follow_redirects=False,trust_env=False) as c:
        for p in paths:
            url=f'https://raw.githubusercontent.com/apache/spark/{commit}/{p}'
            r=c.get(url)
            if r.status_code!=200 or len(r.content)>200000:
                report['sources'].append({'url':url,'status':r.status_code});continue
            dest=source/Path(p).name
            if dest.exists(): assert dest.read_bytes()==r.content
            else: dest.write_bytes(r.content)
            report['sources'].append({'url':url,'sha256':hashlib.sha256(r.content).hexdigest(),'bytes':len(r.content)})
    jars=Path(pyspark.__file__).parent/'jars'
    for name in ['org.apache.spark.sql.SparkSessionExtensions','org.apache.spark.sql.execution.exchange.EnsureRequirements']:
        report[name]=run(['javap','-classpath',str(jars/'*'),name])
    (ROOT/'preflight.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
if __name__=='__main__': main()
