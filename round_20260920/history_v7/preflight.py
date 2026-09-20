"""Resume the scoped history experiment. Metadata/public documentation only.
No secret reads, Spark execution, package installation or model API calls.
"""
from pathlib import Path
import hashlib, importlib.metadata as md, json, os, shutil, subprocess, sys, time
import httpx
R=Path(__file__).resolve().parent;ROUND=R.parent;WS=ROUND.parent
sys.path.insert(0,str(ROUND/'production_v4'))
from authorized_budget import AuthorizedBudget

def git(*args):
    p=subprocess.run(['git',*args],cwd=WS,capture_output=True,text=True,timeout=15)
    return {'returncode':p.returncode,'stdout':p.stdout.strip()[:3000]}

def main():
    assert WS.resolve()==Path.home()/'JevResearch'
    report={'created_unix':time.time(),'paths':{str(p):{'directory':p.is_dir()} for p in (WS,Path.home()/'jev-mac-native')},
      'budget':AuthorizedBudget().status(),'git_head':git('rev-parse','HEAD'),'branch':git('branch','--show-current'),
      'staged_changes':git('diff','--cached','--name-only'),'disk_free_bytes':shutil.disk_usage(WS).free,
      'versions':{k:md.version(k) for k in ('pyspark','httpx')},'load_average':os.getloadavg(),
      'spark_queries':0,'model_api_calls':0,'secret_reads':0,'public_documents':[]}
    # Wider authorization is recorded, while this finite iteration remains below
    # the already configured 10,000 cumulative safety ceiling. No ledger reset.
    authorization={'user_message_utc':'2026-09-20T18:08:42Z','user_authorization':'Proceed without an API or Git-call allowance limit.',
      'execution_policy':'Retain cumulative usage; use at most 60 model attempts in each tracked job and 80 planned in this iteration. Existing 10000 ceiling need not be raised for this experiment.',
      'unchanged_limits':'Synthetic data; native local[2]; research workspace only; no detached work; bridge maximum 600 seconds.'}
    (R/'AUTHORIZATION.md').write_text('# Authorization and operational bounds\n\n'+json.dumps(authorization,indent=2)+'\n')
    with httpx.Client(timeout=15,follow_redirects=False,trust_env=False) as client:
        for name,url in [('choice','https://docs.typesafe.ai/primitives/choice'),('api','https://docs.typesafe.ai/api'),('models','https://docs.typesafe.ai/models')]:
            rec={'url':url,'name':name}
            try:
                with client.stream('GET',url) as res:
                    rec['http_status']=res.status_code;b=bytearray()
                    for chunk in res.iter_bytes():
                        b.extend(chunk)
                        if len(b)>1048576:raise ValueError('public_document_size_bound')
                if res.status_code==200:
                    (R/('official_'+name+'.html')).write_bytes(b)
                    rec.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest(),
                      contains_choice=b'choice' in b,contains_pinned_model=b'jev-1.13.0' in b,contains_endpoint=b'/v1/systemone' in b)
            except Exception as e:rec['error_type']=type(e).__name__
            report['public_documents'].append(rec)
    (R/'PREFLIGHT.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
if __name__=='__main__':main()
