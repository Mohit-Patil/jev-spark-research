"""Commit only reviewed production_v4 files, never another research task's work."""
from pathlib import Path
import argparse,json,re,shutil,subprocess,sys
R=Path(__file__).resolve().parent;WS=R.parents[1]
sys.path.insert(0,str(R.parent))
from repo_checkpoint import scan
from safe_jev import Budget,load_key

def cmd(args,timeout=30):
    p=subprocess.run(args,cwd=WS,capture_output=True,text=True,timeout=timeout)
    if p.returncode:raise RuntimeError('command_failed_'+Path(args[0]).name)
    return p.stdout.strip()

def main():
    p=argparse.ArgumentParser();p.add_argument('--message',required=True);a=p.parse_args()
    assert 1<=len(a.message)<=160 and WS.resolve()==Path.home()/'JevResearch'
    assert Path(cmd(['git','rev-parse','--show-toplevel'])).resolve()==WS.resolve()
    staged=cmd(['git','diff','--cached','--name-only'])
    if staged:raise RuntimeError('another_staged_change_present')
    key,meta=load_key();files=[]
    for f in R.rglob('*'):
        if not f.is_file() or f.is_symlink():continue
        if any(x.startswith('.') or x in ('classes','__pycache__','warehouse','data') for x in f.relative_to(R).parts):continue
        if f.suffix not in ('.py','.scala','.json','.jsonl','.md','.txt'):continue
        if f.stat().st_size>4000000:raise RuntimeError('artifact_exceeds_review_limit')
        if not scan(f.read_bytes(),key):raise RuntimeError('secret_scan_failed')
        files.append(str(f.relative_to(WS)))
    key=None
    cmd(['git','check-ignore','--no-index','round_20260920/production_v4/.env'])
    cmd(['git','add','--']+sorted(files))
    changed=bool(cmd(['git','diff','--cached','--name-only']))
    if changed:cmd(['git','commit','-m',a.message],90)
    report={'commit':cmd(['git','rev-parse','HEAD']),'commit_created':changed,'files_scanned':len(files),
        'scope':'round_20260920/production_v4 only','secret_scan_passed':True,'env_ignored':True,
        'budget_used':Budget().count(),'new_jev_calls':0,'push_succeeded':False}
    remote=cmd(['git','remote','get-url','origin'])
    if not re.fullmatch(r'(?:https://github\.com/|git@github\.com:)Mohit-Patil/jev-spark-research(?:\.git)?',remote):
        raise RuntimeError('unexpected_remote')
    gh=shutil.which('gh')
    if gh:
        info=json.loads(cmd([gh,'repo','view','Mohit-Patil/jev-spark-research','--json','isPrivate,nameWithOwner']))
        assert info['isPrivate'] and info['nameWithOwner']=='Mohit-Patil/jev-spark-research'
        branch=cmd(['git','branch','--show-current']);assert branch=='main'
        q=subprocess.run(['git','-c','credential.helper=','-c',f'credential.helper=!{gh} auth git-credential','push','origin',branch],cwd=WS,capture_output=True,timeout=90)
        report['push_succeeded']=q.returncode==0
    (R/'last_git_checkpoint.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
if __name__=='__main__':
    try:main()
    except Exception as e:
        print(json.dumps({'status':'failed','error_type':type(e).__name__,'details_suppressed':True}));raise SystemExit(1)
