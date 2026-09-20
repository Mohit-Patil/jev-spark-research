"""Commit audited history_v7 files plus the two explicitly updated root docs.
No model requests. Secret scan uses the configured key locally without disclosure.
Other iterations, data files, compiled artifacts, and preexisting staged changes
are not included. Does not execute any untested or blocked experiment code.
"""
from pathlib import Path
import argparse,json,re,shutil,subprocess,sys
R=Path(__file__).resolve().parent;WS=R.parents[1]
sys.path[:0]=[str(R.parent/'production_v4'),str(R.parent)]
from authorized_budget import AuthorizedBudget
from repo_checkpoint import scan
from safe_jev import load_key
ROOT_DOCS={'CHECKPOINT.md','RESEARCH_LOG.md'}

def cmd(args,timeout=30):
    p=subprocess.run(args,cwd=WS,capture_output=True,text=True,timeout=timeout)
    if p.returncode:raise RuntimeError('command_failed_'+Path(args[0]).name)
    return p.stdout.strip()

def permitted(path):return path.startswith('round_20260920/history_v7/') or path in ROOT_DOCS

def main():
    p=argparse.ArgumentParser();p.add_argument('--message',required=True);a=p.parse_args()
    assert 1<=len(a.message)<=160 and WS.resolve()==Path.home()/'JevResearch'
    assert Path(cmd(['git','rev-parse','--show-toplevel'])).resolve()==WS.resolve()
    if cmd(['git','diff','--cached','--name-only']):raise RuntimeError('other_staged_changes_present')
    audit=json.loads((R/'FINAL_AUDIT.json').read_text())
    assert audit['status']=='audit_succeeded_with_experimental_failures_preserved'
    assert audit['budget']['attempts_used']==AuthorizedBudget().count()
    key,_=load_key();files=[]
    for f in R.rglob('*'):
        if not f.is_file() or f.is_symlink():continue
        if any(x.startswith('.') or x in ('classes','__pycache__','warehouse','data') for x in f.relative_to(R).parts):continue
        if f.name in ('last_git_checkpoint.json','FINAL_GIT_CHECKPOINT.json') or f.suffix not in ('.py','.scala','.json','.jsonl','.md','.txt'):continue
        if f.stat().st_size>4000000:raise RuntimeError('artifact_review_bound')
        if not scan(f.read_bytes(),key):raise RuntimeError('secret_scan_failed')
        files.append(str(f.relative_to(WS)))
    for name in sorted(ROOT_DOCS):
        f=WS/name
        if not f.is_file() or f.is_symlink() or f.stat().st_size>100000:raise RuntimeError('root_document_bound')
        if 'history_v7' not in f.read_text():raise RuntimeError('root_document_not_updated')
        if not scan(f.read_bytes(),key):raise RuntimeError('root_secret_scan_failed')
        files.append(name)
    key=None
    cmd(['git','check-ignore','--no-index','round_20260920/history_v7/.env'])
    cmd(['git','add','--',*sorted(files)])
    staged=cmd(['git','diff','--cached','--name-only']).splitlines()
    if any(not permitted(f) for f in staged):raise RuntimeError('concurrent_staging_stop')
    if staged:cmd(['git','commit','-m',a.message],90)
    report={'commit':cmd(['git','rev-parse','HEAD']),'commit_created':bool(staged),'files_scanned':len(files),
      'scope':'history_v7 plus CHECKPOINT.md and RESEARCH_LOG.md','secret_scan_passed':True,'env_ignored':True,
      'budget':AuthorizedBudget().status(),'push_succeeded':False}
    remote=cmd(['git','remote','get-url','origin'])
    if not re.fullmatch(r'(?:https://github\.com/|git@github\.com:)Mohit-Patil/jev-spark-research(?:\.git)?',remote):raise RuntimeError('unexpected_remote')
    gh=shutil.which('gh')
    if gh:
        info=json.loads(cmd([gh,'repo','view','Mohit-Patil/jev-spark-research','--json','isPrivate,nameWithOwner']))
        assert info['isPrivate'] and info['nameWithOwner']=='Mohit-Patil/jev-spark-research'
        branch=cmd(['git','branch','--show-current']);assert branch=='main'
        pushed=subprocess.run(['git','-c','credential.helper=','-c',f'credential.helper=!{gh} auth git-credential','push','origin',branch],cwd=WS,capture_output=True,timeout=90)
        report['push_succeeded']=pushed.returncode==0
    report['root_documents_committed']=not bool(cmd(['git','diff','HEAD','--','CHECKPOINT.md','RESEARCH_LOG.md']))
    (R/'FINAL_GIT_CHECKPOINT.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
if __name__=='__main__':
    try:main()
    except Exception as e:
        print(json.dumps({'status':'failed','error_type':type(e).__name__,'details_suppressed':True}));raise SystemExit(1)
