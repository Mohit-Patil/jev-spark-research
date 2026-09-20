"""Create safe local checkpoints; optionally publish to an authenticated private GitHub repo.
Never prints subprocess stderr, credential values, or secret-file contents.
Only explicitly allowed research file types/directories are staged.
"""
from pathlib import Path
import argparse, hashlib, json, os, re, shutil, stat, subprocess, sys
ROOT=Path(__file__).resolve().parent
WS=ROOT.parent
sys.path.insert(0,str(ROOT))

def cmd(args, timeout=30, ok=False):
    p=subprocess.run(args,cwd=WS,capture_output=True,text=True,timeout=timeout)
    if ok and p.returncode: raise RuntimeError('command_failed_'+Path(args[0]).name)
    return p

def scan(data, key):
    if key and key.encode() in data: return False
    patterns=[rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',rb'gh[pousr]_[A-Za-z0-9]{30,}',rb'github_pat_[A-Za-z0-9_]{30,}',rb'(?m)^\s*(?:export\s+)?TYPESAFE_API_KEY\s*=\s*[^\s\x22\x27#]{8,}']
    return not any(re.search(p,data) for p in patterns)

def main():
    a=argparse.ArgumentParser();a.add_argument('--message',required=True);a.add_argument('--publish',action='store_true');args=a.parse_args()
    if not 1<=len(args.message)<=160: raise RuntimeError('invalid_commit_message')
    if WS.resolve()!=Path.home()/'JevResearch': raise RuntimeError('unexpected_workspace')
    from safe_jev import Budget, load_key, SecretError
    key=None
    try:
        key,meta=load_key()
        p=Path(meta['source'])
        if p.parent in (WS,ROOT) and not p.is_symlink(): p.chmod(0o600)
    except SecretError:
        meta={'configured':False}
    report={'paths':{str(p):{'exists':p.exists(),'is_directory':p.is_dir()} for p in (WS,Path.home()/'jev-mac-native')},'attempts_used':Budget().count(),'limit':50,'git':shutil.which('git'),'gh':shutil.which('gh')}
    if not report['git']: raise RuntimeError('git_unavailable')
    git=report['git'];gh=report['gh'];login=None
    if gh:
        p=cmd([gh,'api','user','--jq','.login'])
        if p.returncode==0 and re.fullmatch(r'[A-Za-z0-9-]+',p.stdout.strip()): login=p.stdout.strip()
    report['github_authenticated']=bool(login)
    if not (WS/'.git').exists():
        cmd([git,'init','-b','main'],ok=True)
    top=cmd([git,'rev-parse','--show-toplevel'],ok=True).stdout.strip()
    if Path(top).resolve()!=WS.resolve(): raise RuntimeError('unexpected_git_root')
    for prop,value in [('user.name','Jev Research'),('user.email','research@localhost')]:
        if cmd([git,'config','--get',prop]).returncode: cmd([git,'config','--local',prop,value],ok=True)
    ignore=WS/'.gitignore';old=ignore.read_text() if ignore.exists() else ''
    block='''\n# Research safety: local secrets and durable request budget are never versioned.\n.env\n.env.*\n**/.env\n**/.env.*\n.jev_initial_round_attempts.jsonl\n.venv/\n**/.venv/\n__pycache__/\n.pytest_cache/\n*.pyc\n.DS_Store\nspark-temp/\n**/warehouse/\n**/spark-warehouse/\n**/metastore_db/\n**/derby.log\n**/artifacts/\n.tools/\n.git-safe-checkpoint.json\n'''
    if '# Research safety:' not in old: ignore.write_text(old+block)
    if cmd([git,'check-ignore','--no-index','.env','round_20260920/.env']).returncode: raise RuntimeError('secret_ignore_check_failed')
    tracked=cmd([git,'ls-files','-z'],ok=True).stdout.split('\0')
    if any(Path(x).name=='.env' or Path(x).name.startswith('.env.') for x in tracked if x): raise RuntimeError('secret_already_tracked_stop')
    allowed=[]
    for name in ('.gitignore','README.md','RESEARCH_BRIEF.md','smoke.py','requirements.txt','CHECKPOINT.md','RESEARCH_LOG.md'):
        p=WS/name
        if p.is_file(): allowed.append(p)
    for base in (ROOT,WS/'docs',WS/'tests',WS/'src'):
        if not base.exists():continue
        for p in base.rglob('*'):
            if any(part.startswith('.') or part in ('__pycache__','warehouse','artifacts') for part in p.relative_to(base).parts): continue
            if p.is_file() and not p.is_symlink() and p.suffix in ('.py','.json','.jsonl','.md','.txt','.scala','.java','.sh','.toml'):
                if p.stat().st_size>4000000:continue
                allowed.append(p)
    for p in allowed:
        if not scan(p.read_bytes(),key):raise RuntimeError('secret_scan_failed_stop')
    # Also scan all existing staged blobs, not only newly allowed files.
    for name in cmd([git,'diff','--cached','--name-only','-z'],ok=True).stdout.split('\0'):
        if not name:continue
        blob=subprocess.run([git,'show',':'+name],cwd=WS,capture_output=True,timeout=10)
        if blob.returncode or not scan(blob.stdout,key):raise RuntimeError('staged_secret_scan_failed')
    cmd([git,'add','--']+[str(p.relative_to(WS)) for p in allowed],ok=True)
    changed=cmd([git,'diff','--cached','--quiet']).returncode==1
    if changed:cmd([git,'commit','-m',args.message],ok=True)
    report['commit_created']=changed
    report['commit']=cmd([git,'rev-parse','HEAD'],ok=True).stdout.strip()
    report['branch']=cmd([git,'branch','--show-current'],ok=True).stdout.strip()
    report['tracked_files']=len([x for x in cmd([git,'ls-files','-z'],ok=True).stdout.split('\0') if x])
    report['secret_scan_passed']=True
    report['env_files_ignored']=True
    if args.publish and login:
        remote=cmd([git,'remote','get-url','origin'])
        if remote.returncode:
            name=login+'/jev-spark-research'
            existing=cmd([gh,'repo','view',name,'--json','nameWithOwner,isPrivate,url'])
            if existing.returncode==0:
                report['github_status']='name_exists_no_automatic_reuse'
            else:
                created=cmd([gh,'repo','create',name,'--private','--source',str(WS),'--remote','origin'],timeout=60)
                report['github_status']='created_private' if created.returncode==0 else 'create_failed'
        else:
            report['github_status']='origin_already_configured'
        # Inspect only an exact GitHub repository remote; never disclose token-bearing URLs.
        remote=cmd([git,'remote','get-url','origin'])
        m=re.fullmatch(r'(?:https://github\.com/|git@github\.com:)([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?',remote.stdout.strip()) if remote.returncode==0 else None
        if m:
            repo=m.group(1);view=cmd([gh,'repo','view',repo,'--json','nameWithOwner,isPrivate,url'])
            if view.returncode==0:
                info=json.loads(view.stdout)
                if info.get('isPrivate') and repo.split('/')[0]==login:
                    report['repository']=info
                    # Scoped credential helper; do not modify the account's global Git configuration.
                    push=cmd([git,'-c','credential.helper=','-c',f'credential.helper=!{gh} auth git-credential','push','-u','origin',report['branch']],timeout=90)
                    report['push_succeeded']=push.returncode==0
                else:report['push_succeeded']=False;report['github_status']='ownership_or_privacy_check_failed'
    elif args.publish:
        report['github_status']='no_authenticated_github_cli'
    key=None
    (WS/'.git-safe-checkpoint.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    try:main()
    except Exception as e:
        print(json.dumps({'status':'failed','error_type':type(e).__name__,'reason':str(e) if type(e) is RuntimeError else 'details_suppressed'}));raise SystemExit(1)
