"""Read-only project audit. No network, no secret reads, no research execution."""
from pathlib import Path
import hashlib, json, subprocess, sys, time
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
WS = ROOT.parent
sys.path.insert(0, str(ROOT))
from safe_jev import Budget

def git(*args):
    p = subprocess.run(['git', *args], cwd=WS, capture_output=True, text=True, timeout=10)
    if p.returncode:
        raise RuntimeError('git_check_failed')
    return p.stdout.strip()

def main():
    assert WS.resolve() == (Path.home() / 'JevResearch').resolve()
    calls = [json.loads(x) for x in (ROOT/'jev_calls.jsonl').read_text().splitlines() if x]
    report = {
        'audit_unix': time.time(),
        'workspace_exists': WS.is_dir(),
        'connection_kit_exists': (Path.home()/'jev-mac-native').is_dir(),
        'head': git('rev-parse', 'HEAD'), 'branch': git('branch', '--show-current'),
        'changed_paths': git('status', '--porcelain').splitlines(),
        'api_attempts_used': Budget().count(), 'api_limit': 50,
        'saved_calls': len(calls),
        'last_call_metadata': [{k:r.get(k) for k in ('attempt','purpose','status','latency_s')} for r in calls[-8:]],
        'source_sha256': {name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in ('baseline.py','safe_jev.py','NativeBoundaryObserver.scala','live_pilot.py')},
        'new_jev_attempts': 0,
        'preservation': 'Original scripts, completed runs, request ledger and frozen policy are not modified.'
    }
    target = HERE/'preflight.json'
    with target.open('x') as f: json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))

if __name__ == '__main__': main()
