"""One attached, bounded bridge job: repair/build/smoke, then only on success benchmark.
No detached jobs, no API requests, and no retries of failed experiments.
"""
from pathlib import Path
import json,subprocess,sys,time
R=Path(__file__).resolve().parent
started=time.monotonic();steps=[]
for script,cap in [('repair_and_smoke.py',150),('runtime_benchmark.py',420)]:
    remaining=570-(time.monotonic()-started)
    if remaining<30:raise RuntimeError('round_deadline')
    p=subprocess.run([sys.executable,str(R/script)],cwd=R,timeout=min(cap,remaining))
    steps.append({'script':script,'returncode':p.returncode})
    print(json.dumps(steps[-1]),flush=True)
    if p.returncode:
        (R/'round_attempt_1.json').write_text(json.dumps({'status':'failed','steps':steps},indent=2))
        raise SystemExit(p.returncode)
(R/'round_attempt_1.json').write_text(json.dumps({'status':'succeeded','steps':steps,'elapsed_s':time.monotonic()-started},indent=2))
