"""Preserve failed build provenance, repair the compiler-reported type check, rerun once."""
from pathlib import Path
import subprocess,sys
R=Path(__file__).resolve().parent
p=R/'RuntimeCandidateExtension.scala';s=p.read_text()
a='Set(Inner,LeftOuter).contains(j.joinType)';b='(j.joinType==Inner || j.joinType==LeftOuter)'
assert s.count(a)==1
with (R/'build_smoke_v1'/'failed_source.scala').open('x') as f:f.write(s)
p.write_text(s.replace(a,b))
t=(R/'build_and_smoke.py').read_text();assert t.count("out=ROOT/'build_smoke_v1'")==1
v=R/'build_and_smoke_v2.py'
with v.open('x') as f:f.write(t.replace("out=ROOT/'build_smoke_v1'","out=ROOT/'build_smoke_v2'"))
r=subprocess.run([sys.executable,str(v)],cwd=R,timeout=210)
raise SystemExit(r.returncode)
