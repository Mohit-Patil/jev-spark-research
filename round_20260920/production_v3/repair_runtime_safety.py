"""Fix a Py4J fault-injection harness bug; preserve the original failed evidence.
The helper sets ONLY an observer audit counter inside the JVM. No API accounting,
credentials, network policy or bridge configuration is changed.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
import pyspark
HERE=Path(__file__).resolve().parent

def main():
    out=HERE/'runtime_fault_helper';out.mkdir(exist_ok=False)
    classes=out/'classes';classes.mkdir()
    guard=HERE/'observer_safety_v1'/'guarded-observer.jar';assert guard.is_file()
    jars=Path(pyspark.__file__).parent/'jars'
    source=out/'ObserverFaultInjector.scala'
    source.write_text('''package org.apache.spark.sql.execution.adaptive
/** Test-only observer log-capacity fault injection; unrelated to API budgets. */
object ObserverFaultInjector {
  def saturateAuditCounterForTest():Unit = { NativeBoundaryObserverV6.pairs.set(1000L) }
}
''')
    helper=out/'fault-helper.jar'
    commands=[['java','-Xmx512m','-cp',str(guard)+':'+str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source)],
              ['jar','--create','--file',str(helper),'-C',str(classes),'.']]
    for i,cmd in enumerate(commands):
        p=subprocess.run(cmd,cwd=HERE,capture_output=True,text=True,timeout=60)
        rec={'step':i,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
        (out/f'compile_{i}.json').write_text(json.dumps(rec,indent=2));print(json.dumps(rec),flush=True)
        if p.returncode:raise RuntimeError('fault_helper_compile_failed')
    old=HERE/'runtime_safety_checks.py';text=old.read_text()
    changes=[
      ("out=HERE/'runtime_safety_v1'","out=HERE/'runtime_safety_v2'"),
      (".config('spark.driver.extraClassPath',str(jar)).config('spark.jars',str(jar))",
       ".config('spark.driver.extraClassPath',str(jar)+':'+str(HERE/'runtime_fault_helper'/'fault-helper.jar')).config('spark.jars',str(jar)+','+str(HERE/'runtime_fault_helper'/'fault-helper.jar'))"),
      ('obj.pairs().set(1000)',
       'spark._jvm.org.apache.spark.sql.execution.adaptive.ObserverFaultInjector.saturateAuditCounterForTest()')]
    for a,b in changes:
        assert text.count(a)==1,'unexpected_harness_anchor'
        text=text.replace(a,b)
    revised=HERE/'runtime_safety_checks_v2.py'
    with revised.open('x') as f:f.write(text)
    compile(text,str(revised),'exec')
    info={'kind':'repair_test_harness_only','original_sha256':hashlib.sha256(old.read_bytes()).hexdigest(),
      'revised_sha256':hashlib.sha256(revised.read_bytes()).hexdigest(),
      'preserved_failed_directory':'runtime_safety_v1','new_output_directory':'runtime_safety_v2',
      'bug':'Py4J converted AtomicLong to int; set observer test counter wholly inside JVM instead.'}
    (out/'REPAIR.json').write_text(json.dumps(info,indent=2))
    p=subprocess.run([sys.executable,str(revised)],cwd=HERE,capture_output=True,text=True,timeout=120)
    (out/'runtime_log.txt').write_text(p.stdout+'\n'+p.stderr)
    print(p.stdout,flush=True)
    if p.returncode:print(p.stderr[-12000:],flush=True)
    print(json.dumps({'runtime_returncode':p.returncode,'new_api_requests':0,'repair':info}),flush=True)
    raise SystemExit(p.returncode)
if __name__=='__main__':main()
