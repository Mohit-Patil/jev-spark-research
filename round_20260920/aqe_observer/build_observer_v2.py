"""Compile the reviewed observer in this directory, using the installed Spark compiler jars.
No installations, credential access, network requests, or Spark jobs.
"""
from pathlib import Path
import hashlib,json,subprocess
import pyspark
ROOT=Path(__file__).resolve().parent

def main():
    assert pyspark.__version__=='4.0.1'
    check=json.loads((ROOT/'preflight.json').read_text())
    assert check['call_lines']=={'CURRENT':365,'PROPOSED':366}
    assert check['all_call_lines_in_bytecode_line_table']
    source=ROOT/'NativeBoundaryObserverV2.scala';assert source.is_file()
    out=ROOT/'observer_build_v2';out.mkdir(exist_ok=False)
    classes=out/'classes';classes.mkdir()
    jars=Path(pyspark.__file__).parent/'jars'
    commands=[['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source)],
              ['jar','--create','--file',str(out/'observer.jar'),'-C',str(classes),'.']]
    for i,cmd in enumerate(commands):
        p=subprocess.run(cmd,capture_output=True,text=True,timeout=90,cwd=ROOT)
        rec={'command':cmd,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
        (out/f'compile_{i}.json').write_text(json.dumps(rec,indent=2));print(json.dumps(rec),flush=True)
        if p.returncode:raise RuntimeError('compiler_step_failed')
    result={'status':'succeeded','spark_version':pyspark.__version__,
       'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
       'jar_sha256':hashlib.sha256((out/'observer.jar').read_bytes()).hexdigest()}
    (out/'COMPLETED.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
