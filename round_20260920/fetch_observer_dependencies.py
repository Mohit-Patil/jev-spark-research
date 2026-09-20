"""Read-only inspection of published version-tag sources and installed class signatures."""
from pathlib import Path
import hashlib,json,subprocess
import httpx,pyspark
root=Path(__file__).resolve().parent;out=root/'sources';jars=Path(pyspark.__file__).parent/'jars'
report={}
with httpx.Client(timeout=20,trust_env=False,follow_redirects=False) as c:
    url='https://raw.githubusercontent.com/apache/spark/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/simpleCostEvaluator.scala'
    r=c.get(url);r.raise_for_status();assert len(r.content)<50000
    (out/'simpleCostEvaluator.scala').write_text(r.text)
    report['simple_cost_source']={'url':url,'sha256':hashlib.sha256(r.content).hexdigest(),'text':r.text}
    try:
        tag=c.get('https://api.github.com/repos/apache/spark/git/ref/tags/v4.0.1').json()['object']
        report['release_tag_object']=tag
        if tag['type']=='tag':report['release_commit']=c.get('https://api.github.com/repos/apache/spark/git/tags/'+tag['sha']).json()['object']
        else:report['release_commit']=tag
    except Exception as e:report['tag_resolution_error']=type(e).__name__
for klass in ['org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator','org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec']:
    result=subprocess.run(['javap','-classpath',str(jars/'*'),klass],capture_output=True,text=True,timeout=15)
    report[klass]={'returncode':result.returncode,'signature':result.stdout[:14000],'stderr':result.stderr[:1000]}
(out/'observer_dependencies.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
