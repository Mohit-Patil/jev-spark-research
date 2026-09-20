"""Resolve exact public source names instead of guessing; preserve prior 404 failure."""
from pathlib import Path
import hashlib,json,subprocess
import httpx,pyspark
root=Path(__file__).resolve().parent;out=root/'sources';jars=Path(pyspark.__file__).parent/'jars';report={}
for klass in ['org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator','org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec']:
    r=subprocess.run(['javap','-classpath',str(jars/'*'),klass],capture_output=True,text=True,timeout=15)
    report[klass]={'returncode':r.returncode,'signature':r.stdout[:14000],'stderr':r.stderr[:1000]}
with httpx.Client(timeout=20,trust_env=False,follow_redirects=False) as c:
    base='https://api.github.com/repos/apache/spark/contents/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive?ref=v4.0.1'
    try:
        r=c.get(base);r.raise_for_status();names=[x['name'] for x in r.json() if x['type']=='file'];report['cost_source_names']=[x for x in names if 'cost' in x.lower()]
        for name in report['cost_source_names']:
            if name=='costing.scala':continue
            url='https://raw.githubusercontent.com/apache/spark/v4.0.1/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/'+name
            r=c.get(url);r.raise_for_status();assert len(r.content)<100000
            (out/name).write_text(r.text);report[name]={'url':url,'sha256':hashlib.sha256(r.content).hexdigest(),'text':r.text}
        tag=c.get('https://api.github.com/repos/apache/spark/git/ref/tags/v4.0.1').json()['object'];report['release_tag_object']=tag
        report['release_commit']=c.get('https://api.github.com/repos/apache/spark/git/tags/'+tag['sha']).json()['object'] if tag['type']=='tag' else tag
    except Exception as e:report['public_source_error']=type(e).__name__
(out/'observer_dependencies_v2.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
