"""Inspect installed Spark build identity and download matching PUBLIC sources only."""
from pathlib import Path
import hashlib, json, time, zipfile
import httpx, pyspark
ROOT=Path(__file__).resolve().parent; OUT=ROOT/'sources'; OUT.mkdir(exist_ok=True)
jars=Path(pyspark.__file__).parent/'jars'
report={'pyspark_version':pyspark.__version__,'compiler_jars':[p.name for p in jars.glob('scala-compiler-*.jar')],'artifacts':[]}
for jar in jars.glob('spark-core_*.jar'):
    with zipfile.ZipFile(jar) as z:
        for name in z.namelist():
            if name.endswith('spark-version-info.properties'):
                props=dict(line.split('=',1) for line in z.read(name).decode().splitlines() if '=' in line and not line.startswith('#'))
                report['build']={k:v for k,v in props.items() if k in ('version','revision','branch','date')}
ref=report.get('build',{}).get('revision') or 'v4.0.1'
report['source_ref']=ref
base='https://raw.githubusercontent.com/apache/spark/'+ref+'/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/'
with httpx.Client(timeout=20,follow_redirects=False,trust_env=False) as client:
    for filename in ['AdaptiveSparkPlanExec.scala','costing.scala','QueryStageExec.scala']:
        uri=base+filename
        try:
            r=client.get(uri)
            if r.status_code!=200 or len(r.content)>300000: raise ValueError('source_fetch_failed')
            text=r.text; (OUT/filename).write_text(text)
            report['artifacts'].append({'file':filename,'url':uri,'sha256':hashlib.sha256(r.content).hexdigest(),'fetched_unix':time.time()})
            lines=text.splitlines()
            if filename=='AdaptiveSparkPlanExec.scala':
                hits=[i for i,l in enumerate(lines) if 'evaluateCost' in l or 'customCostEvaluatorClass' in l]
            elif filename=='costing.scala': hits=[i for i,l in enumerate(lines) if 'class SimpleCostEvaluator' in l or 'trait CostEvaluator' in l or 'abstract class Cost' in l]
            else: hits=[i for i,l in enumerate(lines) if 'def mapStats' in l or 'getRuntimeStatistics' in l or 'def isMaterialized' in l]
            report[filename+'_excerpts']=[{'start_line':max(1,i-4),'text':'\n'.join(lines[max(0,i-5):i+17])} for i in hits[:6]]
        except Exception as e: report['artifacts'].append({'file':filename,'status':'failed','error_type':type(e).__name__})
(OUT/'provenance.json').write_text(json.dumps(report,indent=2)); print(json.dumps(report,indent=2))
