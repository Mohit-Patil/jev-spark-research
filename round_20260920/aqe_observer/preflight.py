"""Read-only observer preflight; no network, Spark session, or credentials."""
from pathlib import Path
import hashlib, json, shutil, subprocess, sys
import pyspark
ROOT=Path(__file__).resolve().parent
WS=ROOT.parents[1]
sys.path.insert(0,str(ROOT.parent))
from safe_jev import Budget
jars=Path(pyspark.__file__).parent/'jars'
source=ROOT.parent/'sources'/'AdaptiveSparkPlanExec.scala'
lines=source.read_text().splitlines()
call_lines={}
for i,line in enumerate(lines,1):
    if 'val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)' in line: call_lines['CURRENT']=i
    if 'val newCost = costEvaluator.evaluateCost(newPhysicalPlan)' in line: call_lines['PROPOSED']=i
java=Path(shutil.which('java')).resolve(); javap=java.with_name('javap')
cp=str(jars/'*')
report={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'call_lines':call_lines,
 'spark_version':pyspark.__version__,'java':str(java),'compiler_jars':[p.name for p in jars.glob('scala-compiler-*.jar')],
 'json_jars':[p.name for p in jars.glob('jackson-databind-*.jar')], 'budget_used':Budget().count(),
 'git_head':subprocess.run(['git','rev-parse','HEAD'],cwd=WS,capture_output=True,text=True,check=True).stdout.strip(),
 'git_status':subprocess.run(['git','status','--short'],cwd=WS,capture_output=True,text=True,check=True).stdout,
 'signature_source':'installed PySpark jars; exact v4.0.1 source previously saved'}
for name in ['org.apache.spark.sql.execution.adaptive.QueryStageExec','org.apache.spark.sql.execution.adaptive.CostEvaluator','org.apache.spark.sql.execution.adaptive.Cost']:
    p=subprocess.run([str(javap),'-classpath',cp,name],capture_output=True,text=True,timeout=20)
    report[name]={'returncode':p.returncode,'signature':p.stdout[:16000]}
p=subprocess.run([str(javap),'-p','-c','-l','-classpath',cp,'org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec'],capture_output=True,text=True,timeout=30)
text=p.stdout
(ROOT/'AdaptiveSparkPlanExec.javap.txt').write_text(text)
report['evaluateCost_bytecode_contexts']=[]
for i,line in enumerate(text.splitlines()):
    if 'CostEvaluator.evaluateCost' in line:
        report['evaluateCost_bytecode_contexts'].append('\n'.join(text.splitlines()[max(0,i-5):i+8]))
report['delegate_source_context']='\n'.join(line for line in lines if 'SimpleCostEvaluator' in line or 'ADAPTIVE_FORCE_OPTIMIZE' in line)
report['all_call_lines_in_bytecode_line_table']=all(('line '+str(n)+':') in text for n in call_lines.values())
(ROOT/'preflight.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
