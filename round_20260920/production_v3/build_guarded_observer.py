"""Preserve frozen V3; create and compile a fail-closed derivative and native JVM tests.
No credentials, network requests, or changes to the cumulative API ledger.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
import pyspark
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
WS=ROUND.parent
sys.path.insert(0,str(ROUND))
from safe_jev import Budget
OLD=ROUND/'aqe_observer'/'NativeBoundaryObserverV3.scala'
EXPECTED='d78af7d65b83f354074638c69e2880958bbe8e86319842b5bb62497497df3c79'

def main():
    original=OLD.read_bytes();assert hashlib.sha256(original).hexdigest()==EXPECTED
    out=HERE/'observer_safety_v1';out.mkdir(exist_ok=False);before=Budget().count()
    text=original.decode().replace('NativeBoundaryObserverV3','NativeBoundaryObserverV6')
    changes=[
      ('val sig=stateSignature(oldSnapshot,proposedSnapshot)',
       'val complete=NativeBoundaryObserverV6.completeSnapshot(oldSnapshot) && NativeBoundaryObserverV6.completeSnapshot(proposedSnapshot)\n              val sig=if(complete) stateSignature(oldSnapshot,proposedSnapshot) else ""\n              record.put("complete_plan_evidence",Boolean.box(complete))'),
      ('val eligible=sig==target && record.get("native_would_choose")=="PROPOSED"',
       'val eligible=complete && sig==target && record.get("native_would_choose")=="PROPOSED"'),
      ('NativeBoundaryObserverV6.emit(destination.toString,record)',
       'returned=NativeBoundaryObserverV6.auditedReturn(destination.toString,record,stock,returned)'),
      ('def emit(path:String,value:JMap[String,Object]):Unit = synchronized {',
       'def emit(path:String,value:JMap[String,Object]):Boolean = synchronized {'),
      ('Files.write(Paths.get(path),bytes,StandardOpenOption.CREATE,StandardOpenOption.APPEND);pairs.incrementAndGet()\n    } else dropped.incrementAndGet()',
       'Files.write(Paths.get(path),bytes,StandardOpenOption.CREATE,StandardOpenOption.APPEND);pairs.incrementAndGet();true\n    } else {dropped.incrementAndGet();false}')]
    for a,b in changes:
        assert text.count(a)==1, 'unexpected_source_anchor'
        text=text.replace(a,b)
    marker='  def clockNanos():Long = System.nanoTime()'
    assert text.count(marker)==1
    addition='''  def completeSnapshot(m:JMap[String,Object]):Boolean = {
    m.get("plan_truncated")==java.lang.Boolean.FALSE &&
      (m.get("remaining_physical_plan") match {
        case s:String => s.nonEmpty && s.length<=24000
        case _ => false
      })
  }
  def auditedReturn(path:String,m:JMap[String,Object],stock:Cost,desired:Cost):Cost = {
    try {if(emit(path,m)) desired else stock}
    catch {case NonFatal(e)=>errors.incrementAndGet();stock}
  }
'''
    text=text.replace(marker,addition+marker)
    source=out/'NativeBoundaryObserverV6.scala';source.write_text(text)
    tests=out/'ObserverSafetyChecks.scala'
    tests.write_text(r'''package org.apache.spark.sql.execution.adaptive
import java.nio.file.{Files,Paths}
import java.util.{LinkedHashMap,Map => JMap}
object ObserverSafetyChecks {
  private var checks=0
  private def check(v:Boolean):Unit={assert(v);checks+=1}
  def main(args:Array[String]):Unit={
    val root=Paths.get(args(0)).toRealPath()
    require(root.startsWith(Paths.get(System.getProperty("user.home"),"JevResearch").toRealPath()))
    def snapshot(s:String,truncated:java.lang.Boolean):JMap[String,Object]={
      val m=new LinkedHashMap[String,Object]()
      m.put("remaining_physical_plan",s);m.put("plan_truncated",truncated);m
    }
    val yes=java.lang.Boolean.TRUE;val no=java.lang.Boolean.FALSE
    check(NativeBoundaryObserverV6.completeSnapshot(snapshot("SortMergeJoin",no)))
    check(!NativeBoundaryObserverV6.completeSnapshot(snapshot("SortMergeJoin",yes)))
    check(!NativeBoundaryObserverV6.completeSnapshot(snapshot("SortMergeJoin",null)))
    check(!NativeBoundaryObserverV6.completeSnapshot(snapshot("",no)))
    check(!NativeBoundaryObserverV6.completeSnapshot(snapshot("x"*24001,no)))
    check(NativeBoundaryObserverV6.completeSnapshot(snapshot("x"*24000,no)))
    val prefix="x"*24000;val a=prefix+"Broadcast";val b=prefix+"SortMerge"
    check(a!=b && a.take(24000)==b.take(24000))
    check(!NativeBoundaryObserverV6.completeSnapshot(snapshot(a.take(24000),yes)))
    check(!NativeBoundaryObserverV6.completeSnapshot(snapshot(b.take(24000),yes)))
    val stock=SimpleCost(1);val desired=SimpleCost(2)
    val event=new LinkedHashMap[String,Object]();event.put("kind","test_only")
    val good=root.resolve("audit.jsonl").toString
    NativeBoundaryObserverV6.pairs.set(0)
    check(NativeBoundaryObserverV6.auditedReturn(good,event,stock,desired)==desired)
    check(Files.size(Paths.get(good))>0)
    val previous=Files.size(Paths.get(good))
    NativeBoundaryObserverV6.pairs.set(1000)
    check(NativeBoundaryObserverV6.auditedReturn(good,event,stock,desired)==stock)
    check(Files.size(Paths.get(good))==previous)
    NativeBoundaryObserverV6.pairs.set(0)
    event.put("too_large","x"*131073)
    check(NativeBoundaryObserverV6.auditedReturn(good,event,stock,desired)==stock)
    event.remove("too_large")
    val missing=root.resolve("missing_directory").resolve("audit.jsonl").toString
    check(NativeBoundaryObserverV6.auditedReturn(missing,event,stock,desired)==stock)
    check(NativeBoundaryObserverV6.errors.get()>0)
    println("{\"checks_passed\":"+checks+",\"status\":\"succeeded\",\"spark_queries\":0,\"api_requests\":0}")
  }
}
''')
    classes=out/'classes';classes.mkdir();jars=Path(pyspark.__file__).parent/'jars'
    jar=out/'guarded-observer.jar'
    commands=[['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(source),str(tests)],
      ['jar','--create','--file',str(jar),'-C',str(classes),'.'],
      ['java','-Xmx512m','-cp',str(classes)+':'+str(jars/'*'),'org.apache.spark.sql.execution.adaptive.ObserverSafetyChecks',str(out)]]
    results=[]
    for i,cmd in enumerate(commands):
        p=subprocess.run(cmd,cwd=HERE,capture_output=True,text=True,timeout=90)
        rec={'step':i,'returncode':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
        (out/f'step_{i}.json').write_text(json.dumps(rec,indent=2));results.append(rec)
        print(json.dumps(rec),flush=True)
        if p.returncode:raise RuntimeError('guard_build_or_test_failed')
    summary={'status':'succeeded','original_source_unchanged':OLD.read_bytes()==original,
      'original_sha256':EXPECTED,'guarded_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
      'jvm_tests':json.loads(results[-1]['stdout'].strip()),'budget_before':before,'budget_after':Budget().count(),
      'scope':'native-compiled guard and fault tests; not yet an end-to-end Spark intervention or full runtime-state validation'}
    (out/'COMPLETED.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
if __name__=='__main__':main()
