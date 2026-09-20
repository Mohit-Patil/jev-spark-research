package org.apache.spark.sql.execution.adaptive
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
