package org.apache.spark.sql.execution.adaptive

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.net.{HttpURLConnection, URI}
import java.util.{ArrayList, LinkedHashMap, Map => JMap}
import java.util.concurrent.atomic.AtomicLong
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.mutable
import scala.util.control.NonFatal
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/** Version-pinned, observation-only adapter. All returned costs are stock costs.
 * Optional loopback callback waits for a SHADOW recommendation; it never applies it.
 * Source/installed bytecode call sites are verified by preflight.py before use.
 */
class NativeBoundaryObserverV2(conf: SparkConf) extends CostEvaluator {
  require(org.apache.spark.SPARK_VERSION == "4.0.1", "unverified Spark version")
  private val delegate = SimpleCostEvaluator(SQLConf.get.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))
  private val destination = Paths.get(conf.get("spark.research.observer.path")).toAbsolutePath.normalize
  private val workspace = Paths.get(System.getProperty("user.home"), "JevResearch").toRealPath()
  require(destination.getParent.toRealPath().startsWith(workspace), "outside research workspace")
  require(!Files.isSymbolicLink(destination), "symlink trace rejected")
  private val endpoint = conf.getOption("spark.research.shadow.endpoint")
  endpoint.foreach(x => require(x.matches("http://127\\.0\\.0\\.1:[0-9]{1,5}/shadow/[a-zA-Z0-9_-]+"), "loopback endpoint required"))
  private val pending = mutable.Map.empty[Long, (SparkPlan, Cost, JMap[String,Object], String)]
  private val instance = java.util.UUID.randomUUID().toString
  private def obj(): JMap[String,Object] = new LinkedHashMap[String,Object]()
  private def stageSnapshot(q: QueryStageExec): JMap[String,Object] = {
    val m=obj(); m.put("stage_id",Int.box(q.id)); m.put("node",q.nodeName)
    m.put("materialized",Boolean.box(q.isMaterialized))
    m.put("size_in_bytes",null); m.put("row_count",null); m.put("is_runtime",null)
    if(q.isMaterialized) q.computeStats().foreach { s =>
      m.put("size_in_bytes",s.sizeInBytes.bigInteger)
      m.put("row_count",s.rowCount.map(_.bigInteger).orNull)
      m.put("is_runtime",Boolean.box(s.isRuntime))
    }
    q match {
      case s: ShuffleQueryStageExec if s.isMaterialized => s.mapStats.foreach { stats =>
        val sizes=stats.bytesByPartitionId; val xs=new ArrayList[java.lang.Long]()
        sizes.foreach(x=>xs.add(Long.box(x)))
        m.put("shuffle_serialized_partition_bytes",xs); m.put("partition_count",Int.box(sizes.length))
        m.put("nonempty_partitions",Int.box(sizes.count(_>0)));m.put("partition_bytes_total",Long.box(sizes.sum))
        if(sizes.nonEmpty) {
          val a=sizes.sorted
          m.put("partition_bytes_max",Long.box(a.last))
          m.put("partition_bytes_p50",Long.box(a((a.length-1)/2)))
          m.put("partition_bytes_p95",Long.box(a(math.ceil(0.95*a.length).toInt-1)))
        }
      }
      case _ =>
    }
    m
  }
  private def snapshot(plan: SparkPlan, cost: Cost, site: StackTraceElement): JMap[String,Object] = {
    val m=obj();m.put("captured_epoch_ms",Long.box(System.currentTimeMillis()))
    m.put("captured_nano_time",Long.box(System.nanoTime()));m.put("call_site",site.toString)
    m.put("source_line",Int.box(site.getLineNumber));val text=plan.treeString
    m.put("remaining_physical_plan",text.take(24000));m.put("plan_truncated",Boolean.box(text.length>24000))
    m.put("stock_cost",cost.toString)
    cost match {case SimpleCost(x)=>m.put("stock_cost_value",Long.box(x));case _=>m.put("stock_cost_value",null)}
    val stages=new ArrayList[JMap[String,Object]]()
    plan.collect {case q: QueryStageExec=>q}.groupBy(_.id).toSeq.sortBy(_._1).foreach {case (_,qs)=>stages.add(stageSnapshot(qs.head))}
    m.put("stages",stages);m.put("stage_scope","visible query-stage leaves, not a full history of all completed work")
    m
  }
  private def shadow(record:JMap[String,Object]): Unit = endpoint.foreach { url =>
    val bytes=NativeBoundaryObserverV2.mapper.writeValueAsBytes(record)
    if(bytes.length<=131072) {
      val c=URI.create(url).toURL.openConnection().asInstanceOf[HttpURLConnection]
      try {
        c.setConnectTimeout(2000);c.setReadTimeout(20000);c.setInstanceFollowRedirects(false)
        c.setRequestMethod("POST");c.setDoOutput(true);c.setRequestProperty("Content-Type","application/json")
        c.setFixedLengthStreamingMode(bytes.length)
        val os=c.getOutputStream;try os.write(bytes) finally os.close()
        record.put("shadow_http_status",Int.box(c.getResponseCode))
        if(c.getResponseCode==200) {val in=c.getInputStream;try in.readNBytes(8192) finally in.close()}
      } finally c.disconnect()
    }
  }
  override def evaluateCost(plan: SparkPlan): Cost = synchronized {
    val stock=delegate.evaluateCost(plan);val begin=System.nanoTime()
    NativeBoundaryObserverV2.calls.incrementAndGet()
    try {
      val tag=SQLConf.get.getConfString("spark.research.run_id","unlabeled")
      val tid=Thread.currentThread.getId
      val site=Thread.currentThread.getStackTrace.find(s => s.getClassName=="org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec" && s.getFileName=="AdaptiveSparkPlanExec.scala" && Set(365,366).contains(s.getLineNumber))
      site match {
        case Some(s) if s.getLineNumber==365 =>
          if(pending.contains(tid)) NativeBoundaryObserverV2.errors.incrementAndGet()
          pending(tid)=(plan,stock,snapshot(plan,stock,s),tag)
        case Some(s) if s.getLineNumber==366 =>
          pending.remove(tid) match {
            case Some((current,oldCost,oldSnapshot,oldTag)) if oldTag==tag =>
              val record=obj();record.put("kind","native_aqe_cost_boundary_pair_v2")
              record.put("spark_version","4.0.1");record.put("release_commit","29434ea766b0fc3c3bf6eaadb43a8f931133649e")
              record.put("observer_instance",instance);record.put("thread_id",Long.box(tid));record.put("run_id",tag)
              record.put("current",oldSnapshot);record.put("proposed",snapshot(plan,stock,s))
              record.put("plans_equal",Boolean.box(current==plan))
              record.put("native_would_choose",if(stock<oldCost || (stock==oldCost && current!=plan)) "PROPOSED" else "CURRENT")
              record.put("cost_returned_unchanged",Boolean.box(true));record.put("intervention",Boolean.box(false))
              val runtime=Runtime.getRuntime
              val resources=obj();resources.put("jvm_heap_max_bytes",Long.box(runtime.maxMemory()))
              resources.put("jvm_heap_committed_bytes",Long.box(runtime.totalMemory()))
              resources.put("jvm_heap_free_bytes",Long.box(runtime.freeMemory()))
              resources.put("captured_epoch_ms",Long.box(System.currentTimeMillis()))
              record.put("resources",resources)
              if(current!=plan) {
                try shadow(record) catch {case NonFatal(e)=>record.put("shadow_error_type",e.getClass.getSimpleName)}
              }
              NativeBoundaryObserverV2.emit(destination.toString,record)
            case _ => NativeBoundaryObserverV2.errors.incrementAndGet()
          }
        case _ => NativeBoundaryObserverV2.unmatched.incrementAndGet()
      }
    } catch {case NonFatal(e)=>NativeBoundaryObserverV2.errors.incrementAndGet()}
    finally NativeBoundaryObserverV2.observerNanos.addAndGet(System.nanoTime()-begin)
    stock
  }
}
object NativeBoundaryObserverV2 {
  val mapper=new ObjectMapper()
  val calls=new AtomicLong(0);val pairs=new AtomicLong(0);val errors=new AtomicLong(0)
  val unmatched=new AtomicLong(0);val dropped=new AtomicLong(0);val observerNanos=new AtomicLong(0)
  def metrics():String = {
    val m=new LinkedHashMap[String,Object]()
    m.put("calls",Long.box(calls.get()));m.put("pairs",Long.box(pairs.get()));m.put("errors",Long.box(errors.get()))
    m.put("unmatched",Long.box(unmatched.get()));m.put("dropped",Long.box(dropped.get()))
    m.put("observer_nanos",Long.box(observerNanos.get()));mapper.writeValueAsString(m)
  }
  def emit(path:String,value:JMap[String,Object]):Unit = synchronized {
    val bytes=(mapper.writeValueAsString(value)+"\n").getBytes(StandardCharsets.UTF_8)
    if(pairs.get()<1000 && bytes.length<=131072) {
      Files.write(Paths.get(path),bytes,StandardOpenOption.CREATE,StandardOpenOption.APPEND);pairs.incrementAndGet()
    } else dropped.incrementAndGet()
  }
}
