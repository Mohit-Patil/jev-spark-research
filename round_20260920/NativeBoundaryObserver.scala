package org.apache.spark.sql.execution.adaptive

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.{ArrayList, LinkedHashMap, Map => JMap}
import java.util.concurrent.atomic.AtomicInteger
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.mutable
import scala.util.control.NonFatal
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/** Research-only observer for the verified Spark 4.0.1 cost call sites.
  * Returns the stock cost unchanged. Makes NO network calls and cannot adopt plans.
  */
class NativeBoundaryObserver(conf: SparkConf) extends CostEvaluator {
  private val delegate = SimpleCostEvaluator(
    SQLConf.get.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))
  private val destination = Paths.get(conf.get("spark.research.observer.path")).toAbsolutePath.normalize
  private val workspace = Paths.get(System.getProperty("user.home"), "JevResearch").toRealPath()
  require(destination.getParent.toRealPath().startsWith(workspace), "observer path outside workspace")
  private val pending = mutable.Map.empty[Long, (SparkPlan, Cost, JMap[String, Object])]
  private val instance = java.util.UUID.randomUUID().toString

  private def obj(): JMap[String,Object] = new LinkedHashMap[String,Object]()
  private def stageSnapshot(q: QueryStageExec): JMap[String,Object] = {
    val m=obj();m.put("stage_id",Int.box(q.id));m.put("node",q.nodeName)
    m.put("materialized",Boolean.box(q.isMaterialized))
    q.computeStats().foreach { s =>
      m.put("size_in_bytes",s.sizeInBytes.bigInteger)
      m.put("row_count",s.rowCount.map(_.bigInteger).orNull)
      m.put("is_runtime",Boolean.box(s.isRuntime))
    }
    q match {
      case s: ShuffleQueryStageExec if s.isMaterialized =>
        s.mapStats.foreach { stats =>
          val sizes=stats.bytesByPartitionId;val xs=new ArrayList[java.lang.Long]()
          sizes.foreach(x=>xs.add(Long.box(x)))
          m.put("shuffle_serialized_partition_bytes",xs)
          m.put("partition_count",Int.box(sizes.length))
          m.put("nonempty_partitions",Int.box(sizes.count(_>0)))
          m.put("partition_bytes_total",Long.box(sizes.sum))
          if(sizes.nonEmpty) {
            val sorted=sizes.sorted
            m.put("partition_bytes_max",Long.box(sorted.last))
            m.put("partition_bytes_p50",Long.box(sorted((sorted.length-1)/2)))
            m.put("partition_bytes_p95",Long.box(sorted(math.min(sorted.length-1,math.ceil(0.95*sorted.length).toInt-1))))
          }
        }
      case _ =>
    }
    m
  }
  private def snapshot(plan: SparkPlan, cost: Cost, site: StackTraceElement): JMap[String,Object] = {
    val m=obj();m.put("captured_epoch_ms",Long.box(System.currentTimeMillis()))
    m.put("captured_nano_time",Long.box(System.nanoTime()))
    m.put("call_site",site.toString);m.put("source_line",Int.box(site.getLineNumber))
    m.put("remaining_physical_plan",plan.treeString)
    m.put("stock_cost",cost.toString)
    val stages=new ArrayList[JMap[String,Object]]()
    plan.collect {case q: QueryStageExec if q.isMaterialized =>q}
      .groupBy(_.id).toSeq.sortBy(_._1).foreach {case (_,qs)=>stages.add(stageSnapshot(qs.head))}
    m.put("materialized_stages",stages);m
  }
  override def evaluateCost(plan: SparkPlan): Cost = synchronized {
    val stockCost=delegate.evaluateCost(plan)
    try {
      val site=Thread.currentThread.getStackTrace.find(s =>
        s.getFileName=="AdaptiveSparkPlanExec.scala" && Set(365,366).contains(s.getLineNumber))
      site match {
        case Some(s) if s.getLineNumber==365 =>
          pending(Thread.currentThread.getId)=(plan,stockCost,snapshot(plan,stockCost,s))
        case Some(s) if s.getLineNumber==366 =>
          pending.remove(Thread.currentThread.getId).foreach { case (current,currentCost,currentSnapshot) =>
            val record=obj();record.put("kind","native_aqe_cost_boundary_pair")
            record.put("spark_version","4.0.1");record.put("release_commit","29434ea766b0fc3c3bf6eaadb43a8f931133649e")
            record.put("observer_instance",instance);record.put("thread_id",Long.box(Thread.currentThread.getId))
            record.put("current",currentSnapshot);record.put("proposed",snapshot(plan,stockCost,s))
            record.put("native_would_choose",if(stockCost<currentCost || (stockCost==currentCost && current!=plan)) "PROPOSED" else "CURRENT")
            record.put("cost_returned_unchanged",Boolean.box(true));record.put("intervention",Boolean.box(false))
            NativeBoundaryObserver.emit(destination.toString,record)
          }
        case _ =>
          val record=obj();record.put("kind","unmatched_cost_call_site")
          record.put("stack",Thread.currentThread.getStackTrace.filter(_.getClassName.contains("AdaptiveSparkPlanExec")).map(_.toString).mkString("\n"))
          NativeBoundaryObserver.emit(destination.toString,record)
      }
    } catch {case NonFatal(e)=>System.err.println("Research observer snapshot error: "+e.getClass.getSimpleName)}
    stockCost
  }
}
object NativeBoundaryObserver {
  private val count=new AtomicInteger(0)
  private val mapper=new ObjectMapper()
  def emit(path:String,value:JMap[String,Object]):Unit = synchronized {
    if(count.incrementAndGet()<=100) {
      val bytes=(mapper.writeValueAsString(value)+"\n").getBytes(StandardCharsets.UTF_8)
      if(bytes.length<=131072) Files.write(Paths.get(path),bytes,StandardOpenOption.CREATE,StandardOpenOption.APPEND)
    }
  }
}
