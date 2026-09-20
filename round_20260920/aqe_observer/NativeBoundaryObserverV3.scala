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

/** Version-pinned synthetic counterfactual experiment, NOT a Jev integration.
 * Default: stock costs. Only an exact state-signature match can retain CURRENT,
 * once per query. PROPOSED and CONTROL preserve the native decision at that state.
 * No network callbacks. Intended only for this disposable research session.
 */
class NativeBoundaryObserverV3(conf: SparkConf) extends CostEvaluator {
  require(org.apache.spark.SPARK_VERSION == "4.0.1", "unverified Spark version")
  private val delegate = SimpleCostEvaluator(SQLConf.get.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))
  private val destination = Paths.get(conf.get("spark.research.observer.path")).toAbsolutePath.normalize
  private val workspace = Paths.get(System.getProperty("user.home"), "JevResearch").toRealPath()
  require(destination.getParent.toRealPath().startsWith(workspace), "outside research workspace")
  require(!Files.isSymbolicLink(destination), "symlink trace rejected")
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
  private def stateSignature(current:JMap[String,Object], proposed:JMap[String,Object]):String = {
    def norm(x:String):String = x.replaceAll("#[0-9]+L?","#_")
      .replaceAll("\\[plan_id=[0-9]+\\]","[plan_id=_]")
      .replaceAll("\\*\\([0-9]+\\)","*(_)")
    val state=obj()
    Seq(("current",current),("proposed",proposed)).foreach {case (key,snapshot) =>
      val part=obj();part.put("plan",norm(snapshot.get("remaining_physical_plan").asInstanceOf[String]))
      part.put("stages",snapshot.get("stages"));state.put(key,part)
    }
    val bytes=NativeBoundaryObserverV3.canonicalMapper.writeValueAsBytes(state)
    java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
      .map(b => String.format("%02x",Int.box(b & 255))).mkString
  }
  override def evaluateCost(plan: SparkPlan): Cost = synchronized {
    val stock=delegate.evaluateCost(plan);val begin=System.nanoTime();var returned:Cost=stock
    NativeBoundaryObserverV3.calls.incrementAndGet()
    try {
      val tag=SQLConf.get.getConfString("spark.research.run_id","unlabeled")
      val tid=Thread.currentThread.getId
      val site=Thread.currentThread.getStackTrace.find(s => s.getClassName=="org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec" && s.getFileName=="AdaptiveSparkPlanExec.scala" && Set(365,366).contains(s.getLineNumber))
      site match {
        case Some(s) if s.getLineNumber==365 =>
          if(pending.contains(tid)) NativeBoundaryObserverV3.errors.incrementAndGet()
          pending(tid)=(plan,stock,snapshot(plan,stock,s),tag)
        case Some(s) if s.getLineNumber==366 =>
          pending.remove(tid) match {
            case Some((current,oldCost,oldSnapshot,oldTag)) if oldTag==tag =>
              val record=obj();record.put("kind","aqe_counterfactual_pair_v3")
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
              val proposedSnapshot=record.get("proposed").asInstanceOf[JMap[String,Object]]
              val sig=stateSignature(oldSnapshot,proposedSnapshot)
              val arm=SQLConf.get.getConfString("spark.research.arm","CONTROL")
              require(Set("CONTROL","CURRENT","PROPOSED").contains(arm),"invalid research arm")
              val target=SQLConf.get.getConfString("spark.research.target_signature","")
              val eligible=sig==target && record.get("native_would_choose")=="PROPOSED"
              val selected=eligible && NativeBoundaryObserverV3.claim(tag)
              record.put("state_signature",sig);record.put("arm",arm)
              record.put("target_matches",Boolean.box(eligible));record.put("target_selected_once",Boolean.box(selected))
              record.put("decision_entry_nano_time",Long.box(begin))
              if(selected && arm=="CURRENT") {
                oldCost match {
                  case SimpleCost(v) if v<Long.MaxValue => returned=SimpleCost(v+1)
                  case _ => throw new IllegalStateException("unsupported counterfactual cost")
                }
              }
              record.put("cost_returned_unchanged",Boolean.box(returned==stock))
              record.put("intervention",Boolean.box(returned!=stock))
              record.put("returned_cost",returned.toString)
              record.put("effective_choice",if(returned<oldCost || (returned==oldCost && current!=plan)) "PROPOSED" else "CURRENT")
              NativeBoundaryObserverV3.emit(destination.toString,record)
            case _ => NativeBoundaryObserverV3.errors.incrementAndGet()
          }
        case _ => NativeBoundaryObserverV3.unmatched.incrementAndGet()
      }
    } catch {case NonFatal(e)=>returned=stock;NativeBoundaryObserverV3.errors.incrementAndGet()}
    finally NativeBoundaryObserverV3.observerNanos.addAndGet(System.nanoTime()-begin)
    returned
  }
}
object NativeBoundaryObserverV3 {
  val canonicalMapper=new ObjectMapper().configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,true)
  private val claimed=mutable.Set.empty[String]
  def claim(tag:String):Boolean = synchronized {if(claimed.contains(tag)) false else {claimed.add(tag);true}}
  def clockNanos():Long = System.nanoTime()
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
