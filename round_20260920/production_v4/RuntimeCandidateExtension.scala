package org.apache.spark.sql.execution.adaptive

import com.fasterxml.jackson.databind.ObjectMapper
import java.util.{LinkedHashMap, ArrayList, Map => JMap}
import scala.collection.mutable
import scala.util.control.NonFatal
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, SortExec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

/** Disposable Spark 4.0.1-only experiment. No network or secret access.
 * Construct a broadcast-right alternative only from BOTH materialized shuffle
 * leaves. Preserve those exact objects, join keys, type, condition and outputs.
 * OFF is stock; SHADOW enumerates but returns stock; APPLY tests a fixed rule.
 * This is candidate-generation research, not a trained or Jev-based policy.
 */
class RuntimeCandidateExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectQueryStagePrepRule((s: SparkSession) => new RuntimeCandidateRule(s))
  }
}
class RuntimeCandidateRule(session: SparkSession) extends Rule[SparkPlan] {
  private val used = mutable.LinkedHashSet.empty[String]
  private def leaf(p:SparkPlan):Option[(SparkPlan,ShuffleQueryStageExec)] = p match {
    case s:SortExec if !s.global => leaf(s.child)
    case q:ShuffleQueryStageExec => Some((q,q))
    case r:AQEShuffleReadExec => r.child match {
      case q:ShuffleQueryStageExec => Some((r,q))
      case _ => None
    }
    case _ => None
  }
  private def snap(q:ShuffleQueryStageExec):JMap[String,Object] = {
    val m=new LinkedHashMap[String,Object]()
    m.put("stage_id",Int.box(q.id));m.put("object_id",Int.box(System.identityHashCode(q)))
    m.put("materialized",Boolean.box(q.isMaterialized))
    val s=q.computeStats().get
    m.put("rows",s.rowCount.map(_.bigInteger).orNull);m.put("runtime_size_bytes",s.sizeInBytes.bigInteger)
    m.put("is_runtime",Boolean.box(s.isRuntime))
    q.mapStats.foreach { a =>
      val xs=new ArrayList[java.lang.Long]();a.bytesByPartitionId.foreach(x=>xs.add(Long.box(x)))
      m.put("serialized_partition_bytes",xs);m.put("shuffle_id",Int.box(a.shuffleId))
    }
    m
  }
  override def apply(input:SparkPlan):SparkPlan = {
    val mode=SQLConf.get.getConfString("spark.research.v4.mode","OFF")
    if(mode=="OFF") return input
    val started=System.nanoTime()
    try {
      require(org.apache.spark.SPARK_VERSION=="4.0.1","unsupported version")
      require(Set("SHADOW","APPLY").contains(mode),"unsupported mode")
      val tag=SQLConf.get.getConfString("spark.research.v4.run_id","unlabeled")
      if(tag=="unlabeled" || used.contains(tag)) return input
      var changed=false
      var saved:JMap[String,Object]=null
      val result=input.transformUp {
        case j:SortMergeJoinExec if !changed && !j.isSkewJoin && j.condition.isEmpty &&
          (j.joinType==Inner || j.joinType==LeftOuter) && j.leftKeys.size==1 &&
          j.leftKeys.head.dataType==LongType && j.rightKeys.head.dataType==LongType =>
          (leaf(j.left),leaf(j.right)) match {
            case (Some((l,lq)),Some((r,rq))) if lq.isMaterialized && rq.isMaterialized =>
              val rs=rq.computeStats().get
              val allowed=rs.rowCount.exists(n=>n>0 && n<=1250000) &&
                rs.sizeInBytes>0 && rs.sizeInBytes<=33554432 && Runtime.getRuntime.maxMemory()>=1073741824L
              if(!allowed) j else {
                val b=BroadcastHashJoinExec(j.leftKeys,j.rightKeys,j.joinType,BuildRight,j.condition,l,r)
                b.copyTagsFrom(j)
                val prepared=EnsureRequirements().apply(b)
                require(prepared.output==j.output,"output contract changed")
                require(ValidateRequirements.validate(prepared),"invalid distribution")
                val qs=prepared.collect{case q:ShuffleQueryStageExec=>q}
                require(qs.exists(_ eq lq) && qs.exists(_ eq rq),"materialized inputs not preserved")
                val event=new LinkedHashMap[String,Object]()
                event.put("kind","materialized_broadcast_right_candidate_v4")
                event.put("run_id",tag);event.put("mode",mode)
                event.put("epoch_ms",Long.box(System.currentTimeMillis()))
                event.put("decision_nano_time",Long.box(System.nanoTime()))
                event.put("left",snap(lq));event.put("right",snap(rq))
                event.put("original_join",j.treeString);event.put("candidate_join",prepared.treeString)
                event.put("exact_stage_objects_reused",Boolean.box(true))
                event.put("output_and_distribution_validated",Boolean.box(true))
                event.put("additional_broadcast_exchange",Boolean.box(true))
                event.put("rule_selected",Boolean.box(mode=="APPLY"))
                saved=event;changed=true
                if(mode=="APPLY") prepared else j
              }
            case _ => j
          }
      }
      if(changed) {
        val finalPlan=if(mode=="APPLY") EnsureRequirements().apply(result) else input
        require(finalPlan.output==input.output,"root output changed")
        require(ValidateRequirements.validate(finalPlan),"root distribution invalid")
        if(mode=="APPLY") {
          used.add(tag);if(used.size>128)used.remove(used.head)
        }
        RuntimeCandidateEvents.record(saved)
        finalPlan
      } else input
    } catch {
      case NonFatal(e) => RuntimeCandidateEvents.error(e.getClass.getSimpleName);input
    } finally {RuntimeCandidateEvents.addTime(System.nanoTime()-started)}
  }
}
object RuntimeCandidateEvents {
  private val events=mutable.ArrayBuffer.empty[String]
  private val errors=mutable.ArrayBuffer.empty[String]
  private val mapper=new ObjectMapper()
  private var nanos=0L
  def record(m:JMap[String,Object]):Unit = synchronized {
    val s=mapper.writeValueAsString(m)
    if(events.size<1000 && s.length<65536)events+=s else errors+="event_limit"
  }
  def error(s:String):Unit=synchronized{if(errors.size<100)errors+=s}
  def addTime(n:Long):Unit=synchronized{nanos+=n}
  def clockNanos():Long=System.nanoTime()
  def state():String=synchronized{
    "{\"events\":["+events.mkString(",")+"],\"errors\":"+
      mapper.writeValueAsString(errors.mkString(","))+",\"self_nanos\":"+nanos+"}"
  }
}
