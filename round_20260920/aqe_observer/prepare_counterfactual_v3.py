"""Create a separate, reviewable v3 Scala source; do not execute it or alter v2.
The only possible override is one CURRENT decision at an exact preregistered state hash.
"""
from pathlib import Path
import hashlib,json
ROOT=Path(__file__).resolve().parent
source=ROOT/'NativeBoundaryObserverV2.scala'
assert hashlib.sha256(source.read_bytes()).hexdigest()=='d482cf6005dde7bb76c8ad8ae5382fe13fed6f64d2a823d844ad13033a92973c'
s=source.read_text().replace('NativeBoundaryObserverV2','NativeBoundaryObserverV3')
s=s.replace('native_aqe_cost_boundary_pair_v2','aqe_counterfactual_pair_v3')
s=s.replace('/** Version-pinned, observation-only adapter. All returned costs are stock costs.\n * Optional loopback callback waits for a SHADOW recommendation; it never applies it.\n * Source/installed bytecode call sites are verified by preflight.py before use.\n */',
 '''/** Version-pinned synthetic counterfactual experiment, NOT a Jev integration.
 * Default: stock costs. Only an exact state-signature match can retain CURRENT,
 * once per query. PROPOSED and CONTROL preserve the native decision at that state.
 * No network callbacks. Intended only for this disposable research session.
 */''')
start=s.index('  private val endpoint =')
end=s.index('  private val pending =',start)
s=s[:start]+s[end:]
start=s.index('  private def shadow(')
end=s.index('  override def evaluateCost',start)
signature=r'''  private def stateSignature(current:JMap[String,Object], proposed:JMap[String,Object]):String = {
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
'''
s=s[:start]+signature+s[end:]
needle='    val stock=delegate.evaluateCost(plan);val begin=System.nanoTime()'
assert s.count(needle)==1
s=s.replace(needle,needle+';var returned:Cost=stock')
old='''              if(current!=plan) {
                try shadow(record) catch {case NonFatal(e)=>record.put("shadow_error_type",e.getClass.getSimpleName)}
              }
'''
assert s.count(old)==1
new='''              val proposedSnapshot=record.get("proposed").asInstanceOf[JMap[String,Object]]
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
'''
s=s.replace(old,new)
old='    } catch {case NonFatal(e)=>NativeBoundaryObserverV3.errors.incrementAndGet()}'
assert s.count(old)==1
s=s.replace(old,'    } catch {case NonFatal(e)=>returned=stock;NativeBoundaryObserverV3.errors.incrementAndGet()}')
old='\n    stock\n  }\n}'
assert s.count(old)==1
s=s.replace(old,'\n    returned\n  }\n}')
needle='object NativeBoundaryObserverV3 {\n'
assert s.count(needle)==1
s=s.replace(needle,needle+'''  val canonicalMapper=new ObjectMapper().configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,true)
  private val claimed=mutable.Set.empty[String]
  def claim(tag:String):Boolean = synchronized {if(claimed.contains(tag)) false else {claimed.add(tag);true}}
  def clockNanos():Long = System.nanoTime()
''')
assert 'shadow(record)' not in s and 'getResponseCode' not in s
out=ROOT/'NativeBoundaryObserverV3.scala'
with out.open('x') as f:f.write(s)
print(json.dumps({'created':out.name,'source_sha256':hashlib.sha256(s.encode()).hexdigest(),'compiled':False,'executed':False,'source_v2_unchanged':True}))
