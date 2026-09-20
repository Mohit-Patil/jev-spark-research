"""Create a separately versioned bounded advisor hook; do not mutate frozen V4."""
from pathlib import Path
import hashlib,json,subprocess
import pyspark
R=Path(__file__).resolve().parent
s=(R/'RuntimeCandidateExtension.scala').read_text()
assert hashlib.sha256(s.encode()).hexdigest()=='5d93c4096c5d8c6506912cd8c35db2f10fbecdcd59ca3e67ea71d8d4322e631e'
s=s.replace('RuntimeCandidateExtension','RuntimeCandidateAdviceExtension').replace('RuntimeCandidateRule','RuntimeCandidateAdviceRule').replace('RuntimeCandidateEvents','RuntimeCandidateAdviceEvents').replace('spark.research.v4.','spark.research.v5.')
def rep(a,b):
    global s
    assert s.count(a)==1,a
    s=s.replace(a,b)
rep('Set("SHADOW","APPLY")','Set("SHADOW","APPLY","JEV")')
rep('var changed=false','var changed=false\n      var didApply=false')
rep('event.put("rule_selected",Boolean.box(mode=="APPLY"))',
    'val advice=if(mode=="JEV") RuntimeCandidateAdviceEvents.advice(event) else "NOT_REQUESTED"\n                val select=mode=="APPLY" || (mode=="JEV" && advice=="BROADCAST")\n                didApply=select\n                event.put("advice",advice)\n                event.put("rule_selected",Boolean.box(select))')
rep('if(mode=="APPLY") prepared else j','if(select) prepared else j')
rep('val finalPlan=if(mode=="APPLY") EnsureRequirements().apply(result) else input','val finalPlan=if(didApply) EnsureRequirements().apply(result) else input')
rep('if(mode=="APPLY") {','if(mode=="APPLY" || mode=="JEV") {')
http='''  /** Only a bounded loopback service; no remote endpoint or credential in the JVM. */
  def advice(event:JMap[String,Object]):String = {
    val started=System.nanoTime()
    var conn:java.net.HttpURLConnection=null
    try {
      val uri=new java.net.URI(SQLConf.get.getConfString("spark.research.v5.callback",""))
      require(uri.getScheme=="http" && uri.getHost=="127.0.0.1" && uri.getPort>0 &&
        uri.getUserInfo==null && uri.getQuery==null && uri.getFragment==null && uri.getPath.startsWith("/r/"),"invalid callback")
      val bytes=mapper.writeValueAsBytes(event);require(bytes.length<=32768,"callback input bound")
      conn=uri.toURL.openConnection().asInstanceOf[java.net.HttpURLConnection]
      conn.setConnectTimeout(200);conn.setReadTimeout(2200);conn.setRequestMethod("POST")
      conn.setInstanceFollowRedirects(false);conn.setDoOutput(true)
      conn.setRequestProperty("Content-Type","application/json");conn.setFixedLengthStreamingMode(bytes.length)
      val os=conn.getOutputStream;try os.write(bytes) finally os.close()
      require(conn.getResponseCode==200,"callback status")
      val is=conn.getInputStream
      val raw=try is.readNBytes(2049) finally is.close()
      require(raw.length<=2048,"callback response bound")
      val choice=mapper.readTree(raw).get("choice").asText()
      require(Set("NATIVE","BROADCAST","ABSTAIN").contains(choice),"invalid callback choice")
      choice
    } catch {case NonFatal(e)=>event.put("callback_error",e.getClass.getSimpleName);"ABSTAIN"}
    finally {
      if(conn!=null)conn.disconnect()
      event.put("advice_wall_nanos",Long.box(System.nanoTime()-started))
    }
  }
'''
rep('  def record(m:JMap[String,Object]):Unit = synchronized {',http+'  def record(m:JMap[String,Object]):Unit = synchronized {')
p=R/'RuntimeCandidateAdviceExtension.scala'
with p.open('x') as f:f.write(s)
out=R/'advisor_build_v5';out.mkdir(exist_ok=False);classes=out/'classes';classes.mkdir()
jars=Path(pyspark.__file__).parent/'jars';jar=out/'advisor.jar'
for i,args in enumerate([['java','-Xmx768m','-cp',str(jars/'*'),'scala.tools.nsc.Main','-usejavacp','-d',str(classes),str(p)],['jar','--create','--file',str(jar),'-C',str(classes),'.']]):
    x=subprocess.run(args,capture_output=True,text=True,timeout=90)
    rec={'command':args,'returncode':x.returncode,'stdout':x.stdout,'stderr':x.stderr}
    (out/f'compile_{i}.json').write_text(json.dumps(rec,indent=2));print(json.dumps(rec),flush=True)
    if x.returncode:raise SystemExit(1)
manifest={'status':'succeeded','source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'jar_sha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'new_api_calls':0,'scope':'compiled only; runtime tests required'}
(out/'COMPLETED.json').write_text(json.dumps(manifest,indent=2));print(json.dumps(manifest),flush=True)
