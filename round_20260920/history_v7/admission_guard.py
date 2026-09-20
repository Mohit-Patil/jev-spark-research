"""Conservative experimental cache admission screen and native Spark checks.
Uses UNFILTERED footer bytes, never the optimistic filter fraction as a safety
certificate. This deliberately rejects the selective-wide opportunity as well.
No new model calls, secret reads, dataset writes or global Spark changes.
"""
import copy,json,math,random,threading,time,unittest
from history_core import *
from cache_trial import CASES_CACHE,identity
from diagnose_broadcast import java_cause
MIB=1024*1024

def admit(advice,features,resources):
    decision={'selected':'NATIVE','accepted':False,'reason':'advice_not_broadcast'}
    if advice!='BROADCAST':return decision
    vals=[features.get('right',{}).get('column_uncompressed_bytes'),features.get('right',{}).get('file_rows'),
          resources.get('heap_max'),resources.get('heap_used')]
    if any(type(x) not in (int,float) or not math.isfinite(x) or x<0 for x in vals):
        return {**decision,'reason':'missing_or_invalid_observations'}
    encoded,rows,maximum,used=vals
    if maximum<=0 or used>maximum or encoded<=0 or rows<=0:
        return {**decision,'reason':'inconsistent_observations'}
    # Narrow experimental domain, not a peak-memory estimator or reservation.
    # Encoded Parquet metadata is not a proven upper bound on JVM hash size.
    if encoded>16*MIB or rows>100000:
        return {**decision,'reason':'unfiltered_build_outside_experimental_envelope'}
    headroom=maximum-used
    if headroom<512*MIB or 8*encoded>0.25*headroom:
        return {**decision,'reason':'insufficient_observed_heap_headroom'}
    return {'selected':'BROADCAST','accepted':True,'reason':'inside_conservative_experimental_envelope'}

class Tests(unittest.TestCase):
    def setUp(self):
        self.f={'right':{'column_uncompressed_bytes':1024*1024,'file_rows':1000},'estimated_build_encoded_bytes':1}
        self.r={'heap_max':2*1024**3,'heap_used':128*MIB}
    def test_small_complete_accepts(self):self.assertTrue(admit('BROADCAST',self.f,self.r)['accepted'])
    def test_large_unfiltered_rejected_even_tiny_estimate(self):
        self.f['right']['column_uncompressed_bytes']=160*MIB
        self.assertEqual(admit('BROADCAST',self.f,self.r)['selected'],'NATIVE')
    def test_missing_is_not_zero(self):
        self.f['right'].pop('column_uncompressed_bytes')
        self.assertFalse(admit('BROADCAST',self.f,self.r)['accepted'])
    def test_nonfinite_rejected(self):
        self.r['heap_used']=float('nan');self.assertFalse(admit('BROADCAST',self.f,self.r)['accepted'])
    def test_invalid_type_rejected(self):
        self.f['right']['file_rows']=True;self.assertFalse(admit('BROADCAST',self.f,self.r)['accepted'])
    def test_low_headroom_rejected(self):
        self.r['heap_used']=self.r['heap_max']-100*MIB;self.assertFalse(admit('BROADCAST',self.f,self.r)['accepted'])
    def test_rechecked_after_first_admission(self):
        self.assertTrue(admit('BROADCAST',self.f,self.r)['accepted']);self.r['heap_used']=self.r['heap_max']
        self.assertFalse(admit('BROADCAST',self.f,self.r)['accepted'])
    def test_unknown_choice_fallback(self):self.assertEqual(admit('INVENTED',self.f,self.r)['selected'],'NATIVE')
    def test_no_input_mutation(self):
        old=copy.deepcopy((self.f,self.r));admit('BROADCAST',self.f,self.r);self.assertEqual(old,(self.f,self.r))

def main():
    out=R/'admission_guard_v1';out.mkdir(exist_ok=False);before=AuthorizedBudget().count()
    t=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    dump(out/'TESTS.json',{'tests':t.testsRun,'passed':t.wasSuccessful(),'new_api_calls':0})
    if not t.wasSuccessful():raise ValueError('admission_tests_failed')
    c=CASES_CACHE['repeat_wide'];source=R/'cache_trial_v1'/'repeat_wide'
    stored=json.loads((source/'IMMUTABLE_DATASET.json').read_text());sig,_=identity(source/'data');assert sig==stored['identity']
    prior=rows(source/'decisions.jsonl')
    valid=[d for d in prior if d['record']['status']=='succeeded' and d['record']['choice']=='BROADCAST']
    assert valid
    advice_source=next(d for d in valid if d['record'].get('attempt') is not None)
    dump(out/'FROZEN.json',{'created_unix':time.time(),'source_sha256':sha(R/'admission_guard.py'),
      'source_advice_attempt':advice_source['record']['attempt'],'source_snapshot':sig,
      'repetitions':6,'policies':['NATIVE','GUARDED_CACHED_BROADCAST'],
      'purpose':'Safety regression on already exposed data; NOT an untouched performance evaluation.',
      'scope':'Source advice is replayed with identity verified, but gate is reapplied per query. No original failing result is overwritten.',
      'limits':'16 MiB unfiltered encoded bytes, 100000 rows, 512 MiB observed headroom. Heuristic research admission, not production memory guarantee.'})
    spark=corpus.session();spark.sparkContext.setLogLevel('ERROR');rr=[];want=oracle(c)
    try:
        spark.read.parquet(str(source/'data'/'fact')).createOrReplaceTempView('f')
        spark.read.parquet(str(source/'data'/'dim')).where(f'(k*17+{c["seed"]}) % 1000 < {c["keep"]}').createOrReplaceTempView('d')
        runtime=spark._jvm.java.lang.Runtime.getRuntime();rng=random.Random(7499)
        for rep in range(6):
            order=['NATIVE','GUARDED_CACHED_BROADCAST'];rng.shuffle(order)
            for policy in order:
                tag='guard7_'+str(rep)+'_'+policy;start=time.perf_counter()
                corpus.configure(spark,'NATIVE');decision={'selected':'NATIVE','reason':'native_control'}
                if policy!='NATIVE':
                    f=metadata_features(spark,c,source/'data')
                    resources={'heap_max':int(runtime.maxMemory()),'heap_used':int(runtime.totalMemory()-runtime.freeMemory())}
                    decision=admit(advice_source['record']['choice'],f,resources)
                    assert not decision['accepted']
                spark.sparkContext.setJobGroup(tag,'bounded cache-admission regression',interruptOnCancel=True)
                timer=threading.Timer(30,spark.sparkContext.cancelJobGroup,args=(tag,));timer.start()
                r={'run_id':tag,'rep':rep,'policy':policy,'decision':decision}
                try:
                    df=spark.sql(query(c,decision['selected']));got=sorted([list(x) for x in df.collect()])
                    r.update(status='succeeded' if got==want else 'failed',correct=got==want,result=got,to_result_s=time.perf_counter()-start,
                      final_plan=df._jdf.queryExecution().executedPlan().toString())
                except Exception as e:r.update(status='failed',error_type=type(e).__name__,java_causes=java_cause(e),elapsed_s=time.perf_counter()-start)
                finally:timer.cancel();timer.join();spark.sparkContext.setLocalProperty('spark.jobGroup.id',None)
                append(out/'raw.jsonl',r);rr.append(r)
                if r['status']!='succeeded':raise RuntimeError('guarded_query_failed_preserved')
        summary={'status':'succeeded','queries':len(rr),'all_correct':all(r['correct'] for r in rr),'guard_vetoes':sum(r['policy']!='NATIVE' and r['decision']['selected']=='NATIVE' for r in rr),
          'new_api_calls':0,'budget_before':before,'budget_after':AuthorizedBudget().count(),'unit_tests':t.testsRun,
          'scope':'Previously exposed failing workload, conservative fallback validation only; no production safety guarantee.',
          'opportunity_tradeoff':'Unfiltered-byte gate also refuses large source tables that become small only after selective filtering. Runtime evidence is needed for less conservative admission.'}
        dump(out/'COMPLETED.json',summary);print(json.dumps(summary,indent=2),flush=True)
    finally:spark.stop()
if __name__=='__main__':main()
