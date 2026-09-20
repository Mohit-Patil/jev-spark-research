"""Bounded repeated-query cache experiment on TWO new immutable snapshots.
First misses, failures, feature preparation and candidate enumeration are timed.
Not a production cache: file content hashes register immutable test snapshots;
production would need reliable table/snapshot versions and invalidation events.
"""
from collections import OrderedDict
import copy, hashlib, json, random, statistics, time, unittest
from history_core import *
from experiment import run_policy

class AdviceCache:
    def __init__(self,client,snapshot_id,ttl=60,maximum=8,clock=time.monotonic):
        self.client=client;self.snapshot_id=snapshot_id;self.ttl=ttl;self.maximum=maximum;self.clock=clock;self.entries=OrderedDict()
    def request(self,p,purpose):
        started=time.perf_counter();body=encode(p);key=(self.snapshot_id,hashlib.sha256(body).hexdigest());now=self.clock()
        if key in self.entries:
            expires,source=self.entries.pop(key)
            if now<expires:
                self.entries[key]=(expires,source)
                return {'status':'succeeded','choice':source['choice'],'model':MODEL,'confidence':source.get('confidence'),
                  'probabilities':source.get('probabilities'),'attempt':None,'source_attempt':source['attempt'],
                  'source_completed_unix':source['completed_unix'],'completed_unix':time.time(),
                  'request_sha256':key[1],'request_bytes':len(body),'cache_hit':True,'snapshot_id':self.snapshot_id,
                  'latency_s':0.0,'adapter_wall_s':time.perf_counter()-started,'source_latency_s':source['latency_s']}
        rec=self.client.request(p,purpose);rec=copy.deepcopy(rec);rec['cache_hit']=False;rec['snapshot_id']=self.snapshot_id
        if rec.get('status')=='succeeded' and rec.get('choice') in CRITERIA:
            self.entries[key]=(self.clock()+self.ttl,copy.deepcopy(rec))
            while len(self.entries)>self.maximum:self.entries.popitem(last=False)
        return rec

class Fake:
    def __init__(self):self.calls=0;self.fail=False
    def request(self,p,purpose):
        self.calls+=1
        return {'status':'failed' if self.fail else 'succeeded','choice':'BROADCAST','attempt':self.calls,
          'completed_unix':time.time(),'latency_s':.5,'model':MODEL,'confidence':.5}

class Tests(unittest.TestCase):
    def setUp(self):
        self.p={'model':MODEL,'state':{},'questions':{'plan':{'type':'choice','instructions':INSTRUCTIONS,'criteria':CRITERIA}}}
        self.fake=Fake();self.now=[0.];self.c=AdviceCache(self.fake,'data1',ttl=10,maximum=2,clock=lambda:self.now[0])
    def test_hit_avoids_request(self):
        self.assertFalse(self.c.request(self.p,'x')['cache_hit']);self.assertTrue(self.c.request(self.p,'x')['cache_hit']);self.assertEqual(self.fake.calls,1)
    def test_hits_do_not_claim_new_attempts(self):
        self.c.request(self.p,'x');r=self.c.request(self.p,'x');self.assertIsNone(r['attempt']);self.assertEqual(r['source_attempt'],1)
    def test_failure_not_cached(self):
        self.fake.fail=True;self.c.request(self.p,'x');self.c.request(self.p,'x');self.assertEqual(self.fake.calls,2)
    def test_expiry(self):
        self.c.request(self.p,'x');self.now[0]=11;self.assertFalse(self.c.request(self.p,'x')['cache_hit'])
    def test_snapshot_invalidation(self):
        self.c.request(self.p,'x');self.c.snapshot_id='data2';self.assertFalse(self.c.request(self.p,'x')['cache_hit'])
    def test_metadata_invalidation(self):
        self.c.request(self.p,'x');self.p['state']['different_filter']=True;self.assertFalse(self.c.request(self.p,'x')['cache_hit'])
    def test_mutation_does_not_change_cache(self):
        r=self.c.request(self.p,'x');r['choice']='MERGE';self.assertEqual(self.c.request(self.p,'x')['choice'],'BROADCAST')
    def test_capacity(self):
        for i in range(3):self.p['state']['i']=i;self.c.request(self.p,'x')
        self.assertEqual(len(self.c.entries),2)

CASES_CACHE={
 'repeat_selective':dict(n=4800000,m=360000,width=448,keep=20,seed=983,family='LEFT_LOOKUP'),
 'repeat_wide':dict(n=200000,m=350000,width=448,keep=1000,seed=991,family='LEFT_LOOKUP')}
POLICIES_CACHE=('NATIVE','TUNED64','LOCAL_HISTORY','JEV_HISTORY','CACHED_HISTORY')

def identity(directory):
    manifest=[]
    for p in sorted(directory.rglob('*.parquet')):
        h=hashlib.sha256()
        with p.open('rb') as f:
            for b in iter(lambda:f.read(1048576),b''):h.update(b)
        manifest.append({'file':str(p.relative_to(directory)),'sha256':h.hexdigest(),'bytes':p.stat().st_size})
    return hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest(),manifest

def main():
    out=R/'cache_trial_v1';out.mkdir(exist_ok=False);before=AuthorizedBudget().count()
    tests=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    dump(out/'TESTS.json',{'passed':tests.wasSuccessful(),'tests':tests.testsRun,'real_api_calls':0})
    if not tests.wasSuccessful():raise ValueError('cache_tests_failed')
    base=json.loads((R/'FROZEN_PROTOCOL.json').read_text())
    for path,h in base['source_hashes'].items():assert sha(WS/path)==h
    assert sha(R/'HISTORY.json')==base['history_sha256']
    history=json.loads((R/'HISTORY.json').read_text())['observations']
    protocol={'frozen_unix':time.time(),'cases':CASES_CACHE,'policies':POLICIES_CACHE,'reps':6,
      'source_sha256':sha(R/'cache_trial.py'),'base_protocol_sha256':sha(R/'FROZEN_PROTOCOL.json'),
      'history_sha256':sha(R/'HISTORY.json'),'deadline_s':1.25,'ttl_s':60,'cache_limit':8,
      'key':'immutable dataset content hash plus canonical payload (including model, prompt, plans, config, footer features and history)',
      'max_new_api_attempts':24,'cache_population':'Only valid responses; no cached failures. First misses are inside measured workflows.',
      'scope':'Repeated queries on two newly generated immutable LEFT_LOOKUP datasets. Exploratory follow-up, not an independent family test.',
      'input_identity_cost':'Full file hashing is charged as dataset-registration/setup work and reported separately; not repeated for immutable test snapshots.'}
    dump(out/'FROZEN.json',protocol);budget=BatchBudget(24);spark=corpus.session();spark.sparkContext.setLogLevel('ERROR')
    start=time.perf_counter();summaries=[];allrows=[]
    try:
        for name,c in CASES_CACHE.items():
            caseout=out/name;caseout.mkdir();want=oracle(c);dump(caseout/'oracle.json',want)
            dataset=corpus.write_inputs(spark,c,caseout);dump(caseout/'dataset.json',dataset)
            t=time.perf_counter();snapshot,files=identity(caseout/'data');registration=time.perf_counter()-t
            dump(caseout/'IMMUTABLE_DATASET.json',{'identity':snapshot,'files':files,'registration_s':registration})
            client=DeadlineClient(budget,ROUND/'jev_calls.jsonl',deadline=1.25);cached=AdviceCache(client,snapshot);rr=[]
            try:
                rng=random.Random(7393)
                arms=list(corpus.ARMS);rng.shuffle(arms)
                for arm in arms:
                    wr=corpus.run(spark,arm,want,'cache7_'+name+'_warm_'+arm)
                    wr.update(case=name,phase='warmup',policy=arm,selected=arm,rep=-1)
                    append(caseout/'raw.jsonl',wr);rr.append(wr);assert wr['status']=='succeeded'
                for rep in range(6):
                    order=list(POLICIES_CACHE);rng.shuffle(order)
                    for pos,policy in enumerate(order):
                        if time.perf_counter()-start>430:raise TimeoutError('batch_launch_deadline')
                        inner='JEV_HISTORY' if policy=='CACHED_HISTORY' else policy
                        tag='cache7_'+name+'_'+str(rep)+'_'+policy
                        r=run_policy(spark,c,inner,history,caseout/'data',caseout,tag,want,cached if policy=='CACHED_HISTORY' else client)
                        r.update(case=name,phase='measured',policy=policy,rep=rep,position=pos)
                        append(caseout/'raw.jsonl',r);rr.append(r);assert r['status']=='succeeded'
                    print(json.dumps({'case':name,'rep':rep,'completed':True}),flush=True)
                measured=[r for r in rr if r['phase']=='measured'];native={r['rep']:r for r in measured if r['policy']=='NATIVE'}
                s={'case':name,'queries':len(rr),'snapshot':snapshot,'registration_s':registration,'policies':{}}
                for policy in POLICIES_CACHE:
                    xs=[r for r in measured if r['policy']==policy]
                    s['policies'][policy]={'n':len(xs),'median_s':statistics.median(r['to_result_s'] for r in xs),
                      'total_s':sum(r['to_result_s'] for r in xs),'times_s':[r['to_result_s'] for r in xs],
                      'first_query_s':xs[0]['to_result_s'],'faster_than_native_pairs':sum(r['to_result_s']<native[r['rep']]['to_result_s'] for r in xs),
                      'cache_hits':sum(bool((r.get('decision') or {}).get('record',{}).get('cache_hit')) for r in xs),
                      'choices':{k:sum(r['selected']==k for r in xs) for k in (*ACTIONS,'TUNED64')}}
                dump(caseout/'summary.json',s);summaries.append(s);allrows.extend(rr)
                print(json.dumps(s,indent=2),flush=True)
            finally:client.close()
        report={'status':'succeeded','new_queries':len(allrows),'measured_queries':60,'all_correct':True,'new_api_attempts':budget.used,
          'budget_before':before,'budget':AuthorizedBudget().status(),'cases':summaries,'runtime_aqe_interventions':0}
        dump(out/'COMPLETED.json',report);print(json.dumps(report,indent=2),flush=True)
    finally:spark.stop()
if __name__=='__main__':main()
