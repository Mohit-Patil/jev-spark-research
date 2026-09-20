"""Offline tests: mocked HTTP only, no real key/ledger reads in requests."""
import asyncio, copy, json, time, unittest
from history_core import *

class FakeBudget:
    def __init__(self):self.used=0
    def reserve(self,*args):self.used+=1;return self.used

def feature(family='LEFT_LOOKUP'):
    return {'family':family,'left':{'file_rows':100000,'file_bytes':1000000,'column_uncompressed_bytes':2000000},
      'right':{'file_rows':10000,'file_bytes':500000,'column_uncompressed_bytes':1000000},
      'estimated_build_rows':10000,'estimated_build_encoded_bytes':1000000,
      'estimated_retained_fraction':1.0,'filter_sql':'k >= 0','residual_condition':None}

def fixture(choice='BROADCAST'):
    return {'model':MODEL,'answers':{'plan':{'type':'choice','choice':choice,'confidence':0.5,
      'probabilities':{k:0.2 for k in CRITERIA}}},'usage':{'input_tokens':200,'output_tokens':20}}

class Drip(httpx.AsyncByteStream):
    async def __aiter__(self):
        for _ in range(100):
            await asyncio.sleep(.01);yield b' '

class Tests(unittest.TestCase):
    def setUp(self):
        self.f=feature();self.plans={k:{'initial_plan':'LEFT join '+k} for k in ACTIONS}
        self.h=[{'historical_id':'development_only','features':self.f,
          'candidate_median_s':{'NATIVE':1.0,'BROADCAST':.5,'MERGE':1.2,'SHUFFLE_HASH':.8}}]
        self.p=payload(self.f,self.plans)
    def client(self,handler,deadline=.2):
        return DeadlineClient(FakeBudget(),None,deadline,transport=httpx.MockTransport(handler),
          key_loader=lambda:('not-a-real-key-for-tests',{}))
    def test_histories_are_not_mutated(self):
        before=copy.deepcopy(self.h);p=payload(self.f,self.plans,self.h)
        p['state']['historical_observations'][0]['candidate_median_s']['NATIVE']=999
        self.assertEqual(before,self.h)
    def test_zero_history_excluded(self):self.assertNotIn('historical_observations',self.p['state'])
    def test_same_question_across_views(self):self.assertEqual(self.p['questions'],payload(self.f,self.plans,self.h)['questions'])
    def test_local_retrieves_prior_strategy(self):self.assertEqual(local_select(self.f,self.h)['choice'],'BROADCAST')
    def test_local_unseen_family_fallback(self):self.assertEqual(local_select(feature('LEFT_RESIDUAL'),self.h)['choice'],'NATIVE')
    def test_local_noise_margin(self):
        self.h[0]['candidate_median_s']['BROADCAST']=.96
        self.h[0]['candidate_median_s']['SHUFFLE_HASH']=1
        self.assertEqual(local_select(self.f,self.h)['choice'],'NATIVE')
    def test_invalid_numeric_features(self):
        self.f['estimated_build_rows']=-1
        with self.assertRaises(ValueError):vector(self.f)
    def test_canonical_serialization(self):self.assertEqual(encode(self.p),encode(json.loads(json.dumps(self.p,sort_keys=True))))
    def test_oversized_input_before_reservation(self):
        c=self.client(lambda r:httpx.Response(200,json=fixture()));p=copy.deepcopy(self.p);p['state']['huge']='x'*MAX_REQUEST_BYTES
        try:
            with self.assertRaises(BudgetError):c.request(p,'mock')
            self.assertEqual(c.budget.used,0)
        finally:c.close()
    def test_success_and_single_reservation(self):
        c=self.client(lambda r:httpx.Response(200,json=fixture()))
        try:
            rec=c.request(self.p,'mock');self.assertEqual(effective(rec),'BROADCAST');self.assertEqual(c.budget.used,1)
        finally:c.close()
    def test_http_error_falls_back_without_retry(self):
        c=self.client(lambda r:httpx.Response(500,text='not-a-real-key-for-tests'))
        try:
            rec=c.request(self.p,'mock');self.assertEqual(effective(rec),'NATIVE');self.assertEqual(c.budget.used,1)
            self.assertNotIn('not-a-real-key-for-tests',json.dumps(rec))
        finally:c.close()
    def test_invalid_choice_fallback(self):
        c=self.client(lambda r:httpx.Response(200,json=fixture('INVALID')))
        try:self.assertEqual(effective(c.request(self.p,'mock')),'NATIVE')
        finally:c.close()
    def test_wrong_model_rejected(self):
        obj=fixture();obj['model']='unapproved'
        c=self.client(lambda r:httpx.Response(200,json=obj))
        try:self.assertEqual(effective(c.request(self.p,'mock')),'NATIVE')
        finally:c.close()
    def test_slow_drip_total_deadline_and_cleanup(self):
        c=self.client(lambda r:httpx.Response(200,stream=Drip()),deadline=.05)
        try:
            rec=c.request(self.p,'mock');self.assertEqual(rec['error_type'],'TimeoutError')
            self.assertLess(rec['latency_s'],.25);self.assertEqual(effective(rec),'NATIVE');self.assertEqual(c.budget.used,1)
            self.assertEqual(len(asyncio.all_tasks(c.runner.get_loop())),0)
        finally:c.close()
    def test_oversized_response_fallback(self):
        c=self.client(lambda r:httpx.Response(200,content=b'x'*(MAX_RESPONSE_BYTES+1)))
        try:self.assertEqual(effective(c.request(self.p,'mock')),'NATIVE')
        finally:c.close()
    def test_residual_oracle_subset(self):
        c=dict(n=200,m=97,width=64,keep=500,seed=947,family='LEFT_RESIDUAL')
        a=oracle(c);b=corpus.oracle(c)
        self.assertEqual(sum(r[1] for r in a),200)
        self.assertLessEqual(sum(r[-1] for r in a),sum(r[-1] for r in b))

if __name__=='__main__':
    before=AuthorizedBudget().count();result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    report={'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'passed':result.wasSuccessful(),
      'budget_before':before,'budget_after':AuthorizedBudget().count(),'real_api_calls':0}
    dump(R/'OFFLINE_TESTS.json',report);print(json.dumps(report));raise SystemExit(0 if result.wasSuccessful() else 1)
