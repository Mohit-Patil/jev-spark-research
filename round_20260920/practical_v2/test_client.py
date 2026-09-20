"""Mock-only failure-path tests. No credentials read and no live requests."""
from pathlib import Path
import json,time,unittest
import httpx
from bounded_client import BoundedClient,encode,resolve_advice,MODEL,Budget,BudgetError

PAYLOAD={'model':MODEL,'state':'synthetic test','questions':{'plan':{'type':'choice','instructions':'Choose one','criteria':{'CURRENT':'Current','PROPOSED':'Proposed','ABSTAIN':'Abstain'}}}}
FAKE='offline-credential-sentinel'
class MemoryBudget:
    def __init__(self):self.n=0
    def reserve(self,*args):self.n+=1;return self.n

def response():
    return {'model':MODEL,'answers':{'plan':{'type':'choice','choice':'PROPOSED','confidence':0.9,'probabilities':{'CURRENT':0.05,'PROPOSED':0.9,'ABSTAIN':0.05}}},'usage':{'input_tokens':30,'output_tokens':10}}

class Checks(unittest.TestCase):
    def client(self,handler,**kw):
        self.budget=MemoryBudget();self.hits=0
        def wrapped(req):
            self.hits+=1
            if 'trace' in req.extensions:req.extensions['trace']('connection.connect_tcp.started',{'DO_NOT_LOG':FAKE})
            return handler(req)
        return BoundedClient(transport=httpx.MockTransport(wrapped),budget=self.budget,
          key_loader=lambda:(FAKE,{}),log_path=None,**kw)
    def call(self,handler,**kw):
        c=self.client(handler,**kw)
        try:return c.request(PAYLOAD,'offline_test')
        finally:c.close()
    def test_valid(self):
        r=self.call(lambda q:httpx.Response(200,json=response()))
        self.assertEqual(r['choice'],'PROPOSED');self.assertEqual(self.budget.n,1)
    def test_auth_failure_charged_once(self):
        r=self.call(lambda q:httpx.Response(401,text=FAKE))
        self.assertEqual(r['choice'],'ABSTAIN');self.assertEqual(self.hits,1);self.assertEqual(self.budget.n,1)
        self.assertNotIn(FAKE,json.dumps(r))
    def test_model_mismatch_fallback(self):
        x=response();x['model']='unpinned'
        r=self.call(lambda q:httpx.Response(200,json=x));self.assertEqual(r['status'],'failed')
    def test_timeout_does_not_leak(self):
        def handler(q):raise httpx.ReadTimeout(FAKE)
        r=self.call(handler);self.assertEqual(r['choice'],'ABSTAIN');self.assertNotIn(FAKE,json.dumps(r));self.assertEqual(self.hits,1)
    def test_oversized_reply(self):
        r=self.call(lambda q:httpx.Response(200,text='x'*70000));self.assertEqual(r['status'],'failed')
    def test_bad_payload_before_reservation(self):
        c=self.client(lambda q:httpx.Response(200,json=response()))
        try:
            with self.assertRaises(ValueError):c.request({'model':'wrong'},'offline_test')
            self.assertEqual(self.hits,0);self.assertEqual(self.budget.n,0)
        finally:c.close()
    def test_oversized_request(self):
        x={**PAYLOAD,'state':'x'*25000}
        with self.assertRaises(BudgetError):encode(x)
    def test_trace_info_never_serialized(self):
        r=self.call(lambda q:httpx.Response(200,json=response()))
        self.assertNotIn(FAKE,json.dumps(r));self.assertNotIn('DO_NOT_LOG',json.dumps(r))
    def test_late_response_rejected(self):
        def handler(q):time.sleep(0.02);return httpx.Response(200,json=response())
        r=self.call(handler,late_after_s=0.001);self.assertEqual(r['choice'],'ABSTAIN');self.assertEqual(self.budget.n,1)
    def test_abstain_follows_native_proposed(self):
        self.assertEqual(resolve_advice({'status':'succeeded','choice':'ABSTAIN'},'PROPOSED'),'PROPOSED')
    def test_late_advice_follows_native_not_current(self):
        r={'status':'succeeded','choice':'CURRENT','confidence':0.99,'latency_s':1.0}
        self.assertEqual(resolve_advice(r,'PROPOSED'),'PROPOSED')
    def test_good_advice(self):
        r={'status':'succeeded','choice':'CURRENT','confidence':0.99,'latency_s':0.2}
        self.assertEqual(resolve_advice(r,'PROPOSED'),'CURRENT')

if __name__=='__main__':
    before=Budget().count();r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Checks))
    assert Budget().count()==before
    data={'tests':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),'successful':r.wasSuccessful(),'real_api_attempts':before,'new_jev_attempts':0}
    (Path(__file__).resolve().parent/'test_client.json').write_text(json.dumps(data,indent=2));print(json.dumps(data))
    raise SystemExit(0 if r.wasSuccessful() else 1)
