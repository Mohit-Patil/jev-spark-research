"""Mocked workflow ordering and fallback checks; no Spark jobs or model calls."""
from pathlib import Path
import json,tempfile,time,unittest
from unittest.mock import patch
import policy_holdout as p

class FakeContext:
    def setJobGroup(self,*a,**kw):pass
    def cancelJobGroup(self,*a):pass
    def setLocalProperty(self,*a):pass

class Checks(unittest.TestCase):
    def setUp(self):
        self.events=[];self.tmp=tempfile.TemporaryDirectory(dir=p.HERE,prefix='.policy_test_');self.out=Path(self.tmp.name)
        self.c=dict(name='tiny',n=3,m=2,keep=1000,skew=False,seed=0,join='LEFT');self.want=[[0,3,0,0,0]]
    def tearDown(self):self.tmp.cleanup()
    def run_workflow(self,arm,choice='BROADCAST',fail=False):
        owner=self
        class Frame:
            def __init__(self,text):self.text=text;self._jdf=self
            def collect(self):owner.events.append('collect');return owner.want
            def queryExecution(self):return self
            def executedPlan(self):return self
            def toString(self):return self.text
        class Spark:
            sparkContext=FakeContext()
            def sql(self,text):owner.events.append('sql');return Frame(text)
        class Client:
            def request(self,*a):
                owner.events.append('request')
                if fail:raise RuntimeError('private-error-sentinel')
                return {'status':'succeeded','choice':choice,'confidence':0.4,'completed_unix':time.time()}
        original=p.b.append
        def append(path,value):owner.events.append('persist_decision');return original(path,value)
        with patch.object(p.b,'plan_info',side_effect=lambda df:{'initial_plan':df.text,'fingerprint':'same'}),patch.object(p.b,'append',side_effect=append):
            return p.workflow(Spark(),self.c,arm,Client(),2147483648,self.want,'offline_policy',self.out)
    def test_decision_persisted_before_action(self):
        r=self.run_workflow('JEV_UNGATED');self.assertEqual(r['status'],'succeeded')
        self.assertLess(self.events.index('request'),self.events.index('persist_decision'))
        self.assertLess(self.events.index('persist_decision'),self.events.index('collect'))
        self.assertEqual(self.events.count('collect'),1);self.assertEqual(r['selected'],'BROADCAST')
    def test_abstain_uses_native(self):self.assertEqual(self.run_workflow('JEV_UNGATED','ABSTAIN')['selected'],'NATIVE')
    def test_error_uses_native_without_message(self):
        r=self.run_workflow('JEV_UNGATED',fail=True);self.assertEqual(r['selected'],'NATIVE');self.assertNotIn('private-error-sentinel',json.dumps(r))
    def test_native_no_api_and_one_query(self):
        r=self.run_workflow('NATIVE');self.assertNotIn('request',self.events);self.assertEqual(self.events.count('sql'),1);self.assertTrue(r['correct'])
    def test_rule_no_api(self):
        r=self.run_workflow('RULE');self.assertNotIn('request',self.events);self.assertEqual(r['selected'],'BROADCAST')
    def test_outcomes_not_in_payload(self):
        plans={k:{'initial_plan':'physical plan','outcome':'DO_NOT_SEND'} for k in p.b.STRATEGIES}
        text=json.dumps(p.make_payload(self.c,plans,2147483648))
        self.assertNotIn('DO_NOT_SEND',text);self.assertNotIn('native_would_choose',text);self.assertNotIn('median',text)

if __name__=='__main__':
    before=p.Budget().count();r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Checks))
    assert p.Budget().count()==before
    result={'tests':r.testsRun,'successful':r.wasSuccessful(),'failures':len(r.failures),'errors':len(r.errors),'new_jev_attempts':0}
    (p.HERE/'test_policy.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
    raise SystemExit(0 if r.wasSuccessful() else 1)
