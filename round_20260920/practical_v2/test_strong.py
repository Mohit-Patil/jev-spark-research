"""Offline checks for the stronger synthetic-domain comparator. No network."""
from pathlib import Path
import json,unittest
from unittest.mock import patch
import strong_baseline_confirm as s

class Checks(unittest.TestCase):
    def test_tested_domain(self):
        for c in s.CASES:self.assertEqual(s.bounded_strategy(c),'BROADCAST')
    def test_outside_domain_native(self):
        c=dict(s.CASES[0],m=2000001)
        self.assertEqual(s.bounded_strategy(c),'NATIVE')
    def test_too_many_filtered_rows_native(self):
        c=dict(s.CASES[0],keep=900)
        self.assertEqual(s.bounded_strategy(c),'NATIVE')
    def test_snapshots_distinct_from_previous(self):
        self.assertFalse({c['seed'] for c in s.CASES}&{c['seed'] for c in s.p.CASES})
    def test_skew_retained(self):
        self.assertTrue(s.b.features(s.CASES[-1])['hot_key_survives_dimension_filter'])
    def test_direct_workflow(self):
        want=[[0,1,0,0,1]]
        class Ctx:
            def setJobGroup(self,*a,**k):pass
            def cancelJobGroup(self,*a):pass
            def setLocalProperty(self,*a):pass
        class DF:
            def __init__(self):self._jdf=self
            def collect(self):return want
            def queryExecution(self):return self
            def executedPlan(self):return self
            def toString(self):return 'BroadcastHashJoin'
        class Spark:
            sparkContext=Ctx()
            def sql(self,q):return DF()
        with patch.object(s.b,'plan_info',return_value={'initial_plan':'BroadcastHashJoin'}):
            r=s.broadcast_workflow(Spark(),s.CASES[0],2147483648,want,'offline_strong')
        self.assertTrue(r['correct']);self.assertEqual(r['selected'],'BROADCAST');self.assertIsNone(r['decision'])
        self.assertGreaterEqual(r['workflow_s'],r['collect_s'])

if __name__=='__main__':
    before=s.p.Budget().count();r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Checks))
    assert s.p.Budget().count()==before
    result={'tests':r.testsRun,'successful':r.wasSuccessful(),'failures':len(r.failures),'errors':len(r.errors),'new_jev_attempts':0}
    (Path(__file__).resolve().parent/'test_strong.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
    raise SystemExit(0 if r.wasSuccessful() else 1)
