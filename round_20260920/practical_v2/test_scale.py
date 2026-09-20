"""Offline checks for the new workload family and policy inventory."""
from pathlib import Path
import json, unittest
from unittest.mock import patch
import scale_benchmark as b
from baseline import expected

class Checks(unittest.TestCase):
    def c(self,**kw):
        c=dict(name='tiny',n=3,m=2,seed=0,keep=1000,skew=False,join='INNER');c.update(kw);return c
    def test_inner_hand_calculated(self):
        self.assertEqual(b.oracle(self.c()),[[0,1,0,0,1],[1,1,91,1,1],[2,1,0,0,1]])
    def test_left_preserves_unmatched(self):
        self.assertEqual(b.oracle(self.c(join='LEFT',keep=0)),[[0,1,0,0,0],[1,1,0,1,0],[2,1,0,0,0]])
    def test_inner_empty(self): self.assertEqual(b.oracle(self.c(keep=0)),[])
    def test_inner_agrees_with_original_oracle(self):
        c=self.c(n=2013,m=107,seed=313,keep=500)
        self.assertEqual([r[:-1] for r in b.oracle(c)],expected(c))
    def test_skew_matches_original_oracle(self):
        c=self.c(n=2013,m=107,seed=313,keep=500,skew=True)
        self.assertEqual([r[:-1] for r in b.oracle(c)],expected(c))
    def test_dimension_count_uneven_period(self):
        c=self.c(m=1043,seed=313,keep=271)
        self.assertEqual(b.features(c)['filtered_dimension_rows'],sum((k*17+313)%1000<271 for k in range(1043)))
    def test_initial_collisions_retained(self):
        class Spark:
            def sql(self,q):return q
        with patch.object(b,'plan_info',return_value={'fingerprint':'same'}):
            arms,collisions=b.inventory(Spark(),self.c())
        self.assertEqual(len(arms),4);self.assertEqual(len(collisions[0]),4)
    def test_no_invalid_arm(self):
        with self.assertRaises(ValueError):b.query(self.c(),'INVENTED')
    def test_left_skew_hot_key_really_retained(self):
        self.assertTrue(b.features(b.CASES['left_skew80_50'])['hot_key_survives_dimension_filter'])
    def test_no_optimizer_disabling_sql(self):
        self.assertNotIn('hint',b.query(self.c(),'NATIVE').lower())
        self.assertNotIn('/*+',b.query(self.c(),'NATIVE'))

if __name__=='__main__':
    before=b.Budget().count()
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Checks))
    assert b.Budget().count()==before
    report={'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'successful':result.wasSuccessful(),'api_attempts':before,'new_jev_attempts':0}
    (Path(__file__).resolve().parent/'test_scale.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report));raise SystemExit(0 if result.wasSuccessful() else 1)
