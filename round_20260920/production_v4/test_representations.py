"""Offline feature isolation and reproducible serialization checks."""
from pathlib import Path
import copy,hashlib,json,unittest
from representation_inputs import build,stage_features
from authorized_budget import AuthorizedBudget
from runtime_advisor_v5 import payload
R=Path(__file__).resolve().parent
FIXTURES=[json.loads(x)['snapshot'] for x in (R/'runtime_jev_v5'/'model_decisions.jsonl').read_text().splitlines()]
class Tests(unittest.TestCase):
    def test_all_old_events_encode(self):
        for e in FIXTURES:
            for view in ('VERBOSE','COMPACT'):self.assertEqual(build(e,view)['model'],'jev-1.13.0')
    def test_verbose_semantics_unchanged(self):
        for e in FIXTURES:self.assertEqual(build(e,'VERBOSE'),payload(e))
    def test_same_questions_and_options(self):
        for e in FIXTURES:self.assertEqual(build(e,'COMPACT')['questions'],build(e,'VERBOSE')['questions'])
    def test_compact_smaller_for_saved_events(self):
        for e in FIXTURES:self.assertLess(len(json.dumps(build(e,'COMPACT'))),len(json.dumps(build(e,'VERBOSE'))))
    def test_outcomes_ignored(self):
        for e in FIXTURES:
            ee=copy.deepcopy(e);ee.update(final_query_seconds=0.0001,winner='BROADCAST',native_cost=13)
            for view in ('VERBOSE','COMPACT'):self.assertEqual(build(e,view),build(ee,view))
    def test_run_identity_and_stage_ids_ignored(self):
        e=copy.deepcopy(FIXTURES[0]);e['run_id']='changed';e['left']['stage_id']=999999;e['right']['object_id']=0
        for view in ('VERBOSE','COMPACT'):self.assertEqual(build(e,view),build(FIXTURES[0],view))
    def test_canonical_bytes_survive_sorted_log(self):
        for view in ('VERBOSE','COMPACT'):
            p=build(FIXTURES[0],view);original=json.dumps(p,separators=(',',':')).encode()
            logged=json.loads(json.dumps(p,sort_keys=True));reencoded=json.dumps(logged,separators=(',',':')).encode()
            self.assertEqual(original,reencoded)
    def test_unknown_memory_not_zero(self):
        p=build(FIXTURES[0],'COMPACT');self.assertIsNone(p['state']['right']['hash_table_memory_bytes'])
        self.assertIsNone(p['state']['resources']['available_heap_bytes'])
    def test_partition_stats_exact(self):
        e=FIXTURES[0];f=stage_features(e['right']);xs=e['right']['serialized_partition_bytes']
        self.assertEqual(sum(xs),f['serialized_shuffle_bytes_total']);self.assertEqual(max(xs),f['partition_bytes_max'])
    def test_unfinished_stage_rejected(self):
        e=copy.deepcopy(FIXTURES[0]);e['left']['materialized']=False
        with self.assertRaises((AssertionError,ValueError)):build(e,'COMPACT')
    def test_negative_partition_rejected(self):
        e=copy.deepcopy(FIXTURES[0]);e['left']['serialized_partition_bytes'][0]=-1
        with self.assertRaises(ValueError):build(e,'COMPACT')
    def test_unknown_view_rejected(self):
        with self.assertRaises(ValueError):build(FIXTURES[0],'anything')
if __name__=='__main__':
    before=AuthorizedBudget().count();r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
    result={'passed':r.wasSuccessful(),'tests':r.testsRun,'errors':len(r.errors),'failures':len(r.failures),
        'live_requests':0,'budget_before':before,'budget_after':AuthorizedBudget().count()}
    assert result['budget_before']==result['budget_after']
    (R/'REPRESENTATION_TESTS.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
    raise SystemExit(0 if r.wasSuccessful() else 1)
