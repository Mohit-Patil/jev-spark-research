"""Offline regression/evidence checks. No model calls or credential reads."""
from pathlib import Path
import copy,json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent))
from safe_jev import Budget
import headroom_v3
from runtime_features import project

class Checks(unittest.TestCase):
    def setUp(self):
        self.pair=json.loads((ROOT/'observer_check_v2'/'boundary_pairs.jsonl').read_text().splitlines()[0])
    def test_initial_plan_collisions_keep_all_hints(self):
        class FakeSpark:
            def sql(self,s):return s
        with patch.object(headroom_v3,'plan_info',return_value={'fingerprint':'same'}):
            plans,collisions=headroom_v3.inventory(FakeSpark())
        self.assertEqual(set(plans),{'NATIVE','BROADCAST','MERGE','SHUFFLE_HASH'})
        self.assertEqual(len(collisions[0]),4)
    def test_native_label_and_outcomes_not_in_payload(self):
        p=copy.deepcopy(self.pair);p['native_would_choose']='FORBIDDEN_SENTINEL';p['result']='OUTCOME_SENTINEL';p['oracle_remaining_s']=0.001
        p['current']['stock_cost']='COST_SENTINEL'
        for view in ('PLANS','STATS'):
            text=json.dumps(project(p,view))
            for forbidden in ('FORBIDDEN_SENTINEL','OUTCOME_SENTINEL','COST_SENTINEL','oracle_remaining_s','native_would_choose','stock_cost'):
                self.assertNotIn(forbidden,text)
    def test_statistics_view_only(self):
        self.assertNotIn('runtime_stage_observations',project(self.pair,'PLANS'))
        self.assertIn('runtime_stage_observations',project(self.pair,'STATS'))
    def test_unknown_not_zero(self):
        p=copy.deepcopy(self.pair);p['current']['stages'][0]['row_count']=None
        self.assertIsNone(project(p,'STATS')['runtime_stage_observations']['current'][0]['row_count'])
    def test_truncation_rejected(self):
        p=copy.deepcopy(self.pair);p['current']['plan_truncated']=True
        with self.assertRaises(ValueError):project(p,'PLANS')
    def test_reversed_role_rejected(self):
        p=copy.deepcopy(self.pair);p['current']['source_line']=366
        with self.assertRaises(ValueError):project(p,'STATS')
    def test_intervention_rejected(self):
        p=copy.deepcopy(self.pair);p['intervention']=True
        with self.assertRaises(ValueError):project(p,'STATS')
    def test_runtime_partition_totals(self):
        for line in (ROOT/'observer_check_v2'/'boundary_pairs.jsonl').read_text().splitlines():
            p=json.loads(line)
            for view in ('current','proposed'):
                for s in p[view]['stages']:
                    sizes=s.get('shuffle_serialized_partition_bytes')
                    if sizes is not None:
                        self.assertTrue(s['materialized']);self.assertTrue(s['is_runtime'])
                        self.assertEqual(sum(sizes),s['partition_bytes_total'])
                        self.assertEqual(len(sizes),s['partition_count'])
    def test_colliding_plans_really_diverged(self):
        rows=[json.loads(s) for s in (ROOT/'headroom_v3'/'raw.jsonl').read_text().splitlines()]
        native=[r for r in rows if r['case']=='selective_10pct' and r['strategy']=='NATIVE']
        merge=[r for r in rows if r['case']=='selective_10pct' and r['strategy']=='MERGE']
        self.assertEqual(native[0]['fingerprint'],merge[0]['fingerprint'])
        self.assertTrue(all('BroadcastHashJoin' in r['final_plan'].split('== Initial Plan ==')[0] for r in native))
        self.assertTrue(all('SortMergeJoin' in r['final_plan'].split('== Initial Plan ==')[0] for r in merge))
    def test_all_variant_results_match_expected(self):
        rows=[json.loads(s) for s in (ROOT/'headroom_v3'/'raw.jsonl').read_text().splitlines()]
        self.assertEqual(len(rows),144)
        for r in rows:
            expected=json.loads((ROOT/'headroom_v3'/(r['case']+'_candidates.json')).read_text())['expected_result']
            self.assertEqual(r['result'],expected);self.assertEqual(r['status'],'succeeded')

if __name__=='__main__':
    before=Budget().count();result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Checks))
    after=Budget().count();report={'tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
      'successful':result.wasSuccessful(),'budget_before':before,'budget_after':after,'live_requests':0}
    assert before==after
    (ROOT/'offline_v2_tests.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))
    raise SystemExit(0 if result.wasSuccessful() else 1)
