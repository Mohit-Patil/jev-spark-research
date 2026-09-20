"""Offline adapter/harness checks: no key reads or network calls."""
import concurrent.futures, json, tempfile, unittest
from pathlib import Path
from baseline import expected, normalize
from safe_jev import Budget, BudgetError, MAX_REQUEST_BYTES, MODEL, SecretError, parse_key, validate_response
ROOT=Path(__file__).resolve().parent

class Checks(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=ROOT,prefix='offline_test_'); self.p=Path(self.tmp.name)/'ledger.jsonl'
    def tearDown(self): self.tmp.cleanup()
    def test_persistent_budget(self):
        self.assertEqual(Budget(self.p).reserve(20,'test','test'),1)
        self.assertEqual(Budget(self.p).reserve(20,'test','test'),2)
        self.assertEqual(Budget(self.p).count(),2)
    def test_cap_across_concurrent_instances(self):
        def take(i):
            try: return Budget(self.p).reserve(20,'test','test')
            except BudgetError: return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            vals=list(pool.map(take,range(60)))
        self.assertEqual(sorted(x for x in vals if x is not None),list(range(1,51)))
        self.assertEqual(Budget(self.p).count(),50)
    def test_corruption_fail_closed(self):
        self.p.write_text('{bad')
        with self.assertRaises(BudgetError): Budget(self.p).reserve(20,'test','test')
    def test_size_rejected_before_reservation(self):
        with self.assertRaises(BudgetError): Budget(self.p).reserve(MAX_REQUEST_BYTES+1,'test','test')
        self.assertFalse(self.p.exists())
    def test_reservation_survives_no_request(self):
        Budget(self.p).reserve(20,'test','test')
        self.assertEqual(Budget(self.p).count(),1)
    def test_secret_parser_no_expansion(self):
        self.assertEqual(parse_key('export TYPESAFE_API_KEY="not-a-real-credential"'), 'not-a-real-credential')
    def test_missing_key(self):
        with self.assertRaises(SecretError): parse_key('OTHER_VARIABLE=unused')
    def test_duplicate_key(self):
        with self.assertRaises(SecretError): parse_key('TYPESAFE_API_KEY=not-a-real-credential\nTYPESAFE_API_KEY=not-a-real-credential')
    def fixture(self):
        return {'model':MODEL,'answers':{'plan':{'type':'choice','choice':'NATIVE','confidence':0.5,'probabilities':{'NATIVE':0.8,'ABSTAIN':0.2}}},'usage':{'input_tokens':123,'output_tokens':20}}
    def test_response_valid(self):
        self.assertEqual(validate_response(self.fixture(),'plan',['NATIVE','ABSTAIN'])['choice'],'NATIVE')
    def test_model_pinned(self):
        x=self.fixture(); x['model']='different-model'
        with self.assertRaises(ValueError): validate_response(x,'plan',['NATIVE','ABSTAIN'])
    def test_nonfinite_rejected(self):
        x=self.fixture(); x['answers']['plan']['confidence']=float('nan')
        with self.assertRaises(ValueError): validate_response(x,'plan',['NATIVE','ABSTAIN'])
    def test_invalid_choice_rejected(self):
        x=self.fixture(); x['answers']['plan']['choice']='INVENTED'
        with self.assertRaises(ValueError): validate_response(x,'plan',['NATIVE','ABSTAIN'])
    def test_expected_uniform_tiny(self):
        self.assertEqual(expected(dict(n=3,m=2,keep=1000,skew=False,seed=0)),[[0,1,0,0],[1,1,91,1],[2,1,0,0]])
    def test_expected_skew_tiny(self):
        self.assertEqual(expected(dict(n=3,m=2,keep=1000,skew=True,seed=0)),[[0,1,0,0],[1,1,0,0],[2,1,0,0]])
    def test_plan_ids_ignored_not_operators(self):
        self.assertEqual(normalize('BroadcastHashJoin k#91L [plan_id=42]'),normalize('BroadcastHashJoin k#3L [plan_id=2]'))
        self.assertNotEqual(normalize('BroadcastHashJoin'),normalize('SortMergeJoin'))

if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(Checks)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    report={'tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'successful':result.wasSuccessful(),'live_api_attempts':0}
    (ROOT/'offline_tests.json').write_text(json.dumps(report,indent=2)); print(json.dumps(report))
    raise SystemExit(0 if result.wasSuccessful() else 1)
