"""Offline tests only; real workspace request ledger is checked read-only."""
from pathlib import Path
import concurrent.futures,hashlib,json,os,sys,tempfile,unittest
R=Path(__file__).resolve().parent
from authorized_budget import AuthorizedBudget,BatchBudget,BudgetError,HEADER,AUTHORIZATION_TIME,MAX_REQUEST_BYTES
sys.path.insert(0,str(R.parent))
from safe_jev import Budget as LegacyBudget
HASH='a'*64

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=R,prefix='budget_test_')
        self.root=Path(self.tmp.name);self.ledger=self.root/'ledger.jsonl';self.policy=self.root/'authorization.json'
        self.initialize(49)
    def tearDown(self):self.tmp.cleanup()
    def initialize(self,n):
        entries=[HEADER]+[{'kind':'attempt','attempt':i+1,'purpose':'offline_fixture','request_sha256':HASH,'request_bytes':10} for i in range(n)]
        raw=('\n'.join(json.dumps(x) for x in entries)+'\n').encode();self.ledger.write_bytes(raw)
        self.policy.write_text(json.dumps({'kind':'user_authorized_cumulative_budget_v2','limit':10000,
            'authorized_at_utc':AUTHORIZATION_TIME,'preserved_attempts':n,'preserved_prefix_bytes':len(raw),
            'preserved_prefix_sha256':hashlib.sha256(raw).hexdigest()}))
        self.budget=AuthorizedBudget(self.ledger,self.policy);return raw
    def test_usage_retained(self):self.assertEqual(self.budget.status()['attempts_used'],49)
    def test_reservations_pass_original_limit_without_reset(self):
        prefix=self.ledger.read_bytes()
        self.assertEqual(self.budget.reserve(10,HASH,'trial'),50)
        self.assertEqual(AuthorizedBudget(self.ledger,self.policy).reserve(10,HASH,'trial'),51)
        self.assertTrue(self.ledger.read_bytes().startswith(prefix))
    def test_legacy_reader_compatible(self):
        self.budget.reserve(10,HASH,'trial');self.budget.reserve(10,HASH,'trial')
        self.assertEqual(LegacyBudget(self.ledger).count(),51)
    def test_legacy_writer_fails_closed(self):
        self.budget.reserve(10,HASH,'trial')
        with self.assertRaises(BudgetError):LegacyBudget(self.ledger).reserve(10,HASH,'trial')
    def test_hard_cap_with_concurrent_reservations(self):
        self.initialize(9990)
        def take(_):
            try:return AuthorizedBudget(self.ledger,self.policy).reserve(10,HASH,'parallel')
            except BudgetError:return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:got=list(pool.map(take,range(24)))
        self.assertEqual(sorted(x for x in got if x is not None),list(range(9991,10001)))
        self.assertEqual(self.budget.count(),10000)
        with self.assertRaises(BudgetError):self.budget.reserve(10,HASH,'exhausted')
    def test_batch_cap(self):
        b=BatchBudget(2,self.budget);b.reserve(10,HASH,'one');b.reserve(10,HASH,'two')
        with self.assertRaises(BudgetError):b.reserve(10,HASH,'three')
        self.assertEqual(self.budget.count(),51)
    def test_new_batch_does_not_reset_global_count(self):
        BatchBudget(1,self.budget).reserve(10,HASH,'one')
        BatchBudget(1,self.budget).reserve(10,HASH,'two')
        self.assertEqual(self.budget.count(),51)
    def test_no_network_still_charged(self):
        self.budget.reserve(10,HASH,'simulated_interruption');self.assertEqual(self.budget.count(),50)
    def test_deleted_ledger_not_recreated(self):
        self.ledger.unlink()
        with self.assertRaises(FileNotFoundError):self.budget.reserve(10,HASH,'trial')
        self.assertFalse(self.ledger.exists())
    def test_truncated_history_rejected(self):
        self.ledger.write_text(json.dumps(HEADER)+'\n')
        with self.assertRaises(BudgetError):self.budget.count()
    def test_modified_history_rejected(self):
        self.ledger.write_bytes(self.ledger.read_bytes().replace(b'offline_fixture',b'changed_fixture',1))
        with self.assertRaises(BudgetError):self.budget.count()
    def test_partial_line_rejected(self):
        with self.ledger.open('ab') as f:f.write(b'{')
        with self.assertRaises(BudgetError):self.budget.count()
    def test_invalid_sequence_rejected(self):
        with self.ledger.open('ab') as f:f.write(b'{"kind":"attempt","attempt":51}\n')
        with self.assertRaises(BudgetError):self.budget.count()
    def test_unauthorized_cap_rejected(self):
        p=json.loads(self.policy.read_text());p['limit']=10001;self.policy.write_text(json.dumps(p))
        with self.assertRaises(BudgetError):self.budget.count()
    def test_missing_policy_rejected(self):
        self.policy.unlink()
        with self.assertRaises(FileNotFoundError):self.budget.count()
    def test_request_hash_and_size_validation(self):
        for size,h,purpose in [(MAX_REQUEST_BYTES+1,HASH,'test'),(10,'bad','test'),(10,HASH,'bad name')]:
            with self.assertRaises(BudgetError):self.budget.reserve(size,h,purpose)
        self.assertEqual(self.budget.count(),49)
    def test_ledger_symlink_rejected(self):
        real=self.root/'real.jsonl';self.ledger.rename(real);self.ledger.symlink_to(real)
        with self.assertRaises(OSError):self.budget.count()
    def test_policy_symlink_rejected(self):
        real=self.root/'real_policy.json';self.policy.rename(real);self.policy.symlink_to(real)
        with self.assertRaises(OSError):self.budget.count()

if __name__=='__main__':
    before=AuthorizedBudget().count()
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
    r=unittest.TextTestRunner(verbosity=2).run(suite)
    report={'tests':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),'passed':r.wasSuccessful(),
        'live_api_attempts':0,'real_budget_before':before,'real_budget_after':AuthorizedBudget().count()}
    assert report['real_budget_before']==report['real_budget_after']
    (R/'BUDGET_TEST_RESULTS.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
    raise SystemExit(0 if r.wasSuccessful() else 1)
